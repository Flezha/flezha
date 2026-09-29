defmodule Flezha.Plugin.Installer do
  @moduledoc """
  Places plugin packages in the plugin root and says what the controller holds
  of them. Plain functions owning no process: `install/2`, `uninstall/2` and
  `sync/1` act on the plugin root alone, so they have the same effect from the
  CLI's own VM (`flz plugin install`, through `flzvm eval`), from the controller,
  or from a user interface running beside it. What they place is loaded by
  `Flezha.Plugin.Installer.Loader` at the next boot; nothing here touches the
  running VM or `.loaded/`.

  An install fetches or copies the archive under `<plugin_root>/.staging/`,
  verifies it with every check the boot applies that does not need the package
  loaded, and renames it to `<plugin_root>/<id>.fpz`. A refused install leaves
  the plugin root as it was.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Http.Client
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Plugin.Installer.InstalledPlugin
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Package
  alias Flezha.Plugin.Providers

  @list_variable "FLEZHA_PLUGINS"
  @max_redirects 5

  @type placed :: %{id: binary(), version: binary(), signed: boolean(), root: Path.t()}

  @doc """
  Place the package at `source`, a local path or an `https://` URL, as
  `<plugin_root>/<id>.fpz`, replacing every archive declaring the same id.
  Refused while the node declares its list. `opts` may carry `:cacerts` for the
  fetch.
  """
  @spec install(binary(), keyword()) :: {:ok, placed()} | {:error, term()}
  def install(source, opts \\ []) do
    with :ok <- no_list_declared() do
      in_staging(fn staging ->
        with {:ok, candidates} <- stage(staging, [{source, nil}], opts, present(), :replace),
             do: {:ok, candidates |> commit(MapSet.new()) |> hd()}
      end)
    end
  end

  @doc "Remove every archive declaring `id`. Refused while the node declares its list."
  @spec uninstall(binary(), keyword()) ::
          {:ok, %{id: binary(), root: Path.t()}} | {:error, term()}
  def uninstall(id, _opts \\ []) do
    with :ok <- no_list_declared() do
      case for(%{id: ^id, path: path} <- present(), do: path) do
        [] ->
          {:error, {:plugin_not_installed, id}}

        found ->
          Enum.each(found, &File.rm!/1)
          {:ok, %{id: id, root: root()}}
      end
    end
  end

  @doc """
  Make the plugin root hold exactly the packages `FLEZHA_PLUGINS` declares: a
  pinned entry whose archive is present is kept unfetched, every other entry is
  fetched, verified and placed, and every archive the list did not produce is
  removed. A failed entry fails the sync and changes nothing.
  """
  @spec sync(keyword()) ::
          {:ok, %{placed: [placed()], kept: [binary()], removed: [binary()], root: Path.t()}}
          | {:error, term()}
  def sync(opts \\ []) do
    with {:ok, entries} <- declared_list() do
      by_digest = Map.new(present(), &{Package.digest(&1.path), &1})

      {kept, to_fetch} =
        Enum.split_with(entries, fn {_source, pin} -> Map.has_key?(by_digest, pin) end)

      kept = Enum.map(kept, fn {_source, pin} -> by_digest[pin] end)

      in_staging(fn staging ->
        with {:ok, candidates} <-
               stage(staging, to_fetch, opts, kept, :keep),
             :ok <- distinct_ids(Enum.map(kept ++ Enum.map(candidates, & &1.manifest), & &1.id)) do
          kept_paths = Enum.map(kept, & &1.path)
          placed = commit(candidates, MapSet.new(kept_paths))
          keep = MapSet.new(kept_paths ++ Enum.map(placed, &target(&1.id)))

          removed =
            for %{path: path} <- present(),
                not MapSet.member?(keep, path),
                do: tap(path, &File.rm!/1)

          {:ok, %{placed: placed, kept: Enum.map(kept, & &1.id), removed: removed, root: root()}}
        end
      end)
    end
  end

  @doc """
  One row per package id: what this controller loaded or refused at boot,
  merged with what its plugin root holds now.
  """
  @spec list() :: [InstalledPlugin.t()]
  def list() do
    booted = Enum.uniq_by(Loader.outcomes(), & &1.id)
    present = Enum.group_by(present(), & &1.id)

    at_boot = for outcome <- booted, do: row(outcome, Map.get(present, outcome.id, []))

    since =
      for {id, [declared | _]} <- present,
          not Enum.any?(booted, &(&1.id == id)),
          do: pending(declared, nil)

    at_boot |> Enum.reject(&is_nil/1) |> Enum.concat(since) |> Enum.sort_by(& &1.id)
  end

  @doc """
  Why `provider`, a provider id or module, is no row here although a package
  declares it: the reason naming the package and whether it was refused at boot
  or placed since. `nil` when no package here declares it.
  """
  @spec unavailable(binary() | module()) :: tuple() | nil
  def unavailable(provider) do
    label = if is_binary(provider), do: provider, else: inspect(provider)
    declaring? = &(Map.has_key?(&1.providers, provider) or provider in Map.values(&1.providers))

    cond do
      refused = Enum.find(Loader.outcomes(), &(&1.status == :refused and declaring?.(&1))) ->
        {:provider_unavailable, label, refused.id, "refused", refused.error.code}

      placed = Enum.find(present(), declaring?) ->
        {:provider_unavailable, label, placed.id, "pending", nil}

      true ->
        nil
    end
  end

  @doc "The absolute plugin root the functions act on."
  @spec root() :: Path.t()
  def root(), do: Path.expand(Layout.dir(:plugins))

  ## Rows

  defp row(%{status: :loaded} = outcome, []), do: from(outcome, :removed)
  defp row(_refused, []), do: nil

  defp row(outcome, [declared | _] = present) do
    if Enum.map(present, &Package.digest(&1.path)) == [outcome.digest],
      do: from(outcome, outcome.status),
      else: pending(declared, if(outcome.status == :loaded, do: outcome.version))
  end

  defp from(outcome, status) do
    %InstalledPlugin{
      id: outcome.id,
      version: outcome.version,
      status: status,
      providers: outcome.providers |> Map.keys() |> Enum.sort(),
      error: if(status == :refused, do: outcome.error)
    }
  end

  defp pending(declared, loaded_version) do
    %InstalledPlugin{
      id: declared.id,
      version: declared.version,
      status: :pending,
      loaded_version: loaded_version,
      providers: declared.providers |> Map.keys() |> Enum.sort()
    }
  end

  # Every archive in the plugin root, with what it declares.
  defp present(), do: for(path <- archives(), do: Map.put(Package.declared(path), :path, path))

  defp archives(), do: root() |> Path.join("*.fpz") |> Path.wildcard() |> Enum.sort()

  ## Staging

  # One staging directory per call, removed however the call ends.
  defp in_staging(fun) do
    staging = Path.join([root(), ".staging", Base.url_encode64(:crypto.strong_rand_bytes(9))])
    File.mkdir_p!(staging)

    try do
      fun.(staging)
    after
      File.rm_rf!(staging)
    end
  end

  # Obtain and verify every `{source, pin}`, each checked against the release,
  # the packages staged before it and `others`, less the one it replaces when an
  # install replaces its id.
  defp stage(staging, entries, opts, others, mode) do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {{source, pin}, n}, {:ok, staged} ->
      archive = Path.join(staging, "#{n}.fpz")

      with :ok <- obtain(source, archive, opts),
           :ok <- pinned(source, archive, pin),
           {:ok, verified} <- Package.verify(archive, Path.join(staging, "#{n}")),
           id = verified.manifest.id,
           held = if(mode == :replace, do: Enum.reject(others, &(&1.id == id)), else: others),
           :ok <- fits(verified.manifest, held ++ Enum.map(staged, & &1.manifest)) do
        {:cont, {:ok, staged ++ [Map.put(verified, :archive, archive)]}}
      else
        error -> {:halt, error}
      end
    end)
  end

  # Renaming is the one step a reader can see: an archive is whole or absent.
  defp commit(candidates, keep) do
    placed =
      for candidate <- candidates do
        id = candidate.manifest.id
        File.rename!(candidate.archive, target(id))

        for %{id: ^id, path: path} <- present(),
            path != target(id),
            not MapSet.member?(keep, path),
            do: File.rm!(path)

        %{id: id, version: candidate.manifest.version, signed: candidate.signed, root: root()}
      end

    placed
  end

  defp target(id), do: Path.join(root(), id <> ".fpz")

  ## Install-time checks

  # The checks of the boot that need no loading: one version of each library
  # against the release and the other packages, and provider ids no row or other
  # package holds.
  defp fits(manifest, others) do
    with :ok <- libraries_fit(manifest, others) do
      providers_fit(manifest, others)
    end
  end

  defp libraries_fit(manifest, others) do
    from_packages =
      for other <- others,
          {app, vsn} <- other.apps,
          into: %{},
          do: {app, {vsn, Package.holder(other.id)}}

    from_release =
      for {app, _vsn} <- manifest.apps,
          vsn = release_version(app),
          vsn != nil,
          into: %{},
          do: {app, {vsn, Package.holder(nil)}}

    with {:ok, _to_load} <-
           Package.libraries_to_load(manifest.apps, Map.merge(from_packages, from_release)),
         do: :ok
  end

  defp providers_fit(manifest, others) do
    from_packages =
      for other <- others,
          {id, _module} <- other.providers,
          into: %{},
          do: {id, Package.holder(other.id)}

    from_release = Map.new(Providers.first_party(), fn {id, _} -> {id, Package.holder(nil)} end)
    Package.providers_free(Map.keys(manifest.providers), Map.merge(from_packages, from_release))
  end

  # The version of `app` the release ships, or nil. A running controller has
  # packages' applications on its code path too, under `.loaded/`: those are
  # packages, not the release.
  defp release_version(app) do
    loaded_root = String.to_charlist(Path.join(root(), ".loaded"))

    with dir when is_list(dir) <- :code.lib_dir(app),
         false <- List.starts_with?(dir, loaded_root),
         result when result in [:ok, {:error, {:already_loaded, app}}] <- :application.load(app) do
      to_string(Application.spec(app, :vsn))
    else
      _ -> nil
    end
  end

  ## The declared list

  defp no_list_declared() do
    if Config.get(:plugin_list) == nil, do: :ok, else: {:error, {:list_declared, @list_variable}}
  end

  defp declared_list() do
    case Config.get(:plugin_list) do
      nil ->
        {:error, {:list_not_declared, @list_variable}}

      list ->
        {:ok,
         for entry <- String.split(list, ~r/[\s,]+/, trim: true) do
           case String.split(entry, "#sha256=", parts: 2) do
             [source, pin] -> {source, String.downcase(pin)}
             [source] -> {source, nil}
           end
         end}
    end
  end

  defp distinct_ids(ids) do
    case ids -- Enum.uniq(ids) do
      [] -> :ok
      [id | _] -> {:error, {:duplicate_id, id, [@list_variable]}}
    end
  end

  defp pinned(_source, _archive, nil), do: :ok

  defp pinned(source, archive, pin) do
    case Package.digest(archive) do
      ^pin -> :ok
      actual -> {:error, {:digest_mismatch, source, pin, actual}}
    end
  end

  ## Sources

  defp obtain("https://" <> _ = url, archive, opts) do
    request = %{
      source: url,
      archive: archive,
      max: Config.get(:plugin_max_package_bytes),
      cacerts: Keyword.get_lazy(opts, :cacerts, &:public_key.cacerts_get/0)
    }

    fetch(URI.parse(url), request, @max_redirects)
  end

  defp obtain(source, archive, _opts) do
    cond do
      String.contains?(source, "://") -> {:error, {:source_unsupported, source}}
      File.regular?(source) -> File.cp!(source, archive)
      true -> {:error, {:fetch_failed, source, "no such file"}}
    end
  end

  defp fetch(%URI{scheme: "https", host: host} = uri, request, redirects) when is_binary(host) do
    transport = [
      verify: :verify_peer,
      cacerts: request.cacerts,
      customize_hostname_check: [match_fun: :public_key.pkix_verify_hostname_match_fun(:https)]
    ]

    path = (uri.path || "/") <> if(uri.query, do: "?" <> uri.query, else: "")

    case Client.connect(:https, host, uri.port, transport) do
      {:ok, conn} ->
        answer =
          Client.request(conn, "GET", path, [], "", Durations.get(:plugin_fetch_ms),
            max_body_bytes: request.max
          )

        :ok = Client.close(answer_conn(answer))
        fetched(answer, uri, request, redirects)

      {:error, reason} ->
        {:error, {:fetch_failed, request.source, describe(reason)}}
    end
  end

  # A redirect may lead only to another https:// URL.
  defp fetch(uri, _request, _redirects), do: {:error, {:source_unsupported, URI.to_string(uri)}}

  defp answer_conn({:ok, conn, _status, _headers, _body}), do: conn
  defp answer_conn({:error, _reason, conn}), do: conn

  defp fetched({:ok, _conn, status, _headers, body}, _uri, request, _left)
       when status in 200..299,
       do: File.write!(request.archive, body)

  defp fetched({:ok, _conn, status, headers, _body}, uri, request, left)
       when status in [301, 302, 303, 307, 308] do
    case Client.header(headers, "location") do
      nil ->
        {:error, {:fetch_failed, request.source, "HTTP #{status} without a location"}}

      _location when left == 0 ->
        {:error, {:fetch_failed, request.source, "more than #{@max_redirects} redirects"}}

      location ->
        fetch(URI.merge(uri, location), request, left - 1)
    end
  end

  defp fetched({:ok, _conn, status, _headers, _body}, _uri, request, _left),
    do: {:error, {:fetch_failed, request.source, "HTTP #{status}"}}

  defp fetched({:error, {:body_too_large, bytes}, _conn}, _uri, request, _left),
    do: {:error, {:package_invalid, :too_large, %{bytes: bytes, limit: request.max}}}

  defp fetched({:error, reason, _conn}, _uri, request, _left),
    do: {:error, {:fetch_failed, request.source, describe(reason)}}

  defp describe(%{__exception__: true} = exception), do: Exception.message(exception)
  defp describe(reason), do: inspect(reason)
end
