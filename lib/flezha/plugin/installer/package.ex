defmodule Flezha.Plugin.Installer.Package do
  @moduledoc """
  A `.fpz` plugin package: a gzipped tar holding compiled code, laid out like a
  release's `lib/`.

      manifest.exs
      CHECKSUMS.sha256
      signature.sig                 # optional
      lib/<app>-<vsn>/ebin/<app>.app, *.beam
      lib/<app>-<vsn>/priv/...      # optional

  `verify/3` is the one pipeline of checks, run by an install and again by
  every boot: nothing is written outside the target directory before the
  archive's entries are vetted, and every refusal is a tagged reason the error
  catalogue maps.
  """

  alias Flezha.Config
  alias Flezha.Plugin.Installer.Checksums
  alias Flezha.Plugin.Installer.Manifest
  alias Flezha.Plugin.Installer.Signature

  @allowed_roots ~w(manifest.exs CHECKSUMS.sha256 signature.sig lib)
  @native_suffixes ~w(.so .dll .dylib)

  @typedoc "One bundled application as its `lib/<app>-<vsn>` directory declares it."
  @type app :: %{
          name: atom(),
          vsn: binary(),
          dir: Path.t(),
          modules: [module()],
          applications: [atom()],
          mod: term()
        }

  @type verified :: %{
          manifest: Manifest.t(),
          apps: [app()],
          signed: boolean(),
          digest: binary(),
          warning: binary() | nil
        }

  @doc """
  Extract `archive` into `dir` and check it whole: entries, layout, checksums,
  signature policy, manifest and the runtime line it was built on, with the
  warning a package built on an older Elixir minor loads with. `opts` carries
  `:max_package_bytes`, `:require_signature` and `:trusted_keys`, the
  configuration's by default.
  """
  @spec verify(Path.t(), Path.t(), keyword()) :: {:ok, verified()} | {:error, term()}
  def verify(archive, dir, opts \\ config_opts()) do
    with :ok <- extract(archive, dir, Keyword.fetch!(opts, :max_package_bytes)),
         :ok <- check_root(dir),
         :ok <- Checksums.check(dir),
         {:ok, signed} <-
           Signature.check(
             dir,
             Keyword.fetch!(opts, :require_signature),
             Keyword.fetch!(opts, :trusted_keys)
           ),
         {:ok, manifest} <- Manifest.read(dir),
         {:ok, apps} <- check_contents(dir, manifest),
         {:ok, warning} <- runtime_warning(manifest) do
      {:ok,
       %{
         manifest: manifest,
         apps: apps,
         signed: signed,
         digest: digest(archive),
         warning: warning
       }}
    end
  end

  defp runtime_warning(manifest) do
    case Manifest.check_runtime(manifest, Manifest.runtime()) do
      :ok ->
        {:ok, nil}

      {:ok, {:elixir_minor, built, running}} ->
        {:ok,
         "plugin package #{manifest.id} was built on Elixir #{built} and loads on Elixir " <>
           "#{running}; a call it makes into an Elixir internal changed between them fails " <>
           "only when it runs, so package it again on Elixir #{running}"}

      error ->
        error
    end
  end

  @doc """
  The applications under `dir`'s `lib/`, checked against `manifest` and against
  the rule that an application depending on Flezha declares no callback, in the
  order they load: the plugin application first, then `apps` order.
  """
  @spec check_contents(Path.t(), term()) :: {:ok, [app()]} | {:error, term()}
  def check_contents(dir, manifest) do
    with {:ok, apps} <- read_lib(dir),
         :ok <- Manifest.validate(manifest, apps),
         :ok <- check_callbacks(apps) do
      by_name = Map.new(apps, &{&1.name, &1})
      {:ok, for({name, _vsn} <- manifest.apps, do: Map.fetch!(by_name, name))}
    end
  end

  @doc """
  What `archive` declares, read from its manifest alone and tolerating one that
  does not read: its id (the file name when the manifest gives none), version,
  applications and providers.
  """
  @spec declared(Path.t()) :: %{
          id: binary(),
          version: binary() | nil,
          apps: list(),
          providers: map()
        }
  def declared(archive) do
    case peek_manifest(archive) do
      {:ok, manifest} ->
        %{
          id: manifest.id,
          version: if(is_binary(manifest[:version]), do: manifest.version),
          apps: if(is_list(manifest[:apps]), do: manifest.apps, else: []),
          providers: if(is_map(manifest[:providers]), do: manifest.providers, else: %{})
        }

      :error ->
        %{id: Path.basename(archive, ".fpz"), version: nil, apps: [], providers: %{}}
    end
  end

  defp peek_manifest(archive) do
    with {:ok, [{_name, source}]} <-
           :erl_tar.extract(String.to_charlist(archive), [
             :compressed,
             :memory,
             {:files, [~c"manifest.exs"]}
           ]),
         {:ok, %{id: id} = manifest} when is_binary(id) <- Manifest.parse(source) do
      {:ok, manifest}
    else
      _ -> :error
    end
  end

  @doc """
  The applications of `apps`, `{name, vsn}` pairs, that `held` does not hold yet.
  One version of a library per VM: `held` maps an application to `{vsn, holder}`,
  and one held at the same version is used as it is, at another refuses.
  """
  @spec libraries_to_load([{atom(), binary()}], %{atom() => {binary(), binary()}}) ::
          {:ok, [atom()]} | {:error, term()}
  def libraries_to_load(apps, held) do
    Enum.reduce_while(apps, {:ok, []}, fn {name, vsn}, {:ok, acc} ->
      case held[name] do
        nil -> {:cont, {:ok, [name | acc]}}
        {^vsn, _holder} -> {:cont, {:ok, acc}}
        {other, holder} -> {:halt, {:error, {:library_conflict, name, vsn, other, holder}}}
      end
    end)
    |> then(fn
      {:ok, names} -> {:ok, Enum.reverse(names)}
      error -> error
    end)
  end

  @doc "`:ok` when no provider id of `ids` is one `held` maps to its holder."
  @spec providers_free([binary()], %{binary() => binary()}) :: :ok | {:error, term()}
  def providers_free(ids, held) do
    case Enum.find(Enum.sort(ids), &Map.has_key?(held, &1)) do
      nil -> :ok
      id -> {:error, {:provider_id_taken, id, held[id]}}
    end
  end

  @doc "Who holds a library, a module or a provider id, as a refusal names it."
  @spec holder(binary() | nil) :: binary()
  def holder(nil), do: "the release"
  def holder(package_id), do: "package '#{package_id}'"

  @doc "The verification options the configuration sets."
  @spec config_opts() :: keyword()
  def config_opts() do
    [
      max_package_bytes: Config.get(:plugin_max_package_bytes),
      require_signature: Config.get(:plugin_require_signature),
      trusted_keys: Config.get(:plugin_trusted_keys)
    ]
  end

  @doc "The archive's SHA-256, lowercase hex."
  @spec digest(Path.t()) :: binary()
  def digest(archive) do
    archive
    |> File.stream!(65_536)
    |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  end

  @doc "Write the gzipped tar of everything in `dir` to `archive`."
  @spec create(Path.t(), Path.t()) :: :ok | {:error, term()}
  def create(dir, archive) do
    entries =
      for name <- Enum.sort(File.ls!(dir)),
          do: {String.to_charlist(name), String.to_charlist(Path.join(dir, name))}

    :erl_tar.create(String.to_charlist(archive), entries, [:compressed])
  end

  @doc """
  Every application under `dir`'s `lib/`, read from its `.app`, with its beams
  matched against the `modules` list and its native code refused.
  """
  @spec read_lib(Path.t()) :: {:ok, [app()]} | {:error, term()}
  def read_lib(dir) do
    case File.ls(Path.join(dir, "lib")) do
      {:ok, []} ->
        invalid(:lib_empty, %{})

      {:ok, entries} ->
        entries
        |> Enum.sort()
        |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
          case read_app(dir, entry) do
            {:ok, app} -> {:cont, {:ok, [app | acc]}}
            error -> {:halt, error}
          end
        end)
        |> then(fn
          {:ok, apps} -> {:ok, Enum.reverse(apps)}
          error -> error
        end)

      {:error, _} ->
        invalid(:lib_missing, %{})
    end
  end

  @doc "Every file under `dir`, at any depth; none when `dir` does not exist."
  @spec files(Path.t()) :: [Path.t()]
  def files(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn entry ->
          path = Path.join(dir, entry)
          if File.dir?(path), do: files(path), else: [path]
        end)

      {:error, _} ->
        []
    end
  end

  ## Extraction

  defp extract(archive, dir, max_bytes) do
    with {:ok, entries} <- table(archive),
         :ok <- check_entries(entries),
         :ok <- check_size(entries, max_bytes) do
      File.mkdir_p!(dir)

      case :erl_tar.extract(String.to_charlist(archive), [
             :compressed,
             {:cwd, String.to_charlist(dir)}
           ]) do
        :ok -> :ok
        {:error, reason} -> invalid(:unreadable, %{reason: reason})
      end
    end
  end

  # `:verbose` rows are `{name, type, size, mtime, mode, uid, gid}`.
  defp table(archive) do
    case :erl_tar.table(String.to_charlist(archive), [:compressed, :verbose]) do
      {:ok, entries} ->
        {:ok, for({name, type, size, _, _, _, _} <- entries, do: {to_string(name), type, size})}

      {:error, reason} ->
        invalid(:unreadable, %{reason: reason})
    end
  end

  defp check_entries(entries) do
    Enum.find_value(entries, :ok, fn {name, type, _size} -> entry_problem(name, type) end)
  end

  defp entry_problem(name, type) do
    segments = Path.split(name)

    cond do
      type not in [:regular, :directory] -> invalid(:unsafe_entry, %{entry: name, type: type})
      Path.type(name) != :relative -> invalid(:unsafe_entry, %{entry: name, type: :absolute})
      ".." in segments -> invalid(:unsafe_entry, %{entry: name, type: :traversal})
      Path.extname(name) in [".ex", ".erl"] -> invalid(:source_file, %{entry: name})
      hd(segments) not in @allowed_roots -> invalid(:unexpected_entry, %{entry: name})
      true -> nil
    end
  end

  defp check_size(entries, max_bytes) do
    bytes = Enum.reduce(entries, 0, fn {_name, _type, size}, acc -> acc + size end)

    if bytes > max_bytes,
      do: invalid(:too_large, %{bytes: bytes, limit: max_bytes}),
      else: :ok
  end

  defp check_root(dir) do
    cond do
      not File.regular?(Path.join(dir, "manifest.exs")) -> invalid(:manifest_missing, %{})
      not File.regular?(Path.join(dir, "CHECKSUMS.sha256")) -> invalid(:checksums_missing, %{})
      not File.dir?(Path.join(dir, "lib")) -> invalid(:lib_missing, %{})
      true -> :ok
    end
  end

  ## The lib/ walk

  defp read_app(root, entry) do
    dir = Path.join([root, "lib", entry])

    with {:ok, name, vsn} <- split_app_dir(entry),
         :ok <- only_ebin_and_priv(root, dir),
         {:ok, spec} <- consult_app(root, dir, name),
         :ok <- same_version(entry, spec, vsn),
         :ok <- beams_match(root, dir, name, spec),
         :ok <- no_native_priv(root, dir) do
      {:ok,
       %{
         name: String.to_atom(name),
         vsn: vsn,
         dir: dir,
         modules: Keyword.get(spec, :modules, []),
         applications:
           Keyword.get(spec, :applications, []) ++ Keyword.get(spec, :included_applications, []),
         mod: Keyword.get(spec, :mod)
       }}
    end
  end

  # An application name holds no `-`, so the first one ends it.
  defp split_app_dir(entry) do
    with [name, vsn] when vsn != "" <- String.split(entry, "-", parts: 2),
         true <- Regex.match?(~r/^[a-z][a-z0-9_]*$/, name) do
      {:ok, name, vsn}
    else
      _ -> invalid(:unexpected_entry, %{entry: "lib/" <> entry})
    end
  end

  defp only_ebin_and_priv(root, dir) do
    entries = File.ls!(dir)

    cond do
      not File.dir?(Path.join(dir, "ebin")) ->
        invalid(:ebin_missing, %{entry: rel(root, dir)})

      extra = Enum.find(entries, &(&1 not in ["ebin", "priv"])) ->
        invalid(:unexpected_entry, %{entry: rel(root, Path.join(dir, extra))})

      true ->
        :ok
    end
  end

  defp consult_app(root, dir, name) do
    path = Path.join([dir, "ebin", name <> ".app"])

    case :file.consult(String.to_charlist(path)) do
      {:ok, [{:application, app, spec}]} when is_list(spec) ->
        if Atom.to_string(app) == name,
          do: {:ok, spec},
          else: invalid(:app_mismatch, %{entry: rel(root, path), application: app})

      _ ->
        invalid(:app_unreadable, %{entry: rel(root, dir)})
    end
  end

  defp same_version(entry, spec, vsn) do
    case to_string(Keyword.get(spec, :vsn, "")) do
      ^vsn -> :ok
      other -> invalid(:app_mismatch, %{entry: "lib/" <> entry, vsn: other})
    end
  end

  # The beams in `ebin/` are exactly the `modules` list, each declaring the
  # module its file names.
  defp beams_match(root, dir, name, spec) do
    ebin = Path.join(dir, "ebin")
    files = File.ls!(ebin) -- [name <> ".app"]
    listed = MapSet.new(Keyword.get(spec, :modules, []), &Atom.to_string/1)

    beams =
      for file <- files,
          Path.extname(file) == ".beam",
          into: MapSet.new(),
          do: Path.rootname(file)

    application = rel(root, dir)

    cond do
      other = Enum.find(files, &(Path.extname(&1) != ".beam")) ->
        invalid(:unexpected_entry, %{entry: rel(root, Path.join(ebin, other))})

      missing = first(MapSet.difference(listed, beams)) ->
        invalid(:module_without_beam, %{application: application, module: missing})

      extra = first(MapSet.difference(beams, listed)) ->
        invalid(:beam_not_listed, %{application: application, module: extra})

      true ->
        Enum.find_value(Enum.sort(beams), :ok, &beam_problem(root, ebin, &1))
    end
  end

  # Prepared, never loaded: preparing validates the code and says whether it
  # declares an `on_load`, which is how a NIF is loaded.
  defp beam_problem(root, ebin, module) do
    path = Path.join(ebin, module <> ".beam")

    with {:ok, {name, _}} <- :beam_lib.chunks(String.to_charlist(path), []),
         true <- Atom.to_string(name) == module,
         prepared when not is_tuple(prepared) <- :erlang.prepare_loading(name, File.read!(path)) do
      if :erlang.has_prepared_code_on_load(prepared), do: {:error, {:native_code, module}}
    else
      _ -> invalid(:beam_unreadable, %{entry: rel(root, path)})
    end
  end

  defp no_native_priv(root, dir) do
    dir
    |> Path.join("priv")
    |> files()
    |> Enum.find_value(:ok, fn path ->
      if Path.extname(path) in @native_suffixes, do: {:error, {:native_code, rel(root, path)}}
    end)
  end

  defp first(set), do: set |> Enum.sort() |> List.first()

  # An application that depends on Flezha is loaded inside Flezha's own start
  # and can never be started, so a callback of its own would never run.
  defp check_callbacks(apps) do
    case Enum.find(apps, &(&1.mod != nil and depends_on_flezha?(&1, apps))) do
      nil -> :ok
      app -> {:error, {:app_callback, app.name}}
    end
  end

  @doc "Whether `app` depends on Flezha, directly or through another of `apps`."
  @spec depends_on_flezha?(app(), [app()]) :: boolean()
  def depends_on_flezha?(app, apps),
    do: reaches_flezha?(app, Map.new(apps, &{&1.name, &1}), MapSet.new())

  defp reaches_flezha?(app, by_name, seen) do
    Enum.any?(app.applications, fn dep ->
      dep == :flezha or
        (Map.has_key?(by_name, dep) and not MapSet.member?(seen, dep) and
           reaches_flezha?(by_name[dep], by_name, MapSet.put(seen, dep)))
    end)
  end

  defp rel(root, path), do: Path.relative_to(path, root)

  defp invalid(problem, found), do: {:error, {:package_invalid, problem, found}}
end
