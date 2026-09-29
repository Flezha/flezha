defmodule Flezha.Plugin.Installer.Loader do
  @moduledoc """
  Loads the plugin packages of the plugin root, once, at boot: plain function
  calls made inside the controller's start, after the state root is claimed and
  before the supervision tree's children are computed, so a package's providers
  are rows of the provider table before anything reads it.

  Per archive, in id order: verify, expand into `.loaded/<id>/`, check its
  libraries and modules against what the VM holds, append its code paths, load
  its applications and then all its modules at once, consolidate again the
  protocols it implements, check its providers, start its library applications
  and register its providers. A package failing any step is refused, what is
  cheap to undo is undone, and the next package is tried; the controller boots
  whatever the packages do. Nothing is ever purged or replaced: a module loaded
  before a later step failed stays loaded, unreferenced, until the next restart.
  """

  require Logger

  alias Flezha.Error
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Plugin.Installer.Package
  alias Flezha.Plugin.Providers

  @outcomes_key {__MODULE__, :outcomes}

  @type outcome :: %{
          id: binary(),
          file: binary(),
          version: binary() | nil,
          digest: binary(),
          signed: boolean(),
          providers: %{binary() => module()},
          status: :loaded | :refused,
          error: Error.t() | nil
        }

  @doc "Load every package of the plugin root and record the outcomes."
  @spec load_all() :: [outcome()]
  def load_all() do
    root = Layout.dir(:plugins)
    loaded = Path.join(root, ".loaded")
    File.rm_rf!(loaded)
    reset()

    outcomes =
      root
      |> Path.join("*.fpz")
      |> Path.wildcard()
      |> Enum.sort()
      |> Enum.with_index()
      |> Enum.map(fn {archive, n} -> verify(archive, Path.join(loaded, ".#{n}")) end)
      |> refuse_duplicates()
      |> Enum.sort_by(& &1.id)
      |> Enum.map_reduce(%{apps: %{}, modules: %{}, providers: %{}}, &load(&1, &2, loaded))
      |> elem(0)

    :persistent_term.put(@outcomes_key, outcomes)
    outcomes
  end

  @doc "What the last `load_all/0` of this VM decided; none before it ran."
  @spec outcomes() :: [outcome()]
  def outcomes(), do: :persistent_term.get(@outcomes_key, [])

  @doc """
  Execute the package events for the recorded outcomes. Called once the
  supervision tree is up, because the bridge and the audit sink attach inside it.
  """
  @spec announce() :: :ok
  def announce() do
    for outcome <- outcomes() do
      case outcome.status do
        :loaded ->
          :telemetry.execute([:flezha, :plugin_package, :loaded], %{count: 1}, %{
            id: outcome.id,
            version: outcome.version,
            digest: outcome.digest,
            signed: outcome.signed,
            providers: Map.keys(outcome.providers)
          })

        :refused ->
          :telemetry.execute([:flezha, :plugin_package, :refused], %{count: 1}, %{
            id: outcome.id,
            version: outcome.version,
            digest: outcome.digest,
            code: outcome.error.code,
            message: outcome.error.message
          })
      end
    end

    :ok
  end

  @doc """
  A refusal of `module`'s as a caller sees it. A reason the error catalogue maps
  keeps its code. Any other reason of a loaded package's provider is wrapped as
  `kind`, `:provider_refused` for its options or `:provider_failed` for its
  worker, naming the package and the provider, since a package adds no codes of
  its own. A first-party provider's reason is returned as it is.
  """
  @spec refusal(:provider_refused | :provider_failed, module(), term()) :: term()
  def refusal(kind, module, reason) do
    with false <- Error.catalogued?(reason, nil),
         %{} = package <-
           Enum.find(outcomes(), &(&1.status == :loaded and module in Map.values(&1.providers))) do
      provider = Enum.find_value(package.providers, fn {id, m} -> if m == module, do: id end)
      {kind, package.id, provider, if(is_binary(reason), do: reason, else: inspect(reason))}
    else
      _ -> reason
    end
  end

  @doc "Forget the recorded outcomes. For tests that load packages in one VM."
  @spec reset() :: :ok
  def reset(), do: :persistent_term.put(@outcomes_key, [])

  ## Verification

  # A refused archive still names its version and providers when its manifest
  # reads, so an enable of one of them can say which package was refused.
  defp verify(archive, dir) do
    declared = Package.declared(archive)

    base = %{
      id: declared.id,
      file: Path.basename(archive),
      version: declared.version,
      digest: Package.digest(archive),
      signed: false,
      providers: declared.providers,
      status: :refused,
      error: nil,
      dir: dir,
      apps: []
    }

    case Package.verify(archive, dir) do
      {:ok, verified} ->
        %{
          base
          | id: verified.manifest.id,
            version: verified.manifest.version,
            signed: verified.signed,
            providers: verified.manifest.providers,
            apps: verified.apps,
            status: :verified
        }

      {:error, reason} ->
        %{base | error: reason}
    end
  end

  defp refuse_duplicates(packages) do
    files =
      packages
      |> Enum.group_by(& &1.id, & &1.file)
      |> Map.filter(fn {_id, files} -> length(files) > 1 end)

    Enum.map(packages, fn package ->
      case files[package.id] do
        nil ->
          package

        dupes ->
          %{package | status: :refused, error: {:duplicate_id, package.id, Enum.sort(dupes)}}
      end
    end)
  end

  ## Loading

  defp load(%{status: :verified} = package, held, loaded) do
    dir = Path.join(loaded, package.id)
    File.rename!(package.dir, dir)

    apps =
      for app <- package.apps, do: %{app | dir: Path.join([dir, "lib", Path.basename(app.dir)])}

    case load_apps(%{package | dir: dir, apps: apps}, held) do
      {:ok, to_load} ->
        log_loaded(package)
        {finish(package, :loaded, nil), hold(held, package, to_load)}

      {:error, reason} ->
        File.rm_rf!(dir)
        {refused(package, reason), held}
    end
  end

  defp load(package, held, _loaded) do
    File.rm_rf!(package.dir)
    {refused(package, package.error), held}
  end

  # A refusal takes back what is cheap to take back: the applications this
  # package started (none of `to_load` ran before), their specifications and the
  # code paths. Loaded modules stay, unreferenced, until the next restart.
  defp load_apps(package, held) do
    with {:ok, to_load} <- libraries(package.apps, held),
         :ok <- modules_free(to_load, held) do
      ebins = Enum.map(to_load, &String.to_charlist(Path.join(&1.dir, "ebin")))
      :ok = :code.add_pathsz(ebins)

      case load_code(package, to_load, held) do
        :ok ->
          {:ok, to_load}

        {:error, reason} ->
          Enum.each(Enum.reverse(to_load), &Application.stop(&1.name))
          Enum.each(to_load, &:application.unload(&1.name))
          Enum.each(ebins, &:code.del_path/1)
          {:error, reason}
      end
    end
  end

  defp load_code(package, to_load, held) do
    with :ok <- load_specs(to_load),
         :ok <- load_modules(to_load),
         :ok <- consolidate(to_load),
         :ok <- providers_free(package.providers, held),
         :ok <- start_libraries(package.apps, to_load) do
      register(package.providers)
    end
  end

  # What the VM already holds, the release's and the earlier packages', decides
  # which of the package's applications load.
  defp libraries(apps, held) do
    in_vm =
      for {name, _description, vsn} <- Application.loaded_applications(),
          into: %{},
          do: {name, {to_string(vsn), Package.holder(held.apps[name])}}

    with {:ok, names} <- Package.libraries_to_load(Enum.map(apps, &{&1.name, &1.vsn}), in_vm),
         do: {:ok, Enum.filter(apps, &(&1.name in names))}
  end

  defp modules_free(to_load, held) do
    Enum.find_value(to_load, :ok, fn app ->
      Enum.find_value(app.modules, fn module ->
        if :code.which(module) != :non_existing or :code.is_loaded(module) != false do
          {:error, {:module_conflict, module, Package.holder(held.modules[module])}}
        end
      end)
    end)
  end

  defp load_specs(to_load) do
    Enum.find_value(to_load, :ok, fn app ->
      case :application.load(app.name) do
        :ok -> nil
        {:error, reason} -> {:error, {:load_failed, "application #{app.name}", reason}}
      end
    end)
  end

  # Embedded mode loads nothing on demand, so every module is loaded here, all
  # of them or none.
  defp load_modules(to_load) do
    case :code.atomic_load(Enum.flat_map(to_load, & &1.modules)) do
      :ok ->
        :ok

      {:error, [{module, reason} | _]} ->
        {:error, {:load_failed, "module #{inspect(module)}", reason}}
    end
  end

  # A protocol the release consolidated dispatches only the types it was
  # consolidated with, so it is consolidated again with the package's added.
  # A protocol the package defines itself stays unconsolidated.
  defp consolidate(to_load) do
    own = MapSet.new(Enum.flat_map(to_load, & &1.modules))

    to_load
    |> Enum.flat_map(& &1.modules)
    |> Enum.flat_map(&impls/1)
    |> Enum.reject(fn {protocol, _type} -> MapSet.member?(own, protocol) end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.find_value(:ok, fn {protocol, types} -> reconsolidate(protocol, types) end)
  end

  defp impls(module) do
    for {:__impl__, impl} <- module.module_info(:attributes), do: {impl[:protocol], impl[:for]}
  end

  defp reconsolidate(protocol, types) do
    with {:consolidated, current} <- protocol.__protocol__(:impls),
         {:ok, binary} <- Protocol.consolidate(protocol, Enum.uniq(current ++ types)),
         {:module, ^protocol} <- :code.load_binary(protocol, :code.which(protocol), binary) do
      nil
    else
      :not_consolidated -> nil
      error -> {:error, {:protocol_consolidation_failed, protocol, error}}
    end
  rescue
    exception ->
      {:error, {:protocol_consolidation_failed, protocol, Exception.message(exception)}}
  end

  defp providers_free(providers, held) do
    declaration =
      Enum.find_value(Enum.sort(providers), :ok, fn {id, module} ->
        unless Providers.provider?(module),
          do: {:error, {:package_invalid, :provider_declaration, %{provider: id, module: module}}}
      end)

    rows = Map.new(Providers.all(), fn {id, _} -> {id, Package.holder(held.providers[id])} end)
    with :ok <- declaration, do: Package.providers_free(Map.keys(providers), rows)
  end

  # Library applications start now, before any operator binds. An application
  # depending on Flezha is loaded and never started: Flezha is starting.
  defp start_libraries(apps, to_load) do
    to_load
    |> Enum.reject(&Package.depends_on_flezha?(&1, apps))
    |> Enum.find_value(:ok, fn app ->
      case Application.ensure_all_started(app.name, :temporary) do
        {:ok, _started} -> nil
        {:error, {name, reason}} -> {:error, {:load_failed, "application #{name}", reason}}
      end
    end)
  end

  defp register(providers) do
    Enum.find_value(Enum.sort(providers), :ok, fn {id, module} ->
      case Providers.add(id, module) do
        :ok -> nil
        {:error, _} = error -> error
      end
    end)
  end

  ## Outcomes

  defp hold(held, package, to_load) do
    owner = package.id
    modules = Enum.flat_map(to_load, & &1.modules)

    %{
      apps: Map.merge(held.apps, Map.new(to_load, &{&1.name, owner})),
      modules: Map.merge(held.modules, Map.new(modules, &{&1, owner})),
      providers:
        Map.merge(held.providers, Map.new(package.providers, fn {id, _} -> {id, owner} end))
    }
  end

  defp finish(package, status, error) do
    package
    |> Map.take([:id, :file, :version, :digest, :signed, :providers])
    |> Map.merge(%{status: status, error: error})
  end

  defp refused(package, reason) do
    error = Error.from_reason(reason, nil)

    Logger.error(
      "plugin package #{package.id} (#{package.file}) refused: #{error.message} (#{error.code})"
    )

    finish(package, :refused, error)
  end

  defp log_loaded(package) do
    unless package.signed,
      do: Logger.warning("plugin package #{package.id} #{package.version} is unsigned")

    Logger.info(
      "plugin package #{package.id} #{package.version} loaded, providers: " <>
        Enum.join(Enum.sort(Map.keys(package.providers)), ", ")
    )
  end
end
