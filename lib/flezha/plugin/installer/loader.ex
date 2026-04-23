defmodule Flezha.Plugin.Installer.Loader do
  @moduledoc """
  In-VM module registry for installed plugins.

  Owns three responsibilities:

    1. Write freshly-compiled beam binaries to
       `installed/<id>/<version>/ebin/<module>.beam` and `:code.load_binary/3`
       each.
    2. Remember which modules belong to which plugin id so uninstall and
       upgrade can target a precise set.
    3. On boot, hydrate from the `Store`: every catalog row triggers a best
       effort load from its `ebin/`; rows that fail load are flipped to
       `:load_failed` and the Loader continues.

  Collision guard: if a module listed in the incoming plugin's `modules`
  is already loaded and NOT owned by any installed plugin (i.e. it's part
  of the compiled release), the load is refused.
  """

  use GenServer

  require Logger

  alias Flezha.Plugin.Installer.Store

  @type plugin_id :: binary()
  @type state :: %{registry: %{plugin_id() => {binary(), [module()]}}}

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Write every `{module, beam}` in `beams` to `ebin_dir`, load into the VM,
  and record ownership under `plugin_id`/`version`. Rolls back loaded
  modules on partial failure.
  """
  @spec load_beams(plugin_id(), binary(), Path.t(), [{module(), binary()}]) ::
          :ok | {:error, term()}
  def load_beams(plugin_id, version, ebin_dir, beams)
      when is_binary(plugin_id) and is_binary(version) and is_list(beams) do
    GenServer.call(__MODULE__, {:load_beams, plugin_id, version, ebin_dir, beams}, 30_000)
  end

  @doc "Purge every module owned by `plugin_id`. Idempotent."
  @spec purge_modules(plugin_id()) :: :ok
  def purge_modules(plugin_id) when is_binary(plugin_id) do
    GenServer.call(__MODULE__, {:purge_modules, plugin_id}, 30_000)
  end

  @doc "Ids currently loaded in the VM (known to the Loader)."
  @spec installed_ids() :: MapSet.t(plugin_id())
  def installed_ids do
    case Process.whereis(__MODULE__) do
      nil -> MapSet.new()
      _ -> GenServer.call(__MODULE__, :installed_ids)
    end
  end

  @doc "All registrations as `%{plugin_id => {version, modules}}`."
  @spec all() :: %{plugin_id() => {binary(), [module()]}}
  def all, do: GenServer.call(__MODULE__, :all)

  @doc "Modules owned by `plugin_id`, or `[]` if none."
  @spec modules_of(plugin_id()) :: [module()]
  def modules_of(plugin_id) when is_binary(plugin_id) do
    case GenServer.call(__MODULE__, {:lookup, plugin_id}) do
      {:ok, {_version, mods}} -> mods
      :error -> []
    end
  end

  @doc """
  Re-run boot-time hydration. Intended for tests — in production hydration
  runs once at init/1.
  """
  @spec hydrate_from_store() :: :ok
  def hydrate_from_store do
    GenServer.call(__MODULE__, :hydrate_from_store, 60_000)
  end

  ## GenServer

  @impl true
  def init(_opts) do
    state = %{registry: %{}}
    state = do_hydrate(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:installed_ids, _from, state) do
    {:reply, MapSet.new(Map.keys(state.registry)), state}
  end

  def handle_call(:all, _from, state), do: {:reply, state.registry, state}

  def handle_call({:lookup, plugin_id}, _from, state) do
    {:reply, Map.fetch(state.registry, plugin_id), state}
  end

  def handle_call({:load_beams, plugin_id, version, ebin_dir, beams}, _from, state) do
    with :ok <- reject_collisions(beams, plugin_id, state.registry),
         :ok <- File.mkdir_p!(ebin_dir),
         {:ok, loaded} <- write_and_load(ebin_dir, beams) do
      registry =
        Map.put(state.registry, plugin_id, {version, Enum.map(loaded, fn {m, _} -> m end)})

      {:reply, :ok, %{state | registry: registry}}
    else
      {:error, {:partial, already_loaded, reason}} ->
        Enum.each(already_loaded, &unload_module/1)
        {:reply, {:error, reason}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:purge_modules, plugin_id}, _from, state) do
    case Map.get(state.registry, plugin_id) do
      nil ->
        {:reply, :ok, state}

      {_version, modules} ->
        Enum.each(modules, &unload_module/1)
        {:reply, :ok, %{state | registry: Map.delete(state.registry, plugin_id)}}
    end
  end

  def handle_call(:hydrate_from_store, _from, state) do
    {:reply, :ok, do_hydrate(%{state | registry: %{}})}
  end

  ## Hydration

  defp do_hydrate(state) do
    try do
      rows = safe_store_all()

      Enum.reduce(rows, state, fn {plugin_id, row}, acc ->
        case row[:status] do
          :load_failed ->
            acc

          _ ->
            hydrate_row(plugin_id, row, acc)
        end
      end)
    catch
      kind, reason ->
        Logger.warning("[plugin_installer] Loader hydration failed: #{inspect({kind, reason})}")
        state
    end
  end

  defp hydrate_row(plugin_id, row, state) do
    ebin_dir = Path.join(row[:path] || "", "ebin")

    case load_from_ebin(ebin_dir, row[:modules] || []) do
      {:ok, loaded_mods} ->
        registry =
          Map.put(state.registry, plugin_id, {row[:version], loaded_mods})

        %{state | registry: registry}

      {:error, {module, reason}} ->
        Logger.warning(
          "[plugin_installer] load failed for plugin=#{plugin_id} module=#{inspect(module)}: #{inspect(reason)}"
        )

        :telemetry.execute(
          [:flezha, :plugin_installer, :load_failed],
          %{count: 1},
          %{plugin_id: plugin_id, module: module, reason: reason}
        )

        _ = Store.update_status(plugin_id, :load_failed)
        state
    end
  end

  defp load_from_ebin(ebin_dir, expected_modules) do
    Enum.reduce_while(expected_modules, {:ok, []}, fn module, {:ok, acc} ->
      beam_path = Path.join(ebin_dir, "#{module}.beam")

      with {:ok, bin} <- File.read(beam_path),
           {:module, ^module} <- :code.load_binary(module, String.to_charlist(beam_path), bin) do
        {:cont, {:ok, [module | acc]}}
      else
        {:error, reason} -> {:halt, {:error, {module, reason}}}
        other -> {:halt, {:error, {module, other}}}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      other -> other
    end
  end

  # Access Store without blocking if it is still starting or has crashed.
  defp safe_store_all do
    case Process.whereis(Store) do
      nil -> []
      _ -> Store.all()
    end
  end

  ## Collision + load/unload helpers

  defp reject_collisions(beams, plugin_id, registry) do
    # Build the set of modules owned by OTHER installed plugins.
    other_owned =
      registry
      |> Enum.flat_map(fn
        {^plugin_id, _} -> []
        {_id, {_ver, mods}} -> mods
      end)
      |> MapSet.new()

    Enum.reduce_while(beams, :ok, fn {module, _}, _ ->
      cond do
        MapSet.member?(other_owned, module) ->
          {:halt, {:error, {:module_collision, module}}}

        module_from_release?(module) ->
          {:halt, {:error, {:module_collision, module}}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  # Classify a module as "already compiled into the release and not owned by
  # any installed plugin" — those are collisions. A module not yet loaded or
  # previously loaded by THIS plugin is fine.
  defp module_from_release?(module) do
    case :code.which(module) do
      :non_existing -> false
      :preloaded -> true
      :cover_compiled -> false
      path when is_list(path) -> not under_plugin_root?(List.to_string(path))
    end
  end

  defp under_plugin_root?(path) do
    cfg = Application.get_env(:flezha, :plugin_installer, [])
    root = Keyword.get(cfg, :plugin_root, "app_data/plugins")
    installed_root = Path.expand(Path.join(root, "installed"))
    String.starts_with?(Path.expand(path), installed_root)
  end

  defp write_and_load(ebin_dir, beams) do
    Enum.reduce_while(beams, {:ok, []}, fn {module, bin} = pair, {:ok, acc} ->
      beam_path = Path.join(ebin_dir, "#{module}.beam")

      with :ok <- File.write(beam_path, bin),
           {:module, ^module} <-
             :code.load_binary(module, String.to_charlist(beam_path), bin) do
        {:cont, {:ok, [pair | acc]}}
      else
        {:error, reason} ->
          {:halt, {:error, {:partial, Enum.map(acc, &elem(&1, 0)), {:load_failed, module, reason}}}}

        other ->
          {:halt, {:error, {:partial, Enum.map(acc, &elem(&1, 0)), {:load_failed, module, other}}}}
      end
    end)
  end

  defp unload_module(module) do
    _ = :code.purge(module)
    _ = :code.delete(module)
    _ = :code.purge(module)
    :ok
  end
end
