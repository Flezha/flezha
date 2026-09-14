defmodule Flezha.Plugin.Installer.Store do
  @moduledoc """
  DETS-backed catalog of installed plugins, keyed by `plugin_id` (binary).
  Tracks which packages have been installed on this node and which are
  enabled — orthogonal to which `%Flezha.Plugin{}` hooks are
  live right now (that lives in `Flezha.Service.PluginManager`'s state).

  Rows live at `<plugin_root>/installed_plugins.dets`, alongside the
  installed package trees the installer manages.

  Every mutator ends with `:dets.sync/1` so the row is on disk before the
  call returns — installs must survive an immediate crash. Opening a corrupt
  file rotates it to `installed_plugins.dets.corrupt-<monotonic>` and starts
  fresh; the on-disk `app_data/plugins/installed/` trees remain so runners
  can rehydrate by re-installing.
  """

  use GenServer
  use Flezha.State.Store

  alias Flezha.Config
  alias Flezha.DetsStore
  alias Flezha.Config.Durations
  alias Flezha.Layout.Table, as: Layout

  @table :flz_installed_plugins
  @dets_filename "installed_plugins.dets"

  @type plugin_id() :: binary()
  @type status() :: :enabled | :disabled | :load_failed | :pending_upgrade
  @type row() :: %{
          required(:version) => binary(),
          required(:status) => status(),
          required(:path) => binary(),
          required(:manifest_digest) => binary(),
          required(:modules) => [atom()],
          required(:installed_at) => integer(),
          required(:source_checksums) => %{optional(binary()) => binary()},
          optional(:description) => binary(),
          optional(:author) => binary(),
          optional(:provides) => [map()],
          optional(:dependencies) => [{binary(), binary()}]
        }

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Insert or overwrite the catalog row for `plugin_id`. Row must include at
  least `:version`, `:status`, `:path`, `:manifest_digest`, `:modules`,
  `:installed_at`, `:source_checksums`. Synchronous: returns only after
  `:dets.sync/1` completes.
  """
  @spec put(plugin_id(), row()) :: :ok | {:error, term()}
  def put(plugin_id, row) when is_binary(plugin_id) and is_map(row) do
    GenServer.call(__MODULE__, {:put, plugin_id, row})
  end

  @doc "Return the row for `plugin_id` or `nil`."
  @spec get(plugin_id()) :: row() | nil
  def get(plugin_id) when is_binary(plugin_id) do
    if table_ready?() do
      case DetsStore.fetch(@table, plugin_id) do
        {:ok, row} -> row
        :error -> nil
      end
    else
      nil
    end
  end

  @doc "Delete the row for `plugin_id`. Synchronous; idempotent."
  @spec delete(plugin_id()) :: :ok
  def delete(plugin_id) when is_binary(plugin_id) do
    GenServer.call(__MODULE__, {:delete, plugin_id})
  end

  @doc "Return all rows as `[{plugin_id, row}]`."
  @spec all() :: [{plugin_id(), row()}]
  def all do
    if table_ready?() do
      DetsStore.all(@table)
    else
      []
    end
  end

  @doc "Row count."
  @spec count() :: non_neg_integer()
  def count do
    if table_ready?() do
      DetsStore.count(@table)
    else
      0
    end
  end

  @doc """
  Update only the `:status` field of an existing row; no-op if the row is
  absent. Synchronous: `:dets.sync/1` before returning.
  """
  @spec update_status(plugin_id(), status()) :: :ok | {:error, :not_found}
  def update_status(plugin_id, status)
      when is_binary(plugin_id) and is_atom(status) do
    GenServer.call(__MODULE__, {:update_status, plugin_id, status})
  end

  @doc "Close the DETS table. Primarily for tests."
  @spec close() :: :ok | {:error, term()}
  def close, do: GenServer.call(__MODULE__, :close)

  ## ----- store declaration -----

  @impl Flezha.State.Store
  def subtree(), do: :plugins

  @impl Flezha.State.Store
  def filename(), do: @dets_filename

  @impl Flezha.State.Store
  def table(), do: @table

  # The catalog rotates to `installed_plugins.dets.corrupt-<monotonic>` and
  # auto-saves on the installer's own interval; both are published, so the
  # store declares them rather than taking the seam's defaults.
  @impl Flezha.State.Store
  def open_opts(),
    do: [auto_save: Durations.get(:installer_auto_save_ms), corrupt_suffix: :monotonic]

  # Tests isolate the catalog with a directory of their own; production keeps
  # it beside the installed package trees, which is the `:plugins` root.
  @impl Flezha.State.Store
  def dir(), do: Config.get(:plugin_installer_data_dir) || Layout.dir(:plugins)

  @impl Flezha.State.Store
  def after_open(%{path: path}) do
    :telemetry.execute(
      [:flezha, :plugin_installer, :store_opened],
      %{rows: DetsStore.count(@table)},
      %{path: path}
    )
  end

  ## ----- GenServer -----

  @impl true
  def handle_call({:put, plugin_id, row}, _from, state) do
    {:reply, DetsStore.put(@table, plugin_id, row), state}
  end

  def handle_call({:delete, plugin_id}, _from, state) do
    :ok = DetsStore.delete(@table, plugin_id)
    {:reply, :ok, state}
  end

  def handle_call({:update_status, plugin_id, status}, _from, state) do
    reply =
      case DetsStore.fetch(@table, plugin_id) do
        {:ok, row} -> DetsStore.put(@table, plugin_id, Map.put(row, :status, status))
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:close, _from, state), do: {:reply, Flezha.State.Store.close(__MODULE__), state}

  ## Private

  defp table_ready? do
    not is_nil(Process.whereis(__MODULE__))
  end
end
