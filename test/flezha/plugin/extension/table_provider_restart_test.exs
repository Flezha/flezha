defmodule Flezha.Plugin.Extension.TableProviderRestartTest do
  @moduledoc """
  `Flezha.Plugin.Extension.Table.attempt_restart/3`, the one restart path,
  reached by the convergence loop's `restart_plugin`: it re-runs
  `init_provider/1` and re-registers the hooks, so the caller's
  activation starts a fresh worker where the dead one was. Reshuffling the
  subscription tables alone leaves the socket dead until the container is
  restarted by hand.

  The table itself restarts nothing: a `DOWN` is recorded as `{:down, reason}`
  and no timer is armed.
  """

  # fails when: provider plugin stays dead after watchdog kill (no new worker started)

  use ExUnit.Case, async: false

  # Killing a provider worker is the point of these cases, and its supervisor
  # reports every abnormal child exit at error level.
  @moduletag :capture_log

  import ExUnit.CaptureLog

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour

  @catalog Host.build_catalog(
             [
               {:on_input, :transform, Ctx.IO}
             ],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  defmodule FakeWorker do
    @moduledoc false
    use GenServer

    def start_link(opts),
      do: GenServer.start_link(__MODULE__, opts, name: Map.fetch!(opts, :name))

    @impl true
    def init(opts), do: {:ok, opts}
  end

  defmodule FakeProvider do
    @moduledoc false
    use ProviderBehaviour,
      options: [plugin_id: [type: :string, internal: true]],
      worker: [module: FakeWorker, restart: :temporary]

    @impl true
    def worker_specs(opts), do: [{opts.plugin_id, %{plugin_id: opts.plugin_id}}]

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule FailingProvider do
    @moduledoc false
    use ProviderBehaviour, options: [plugin_id: [type: :string, internal: true]]

    @impl true
    def init_provider(_opts), do: {:error, :econnrefused}

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  setup do
    start_supervised!({WorkerSup, name: WorkerSup.family(FakeProvider)})
    :ok
  end

  defp plugin(plugin_id, provider) do
    %Plugin{
      id: plugin_id,
      provider: provider,
      opts: %{plugin_id: plugin_id},
      hooks: [{:on_input, []}]
    }
  end

  defp worker_ref(table, plugin_id) do
    [{_key, {pid, ref}}] = Map.to_list(table.entries[plugin_id].workers)
    {pid, ref}
  end

  test "a crashed worker is recorded down, armed with nothing, and the restart starts a fresh one" do
    plugin_id = "fake_provider_ws"
    plugin = plugin(plugin_id, FakeProvider)

    {:ok, table} = Table.bulk_subscribe(Table.new(@catalog), [{plugin_id, plugin}])

    # Preparation holds nothing: the worker is the activation's.
    assert Table.phase(table, plugin_id) == :prepared
    assert Table.workers(table, plugin_id) == %{}

    {table, []} = Table.bulk_activate(table)
    {original_pid, original_ref} = worker_ref(table, plugin_id)
    assert Process.alive?(original_pid)
    assert Table.owned_pid(table, plugin_id) == original_pid

    # The watchdog fires: kill the worker with the same shutdown reason
    # `Flezha.Plugin.Provider.Websocket.Client.handle_info(:idle_check)` uses,
    # then drive `Table.handle_down` with the :DOWN the BEAM monitor delivers.
    Process.exit(original_pid, {:shutdown, :idle_timeout})
    assert_receive {:DOWN, ^original_ref, :process, ^original_pid, _}, 1_000

    {{:ok, table}, _log} =
      with_log(fn ->
        Table.handle_down(table, original_ref, original_pid, {:shutdown, :idle_timeout})
      end)

    # The dead worker is evicted and the plugin recorded down, nothing scheduled.
    assert Table.workers(table, plugin_id) == %{}
    refute Map.has_key?(table.monitors, original_ref)
    assert Map.has_key?(table.plugins, plugin_id)
    assert Table.phase(table, plugin_id) == {:down, {:shutdown, :idle_timeout}}
    assert Table.phase_reasons(table, :down) == %{plugin_id => {:shutdown, :idle_timeout}}
    assert Table.running_ids(table) == []
    assert {:messages, []} = Process.info(self(), :messages), "the table armed a timer"

    # The loop's restart: re-prepared, holding nothing, until the caller
    # activates it, which is when a NEW worker is started under a fresh monitor.
    {:ok, table} = Table.attempt_restart(table, plugin_id, plugin)
    assert Table.phase(table, plugin_id) == :prepared
    assert Table.workers(table, plugin_id) == %{}
    assert Table.phase_reasons(table, :down) == %{}

    {table, []} = Table.bulk_activate(table)
    {new_pid, new_ref} = worker_ref(table, plugin_id)
    assert new_pid != original_pid
    assert Process.alive?(new_pid)
    assert new_ref != original_ref
    assert Map.fetch!(table.monitors, new_ref) == {plugin_id, plugin_id}
    assert Table.phase(table, plugin_id) == :running
  end

  test "a restart whose init_provider fails leaves the plugin recorded down for the next rung" do
    plugin_id = "failing_provider"
    plugin = plugin(plugin_id, FailingProvider)

    # Bootstrap by stuffing the plugin into the table directly: a subscribe
    # would refuse a plugin whose first `init_provider/1` call returns :error.
    table =
      Table.new(@catalog)
      |> Map.update!(:plugins, &Map.put(&1, plugin_id, plugin))

    {{:error, :econnrefused, table}, _log} =
      with_log(fn -> Table.attempt_restart(table, plugin_id, plugin) end)

    assert Table.workers(table, plugin_id) == %{}
    assert Map.has_key?(table.plugins, plugin_id)
    assert Table.phase(table, plugin_id) == {:down, :econnrefused}
  end

  test "a running subscriber's uptime is reported; a down one's is not" do
    plugin_id = "uptime_ws"

    {:ok, table} =
      Table.bulk_subscribe(Table.new(@catalog), [{plugin_id, plugin(plugin_id, FakeProvider)}])

    {table, []} = Table.bulk_activate(table)

    assert %{^plugin_id => uptime} = Table.uptimes(table)
    assert is_integer(uptime) and uptime >= 0

    {pid, ref} = worker_ref(table, plugin_id)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    {{:ok, table}, _log} = with_log(fn -> Table.handle_down(table, ref, pid, :killed) end)

    assert Table.uptimes(table) == %{}
  end
end
