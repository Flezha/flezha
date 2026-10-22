defmodule FlezhaTest.Service.PluginListRunsOnTest do
  @moduledoc """
  `flz plugin list` names the node a relay plugin runs on: a file sink bound
  to a running relay, or to every member of a partitioned one, runs on the
  relays' node, not "idle".
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  import ExUnit.CaptureLog

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.{Ctx, Table}
  alias Flezha.Plugin.Provider.File.Sink, as: FileSink
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Relay.Builder
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Cli
  alias Flezha.Service.PluginManager
  alias Flezha.Service.PluginRecords
  alias Flezha.Service.RelayManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.CliCapture
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "plugin_runs_on_ns"
  @factory "store_input"

  # A relay plugin whose worker, once killed, cannot start again: its binding
  # stays down on a running relay.
  defmodule DownProvider do
    use GenServer
    use Flezha.Plugin.ProviderBehaviour, worker: [module: __MODULE__, restart: :temporary]

    @impl true
    def worker_specs(opts), do: [{opts.source_plugin_id, %{}}]

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}

    def start_link(%{name: name}), do: GenServer.start_link(__MODULE__, :ok, name: name)

    @impl GenServer
    def init(:ok) do
      if :persistent_term.get({__MODULE__, :refuse}, false),
        do: {:stop, :refused},
        else: {:ok, nil}
    end
  end

  setup_all do
    TestHelpers.reset_all_cells()
    start_supervised!({WorkerSup, name: WorkerSup.family(DownProvider)})
    NodeTestHelper.start_runner(@namespace)
    on_exit(fn -> FleetManager.stop_namespace(@namespace) end)

    :ok
  end

  setup %{tmp: tmp} do
    Application.put_env(:flezha, :file_plugin_root, tmp)

    on_exit(fn ->
      RelayManager.stop_all_relays(@namespace)
      Enum.each(["writer", "down"], &PluginManager.disable_plugin(@namespace, &1))
      :persistent_term.erase({DownProvider, :refuse})
    end)

    :ok
  end

  test "a file sink bound to a running relay runs on the relay's node" do
    relay = "runs_on_echo"
    {:ok, _} = relay |> RelayFixture.get(@factory) |> RelayFixture.start(@namespace)
    enable_writer()
    :ok = PluginManager.bind_plugin(@namespace, "writer", relay, hooks: [{:on_output, []}])

    # The sink's worker is up on this node: it runs.
    assert WorkerSup.keys(WorkerSup.family(FileSink)) != []

    {row, out} = listed()
    assert row.relays == [relay]
    assert row.controllers == [node()]
    assert out =~ "runs on: #{node()}"
  end

  test "a file sink bound to a partitioned relay runs on its members' node" do
    group = "runs_on_grp"
    on_exit(fn -> RelayManager.stop_relay(@namespace, group) end)

    {:ok, _} =
      PartitionGroup.start(
        @namespace,
        %Builder{id: group, factory_id: "partition_pass", init_values: %{}, opts: []},
        2
      )

    enable_writer()
    :ok = PluginManager.bind_plugin(@namespace, "writer", group, hooks: [{:on_output, []}])

    {row, out} = listed()
    assert row.relays == ["#{group}#p0", "#{group}#p1"]
    assert row.controllers == [node()]
    assert out =~ "runs on: #{node()}"
  end

  test "a plugin bound to a running relay but down there runs nowhere" do
    relay = "runs_on_down"
    {:ok, _} = relay |> RelayFixture.get(@factory) |> RelayFixture.start(@namespace)

    {:ok, _} =
      PluginManager.enable_plugin("down", %Plugin{
        id: "down",
        provider: DownProvider,
        namespace: @namespace,
        opts: %{}
      })

    :ok = PluginManager.bind_plugin(@namespace, "down", relay, hooks: [{:on_input, []}])
    assert [key] = WorkerSup.keys(WorkerSup.family(DownProvider))
    worker = WorkerSup.worker_pid(DownProvider, key)

    :persistent_term.put({DownProvider, :refuse}, true)

    log =
      capture_log(fn ->
        Process.exit(worker, :kill)

        assert NodeTestHelper.wait_until(
                 fn -> not Table.running?(table(relay), "down") end,
                 5_000
               )
      end)

    assert log =~ "[Extension.Table] subscriber down crashed: :killed"

    {row, out} = listed("down")
    assert row.relays == [relay]
    assert row.controllers == []
    assert out =~ "runs on: idle"
  end

  defp table(relay) do
    {:ok, %{extensions: table}} = RelayManager.get_overwatch_state(@namespace, relay)
    table
  end

  defp enable_writer() do
    {:ok, _} =
      PluginManager.enable_plugin("writer", %Plugin{
        id: "writer",
        provider: FileSink,
        namespace: @namespace,
        opts: %{path: "echo-out.txt"}
      })
  end

  # The plugin's row of `flz plugin list` and its rendered line.
  defp listed(name \\ "writer") do
    row = Enum.find(PluginRecords.plugins(@namespace), &(&1.name == name))
    {_, out, _err} = CliCapture.run(fn -> Cli.Plugin.enabled([row], [], []) end)
    {row, out}
  end
end
