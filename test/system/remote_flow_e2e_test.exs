defmodule FlezhaTest.System.RemoteFlowE2ETest do
  @moduledoc """
  `durable: remote` on a flow class, end to end on the `local_path` backend:
  an input is acknowledged only after the remote copy holds the records it
  produced, so a controller that loses its disk comes back holding every
  acknowledged input and delivers none of them again.

  Runs against whichever runtime `FLEZHA_E2E_RUNTIME` selects, with the
  `remote_tier_store` fixture every SDK registers.
  """

  use FlezhaTest.SystemCase, capability: :effectively_once

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Operator.Builder
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.DurabilityTier.Backend.LocalPath
  alias Flezha.State.DurabilityTier.Manifest
  alias Flezha.State.{InputLog, Paths}

  @moduletag timeout: 300_000

  test "an acknowledged input survives the controller's disk and is not delivered again" do
    backend_opts = install_durability_tier(:local_path)
    namespace = unique("dur_remote_flow")
    operator_id = unique("remote_tier_store")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operator_id, id_path) end)

    {port_a, exec_a} = boot_runner(namespace, id_path)

    sink = %Flezha.Plugin{
      id: "remote_sink",
      namespace: namespace,
      operator_id: operator_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    builder = %Builder{
      id: operator_id,
      factory_id: "remote_tier_store",
      init_values: %{},
      opts: [plugins: [sink]]
    }

    assert {:ok, %{runner_id: ^exec_a}} = OperatorManager.start_operator(namespace, builder)
    assert wait_until(fn -> ClassRegistry.remote?(operator_id) end, 30_000)

    send_inputs(operator_id, namespace, ["k1:a", "k2:b"])
    assert await_changelog(%{"k1" => "k1=a", "k2" => "k2=b"}, 60_000)
    assert wait_until(fn -> InputLog.watermark(operator_id) == 1 end, 60_000)

    # Each acknowledged input's Cell is on the backend, holding the offset it
    # consumed: the acknowledgement waited for exactly this.
    for cell_id <- Paths.cells_of(operator_id) do
      assert {:ok, blob} = LocalPath.get(Manifest.key({:cell, cell_id}), backend_opts)
      assert {:ok, %{source_offsets: %{consumed_offset: offset}}} = Manifest.decode(blob)
      assert offset in [0, 1]
    end

    # --- the controller's disk for this operator is gone --------------------
    sigkill(os_pid(port_a))
    assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end, 60_000)
    destroy_local_state(operator_id)
    :ets.delete(InputLog.watermark_table(), {operator_id, :processed_prefix})
    refute File.exists?(Paths.class_dir(operator_id))
    assert InputLog.watermark(operator_id) == nil

    {_port_b, exec_b} = boot_runner(namespace, id_path)
    assert exec_b != exec_a

    assert wait_until(
             fn ->
               case OperatorManager.get_cell_state(namespace, operator_id) do
                 {:ok, json} -> json =~ "k1" and json =~ "k2"
                 _ -> false
               end
             end,
             120_000
           ),
           "the acknowledged state did not come back from the backend"

    # Nothing it acknowledged is owed again: the floor came back with the
    # Cells, so the bind re-forwards no input.
    assert InputLog.tail(operator_id) == {:ok, 1, 1}
    assert InputLog.undelivered(operator_id) == []

    send_inputs(operator_id, namespace, ["k3:c"])
    assert await_changelog(%{"k3" => "k3=c"}, 60_000)
    assert wait_until(fn -> InputLog.watermark(operator_id) == 2 end, 60_000)
  end
end
