defmodule Flezha.Plugin.Provider.NotStartedTest do
  @moduledoc """
  A sink binding that could not come up keeps a guaranteed output owed: the
  output is held and never settled, so the outbox cursor stays behind it and a
  re-drive hands it over again. A plain output to it is dropped and counted on
  its relay's liveness counters, with nothing emitted per output.
  """
  use FlezhaTest.OutboxCase, async: false

  alias Flezha.Relay.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.Telemetry

  @reason {:module_not_loaded, MyOrg.Absent, :nonode@nohost}

  defp not_started_sink(counters \\ nil) do
    binding = %Plugin{
      id: "out",
      provider: MyOrg.Absent,
      namespace: "ns",
      relay_id: "op",
      hooks: [{:on_output, []}]
    }

    Overwatch.__extensions_catalog__()
    |> Table.new()
    |> Table.put_not_started("out", binding, @reason, counters)
  end

  defp dispatch(table, payload, metadata),
    do:
      Extension.run(table, :on_output, %Ctx.IO{
        relay_id: "op",
        namespace: "ns",
        message: payload,
        metadata: metadata
      })

  test "a guaranteed output waits in the outbox and is driven again", %{rc: rc, cell_id: cell_id} do
    table = not_started_sink()
    assert Table.phase(table, "out") == {:not_started, @reason}
    assert Table.health(table) == %{"out" => {:unhealthy, @reason}}

    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "owed")], 1)
    assert {"owed", md, _} = next_output()

    assert {:ok, _ctx} = dispatch(table, "owed", md)
    settle(md)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: -1, awaiting: 1} = WalWriter.outbox_status(pid)

    :ok = WalWriter.close(pid)
    _pid = reopen_with_stale_cursor(cell_id)
    assert {"owed", _md, _} = next_output()
  end

  test "a plain output is dropped and counted on its relay's liveness counters" do
    :ok = Telemetry.capture([:flezha, :plugin, :not_started, :dropped])
    table = not_started_sink(LivenessCounters.register_relay("not_started_op"))

    assert {:ok, _ctx} = dispatch(table, "plain", %{})
    assert {:ok, _ctx} = dispatch(table, "plain", %{})

    assert %{not_started_drops: 2} =
             Enum.find(
               LivenessCounters.snapshot_all().relays,
               &(&1.relay_id == "not_started_op")
             )

    refute_received {:telemetry, _event, _measurements, _metadata}
  end
end
