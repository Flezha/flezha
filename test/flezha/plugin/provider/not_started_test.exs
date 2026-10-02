defmodule Flezha.Plugin.Provider.NotStartedTest do
  @moduledoc """
  A sink binding that could not come up keeps a guaranteed output owed: the
  output is held and never settled, so the outbox cursor stays behind it and a
  re-drive hands it over again. A plain output to it is dropped and counted.
  """
  use FlezhaTest.OutboxCase, async: false

  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias FlezhaTest.Helper.Telemetry

  @reason {:module_not_loaded, MyOrg.Absent, :nonode@nohost}

  defp not_started_sink do
    binding = %Plugin{
      id: "out",
      provider: MyOrg.Absent,
      namespace: "ns",
      operator_id: "op",
      hooks: [{:on_output, []}]
    }

    Table.put_not_started(Table.new(Overwatch.__extensions_catalog__()), "out", binding, @reason)
  end

  defp dispatch(table, payload, metadata),
    do:
      Extension.run(table, :on_output, %Ctx.IO{
        operator_id: "op",
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

  test "a plain output is dropped and counted" do
    :ok = Telemetry.capture([:flezha, :plugin, :not_started, :dropped])

    assert {:ok, _ctx} = dispatch(not_started_sink(), "plain", %{})

    assert_receive {:telemetry, [:flezha, :plugin, :not_started, :dropped], %{count: 1},
                    %{plugin_id: "out", operator_id: "op"}}
  end
end
