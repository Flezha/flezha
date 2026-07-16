defmodule FlezhaTest.Operative.OverwatchRestoreAsyncTest do
  @moduledoc """
  The per-Segment restore replay must run off the Overwatch process so its
  blocking disk reads cannot freeze the GenServer mailbox.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Builder

  @slow_read_ms 2_000

  test "kick_restore does not block the Overwatch on a slow restore disk scan" do
    # Private stub bites only the in-process (regression) path; the spawned
    # worker calls through to the real (fast) scan.
    stub(Flezha.State.Registry, :all_segments, fn ->
      Process.sleep(@slow_read_ms)
      []
    end)

    state = %Overwatch{
      id: "btc_kraken",
      namespace: "default",
      runner_id: "ex_1",
      factory_id: "market",
      builder: %Builder{id: "btc_kraken", factory_id: "market", opts: []},
      arrived_via_hotswap?: false
    }

    {elapsed_us, {:noreply, ^state}} =
      :timer.tc(fn -> Overwatch.handle_cast({:kick_restore, "ex_1"}, state) end)

    assert elapsed_us < 500_000,
           "kick_restore blocked the Overwatch for #{div(elapsed_us, 1000)}ms on the restore disk scan"
  end
end
