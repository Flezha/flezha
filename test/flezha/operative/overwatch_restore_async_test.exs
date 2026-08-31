defmodule FlezhaTest.Operative.OverwatchRestoreAsyncTest do
  @moduledoc """
  The per-Entity restore replay must run off the Overwatch process so its
  blocking disk reads cannot freeze the GenServer mailbox.
  """
  use ExUnit.Case, async: true
  use Mimic

  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Builder
  alias FlezhaTest.Helper.NodeTestHelper

  test "kick_restore does not block the Overwatch on a slow restore disk scan" do
    # Private stub bites only the in-process (regression) path; the spawned
    # worker calls through to the real (fast) scan. A regression therefore
    # hangs here rather than returning late.
    stub(Flezha.State.Paths, :all_entities, fn ->
      NodeTestHelper.park()
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
