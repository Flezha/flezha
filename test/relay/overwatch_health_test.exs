defmodule FlezhaTest.Relay.OverwatchHealthTest do
  @moduledoc """
  Self-health observability for the wedge-prone Overwatch control process:
  the periodic `:heartbeat` (mailbox depth + drift) and the `with_op_span`
  blocking-operation logging. Both handlers are public functions, so they are
  exercised directly without standing up the dist_sup-spawned relay.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Plugin.Extension.Table
  alias Flezha.Relay.Overwatch

  setup do
    prev = Logger.level()
    Logger.configure(level: :warning)
    on_exit(fn -> Logger.configure(level: prev) end)
  end

  test "heartbeat warns and records last_hb when the process is behind" do
    now = System.monotonic_time(:millisecond)

    # last beat was 60s ago with the default 15s interval -> ~45s drift -> warn.
    state = %Overwatch{
      id: "r_hb",
      namespace: "ns",
      executor_id: "ex_hb",
      extensions: Table.new(%{}),
      last_hb: %{at_ms: now - 60_000, reductions: 0}
    }

    {result, log} =
      with_log(fn -> Overwatch.handle_info(:heartbeat, state) end)

    assert {:noreply, %Overwatch{last_hb: %{at_ms: at, reductions: reds}}} = result
    assert is_integer(at) and is_integer(reds)
    assert log =~ "[Overwatch] heartbeat behind relay=r_hb"
    assert log =~ "queue_len="
    assert log =~ "drift_ms="
  end

  test "op span logs op_slow when the operation exceeds the warn threshold" do
    prev = Application.get_env(:flezha, :overwatch_op_warn_ms)
    Application.put_env(:flezha, :overwatch_op_warn_ms, 0)
    on_exit(fn -> restore_env(:overwatch_op_warn_ms, prev) end)

    # :get_partition_state wraps send_control in with_op_span; with no control
    # unit registered for "ex_op" the inner call returns fast, and a 0 ms warn
    # threshold forces the op_slow branch.
    state = %Overwatch{
      id: "r_op",
      namespace: "ns",
      executor_id: "ex_op",
      extensions: Table.new(%{})
    }

    log =
      capture_log(fn ->
        assert {:reply, {:error, _}, ^state} =
                 Overwatch.handle_call(:get_partition_state, {self(), make_ref()}, state)
      end)

    assert log =~ "[Overwatch] op_slow op=partition_state relay=r_op duration_ms="
  end

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, val), do: Application.put_env(:flezha, key, val)
end
