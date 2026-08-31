defmodule FlezhaTest.Runner.FleetManagerMarksTest do
  @moduledoc """
  A close the controller itself asked for is reported differently from one it
  did not — and only reported differently. The teardown a bench or a
  `stop_namespace` performs used to enter the convergence loop as a divergence
  to heal, because the channel could not tell the two apart.

  The load-bearing assertion is the pairing: the marked and unmarked closes
  must leave identical state, or the mark has started deciding something it
  has no business deciding.
  """

  # async: false: writes Application env; the live controller and its fleet; the primary Logger.

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Runner.StreamChannel
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias FlezhaTest.Helper.FleetStateHelper

  setup do
    # The suite runs the primary logger at `:error`, which filters both
    # severities under test before any capture handler sees them.
    previous = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: previous) end)

    :ok
  end

  @doc false
  def forward_stall(_event, _measurements, meta, dest),
    do: send(dest, {:output_stall, meta.operative_instance})

  defp start_channel(operative_id) do
    runner_id = "exec_rc_#{System.unique_integer([:positive])}"
    name = :"rc_#{System.unique_integer([:positive])}"

    {:ok, pid} = StreamChannel.start_link({runner_id, name, :primary})
    :ok = GenServer.call(name, {:operative_pid_update, operative_id, [self()]})

    {runner_id, name, pid}
  end

  # Drive the transport-edge close and return {log, stalled?, published}.
  defp close(name, pid) do
    :telemetry.attach(
      {__MODULE__, name},
      [:flezha, :state, :output_stall],
      &__MODULE__.forward_stall/4,
      self()
    )

    log =
      capture_log(fn ->
        send(pid, {:flz_closed, 1, :normal})
        # Synchronise on the channel having handled it.
        _ = :sys.get_state(pid)
      end)

    :telemetry.detach({__MODULE__, name})

    stalled? =
      receive do
        {:output_stall, _} -> true
      after
        50 -> false
      end

    {log, stalled?, :persistent_term.get({StreamChannel, name})}
  end

  test "an unmarked close is an incident; a marked one is not" do
    operative_id = "op_rc_#{System.unique_integer([:positive])}"

    {_runner_a, name_a, pid_a} = start_channel(operative_id)
    {runner_b, name_b, pid_b} = start_channel(operative_id)

    {log_a, stalled_a, published_a} = close(name_a, pid_a)

    Marks.mark_runner_retiring(runner_b)
    {log_b, stalled_b, published_b} = close(name_b, pid_b)

    # Reporting differs.
    assert log_a =~ "[warning]"
    assert log_a =~ "Connection closed for IO socket"
    assert stalled_a, "an unexpected close must still wake the convergence loop"

    refute log_b =~ "[warning]"
    assert log_b =~ "IO socket closed for retiring runner #{runner_b}"
    refute stalled_b, "a teardown the controller asked for is not a divergence"

    # State does not. Both closes clear the binding identically.
    assert {nil, _ref_a} = published_a
    assert {nil, _ref_b} = published_b
    assert :sys.get_state(pid_a).stream_ref == nil
    assert :sys.get_state(pid_b).stream_ref == nil
  end

  test "an expired mark reports as unexpected" do
    operative_id = "op_rc_#{System.unique_integer([:positive])}"
    {runner_id, name, pid} = start_channel(operative_id)

    # A mark that outlived its disconnect must not silence the diagnosis of a
    # later, genuine close: `runner_id` is a durable identity reused across
    # reconnects.
    Application.put_env(:flezha, :runner_retiring_mark_ttl_ms, -1)
    on_exit(fn -> Application.delete_env(:flezha, :runner_retiring_mark_ttl_ms) end)

    Marks.mark_runner_retiring(runner_id)
    refute Marks.runner_retiring?(runner_id)

    {log, stalled?, _} = close(name, pid)

    assert log =~ "[warning]"
    assert stalled?
  end

  test "clear_retiring drops the mark" do
    runner_id = "exec_rc_#{System.unique_integer([:positive])}"

    Marks.mark_runner_retiring(runner_id)
    assert Marks.runner_retiring?(runner_id)

    Marks.clear_runner_retiring(runner_id)
    refute Marks.runner_retiring?(runner_id)
  end

  test "an unknown runner is never retiring" do
    refute Marks.runner_retiring?("exec_never_marked")
  end

  describe "the controller marks the runners it is about to close" do
    test "stop_namespace marks every active runner before tearing it down" do
      namespace = "ns_rc_#{System.unique_integer([:positive])}"
      runners = ["exec_sn_a", "exec_sn_b"]

      FleetStateHelper.seed(namespace, Map.new(runners, &{&1, ["do_nothing"]}))

      for runner <- runners, do: refute(Marks.runner_retiring?(runner))

      capture_log([level: :error], fn -> FleetManager.stop_namespace(namespace) end)

      for runner <- runners do
        assert Marks.runner_retiring?(runner),
               "#{runner} was closed by the controller and must not be diagnosed as a failure"
      end
    end
  end
end
