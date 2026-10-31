defmodule FlezhaTest.Runner.FleetManagerMarksTest do
  @moduledoc """
  A close the controller itself asked for is reported differently from one it
  did not — and only reported differently. Without the mark the channel cannot
  tell the two apart, and the teardown a bench or a `stop_namespace` performs
  reads as an incident. Neither enters the convergence loop: an unexpected close
  is repaired by the session's own data-plane rebind.

  The load-bearing assertion is the pairing: the marked and unmarked closes
  must leave identical state, or the mark has started deciding something it
  has no business deciding.
  """

  # async: false: writes Application env; the live controller and its fleet; the primary Logger.

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.StreamChannel
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
    do: send(dest, {:output_stall, meta.relay_instance})

  defp start_channel(relay_id) do
    runner_id = "exec_rc_#{System.unique_integer([:positive])}"
    name = :"rc_#{System.unique_integer([:positive])}"

    {:ok, pid} = StreamChannel.start_link({"_system", runner_id, name, :primary})
    :ok = GenServer.call(name, {:relay_pid_update, relay_id, [self()]})
    :ok = GenServer.call(name, {:bind, :marks_stream})

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
        send(pid, {:flz_closed, :marks_stream, :normal})
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
    relay_id = "op_rc_#{System.unique_integer([:positive])}"

    {_runner_a, name_a, pid_a} = start_channel(relay_id)
    {runner_b, name_b, pid_b} = start_channel(relay_id)

    {log_a, stalled_a, published_a} = close(name_a, pid_a)

    Marks.mark_runner_retiring(runner_b)
    {log_b, stalled_b, published_b} = close(name_b, pid_b)

    # Reporting differs.
    assert log_a =~ "[warning]"
    assert log_a =~ "Connection closed for IO socket"
    refute stalled_a, "the session rebinds its data plane; the loop has nothing to heal"

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
    relay_id = "op_rc_#{System.unique_integer([:positive])}"
    {runner_id, name, pid} = start_channel(relay_id)

    # A mark that outlived its disconnect must not silence the diagnosis of a
    # later, genuine close: `runner_id` is a durable identity reused across
    # reconnects.
    Application.put_env(:flezha, :runner_retiring_mark_ttl_ms, -1)
    on_exit(fn -> Application.delete_env(:flezha, :runner_retiring_mark_ttl_ms) end)

    Marks.mark_runner_retiring(runner_id)
    refute Marks.runner_retiring?(runner_id)

    {log, stalled?, _} = close(name, pid)

    assert log =~ "[warning]"
    refute stalled?
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

  describe "a session's identity" do
    setup do
      %{session: "exec_id_#{System.unique_integer([:positive])}"}
    end

    test "answers the identity the session presented until the session is forgotten",
         %{session: session} do
      :ok = Marks.bind_identity(session, "stable-a")
      assert Marks.identity_of(session) == "stable-a"

      :ok = Marks.forget_identity(session)
      assert Marks.identity_of(session) == session
    end

    test "a session that presented none is its own token", %{session: session} do
      :ok = Marks.bind_identity(session, nil)
      assert Marks.identity_of(session) == session
    end

    test "a refusal is first once per session and relay, until the session is forgotten",
         %{session: session} do
      assert Marks.first_refusal?(session, "op-1")
      refute Marks.first_refusal?(session, "op-1")
      assert Marks.first_refusal?(session, "op-2")

      :ok = Marks.forget_identity(session)
      assert Marks.first_refusal?(session, "op-1")
    end
  end
end
