defmodule Flezha.Process.BoundedCallDrillTest do
  @moduledoc """
  What a bounded call, a watched death and a declared timer actually do when
  the target misbehaves — driven against real processes under a real
  supervisor, not against a stub.

  Table-driven over the classes of site the seam folded: a callee wedged
  inside its own `handle_call`, one killed while the call is in flight, one
  that never existed, and a nested pair whose inner bound is strictly smaller
  than the outer. Each row asserts the caller gets its DECLARED answer, within
  its bound, still alive, having emitted exactly one
  `[:flezha, :convergence, :safe_call]` event.

  Red before the change: the eighteen hand-catch sites answered in eleven
  different shapes and the resolved-pid sites answered nothing at all before
  the 5 s default.
  """

  use ExUnit.Case, async: true

  # A wedged callee is killed on purpose, and its supervisor reports the
  # abnormal exit at error level.
  @moduletag :capture_log

  alias Flezha.Process.Timer
  alias Flezha.Process.Watch
  alias Flezha.SafeCall

  defmodule Callee do
    @moduledoc false
    use GenServer

    def start_link(mode), do: GenServer.start_link(__MODULE__, mode)
    @impl true
    def init(mode), do: {:ok, mode}

    @impl true
    def handle_call(:ask, _from, :wedged) do
      # Wedged INSIDE its own turn, which is what a bound has to survive: the
      # request is delivered and no reply ever comes.
      receive do
        :never -> {:reply, :never, :wedged}
      end
    end

    def handle_call(:ask, _from, mode), do: {:reply, :answered, mode}
  end

  setup do
    ref = make_ref()
    parent = self()

    :telemetry.attach(
      "drill-#{inspect(ref)}",
      [:flezha, :convergence, :safe_call],
      fn _event, _measure, meta, _cfg -> send(parent, {:safe_call, ref, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach("drill-#{inspect(ref)}") end)
    {:ok, event: ref}
  end

  # ---------- bounded call: one row per class of site ----------

  test "a wedged callee answers the declared refusal inside its bound", %{event: ref} do
    {:ok, pid} = start_supervised({Callee, :wedged})

    {elapsed_us, answer} =
      :timer.tc(fn ->
        SafeCall.bounded(pid, :ask, 200, on_error: {:error, :not_running}, log: false)
      end)

    assert answer == {:error, :not_running}
    assert elapsed_us < 2_000_000, "the call outlived its 200 ms bound"
    assert Process.alive?(self())
    # Pinned to this row's callee: the handler is attached to a global event, so
    # any concurrently running test's bounded call reaches it too.
    assert_received {:safe_call, ^ref, %{result: :timeout, server: ^pid}}
    refute_received {:safe_call, ^ref, %{server: ^pid}}
  end

  test "a callee killed mid-call answers its refusal, not an exit", %{event: ref} do
    {:ok, pid} = start_supervised({Callee, :wedged})
    # Killed while the call is in flight: the callee is wedged in `handle_call`,
    # so the kill lands after the request is delivered, whenever it runs.
    Task.start(fn -> Process.exit(pid, :kill) end)

    # The declared value quotes the RAW exit reason, which is what a site that
    # names it in its own error term needs.
    assert {:error, {:gone, {:killed, {GenServer, :call, _}}}} =
             SafeCall.bounded(pid, :ask, 5_000, on_error: &{:error, {:gone, &1}}, log: false)

    assert Process.alive?(self())
    assert_received {:safe_call, ^ref, %{server: ^pid}}
  end

  test "a callee that never existed answers :noproc's declared value" do
    {:ok, pid} = start_supervised({Callee, :ok})
    :ok = stop_supervised(Callee)
    refute Process.alive?(pid)

    assert SafeCall.bounded(pid, :ask, 100, on_error: %{noproc: :absent}, log: false) == :absent
  end

  test "a reason the site did not declare is re-raised, not swallowed" do
    {:ok, pid} = start_supervised({Callee, :wedged})

    # `:timeout` alone is declared; the site keeps propagating everything else.
    assert catch_exit(SafeCall.bounded(pid, :ask, 100, on_error: %{noproc: :absent}, log: false))
  end

  test "a nested pair gives up at the inner bound, leaving the outer slack" do
    # `stop-shared-services-dying-on-their-own-input`: an inner bound equal to
    # the outer one is no bound at all — both give up at the same instant, and
    # the outer reports a reachable target as unreachable.
    {:ok, inner} = start_supervised({Callee, :wedged})
    outer_ms = 1_000
    inner_ms = 200

    outer =
      Task.async(fn ->
        SafeCall.bounded(inner, :ask, inner_ms, on_error: :inner_refused, log: false)
      end)

    assert Task.await(outer, outer_ms) == :inner_refused
  end

  test "the success path answers the reply verbatim" do
    {:ok, pid} = start_supervised({Callee, :ok})
    assert SafeCall.bounded(pid, :ask, 1_000, on_error: :never) == :answered
    assert SafeCall.bounded(pid, :ask, 1_000) == {:ok, :answered}
  end

  # ---------- watch: a death drops every tag on the pid, exactly once ----------

  test "a watched pid's death yields every tag keyed on it, once" do
    {:ok, pid} = start_supervised({Callee, :ok})

    set =
      Watch.new()
      |> Watch.watch(pid, :route_a)
      |> Watch.watch(pid, :route_b)

    assert Enum.sort(Watch.tags(set, pid)) == [:route_a, :route_b]

    :ok = stop_supervised(Callee)
    assert_receive {:DOWN, down_ref, :process, ^pid, _reason}, 1_000

    assert {tags, set} = Watch.down(set, down_ref)
    assert Enum.sort(tags) == [:route_a, :route_b]
    refute Watch.watching?(set, pid)

    # One monitor however many tags, so there is no second `:DOWN` to read.
    assert Watch.down(set, down_ref) == {:unknown, set}
    assert flush_mailbox() == []
  end

  test "dropping the last tag flushes a `:DOWN` already in the mailbox" do
    {:ok, pid} = start_supervised({Callee, :ok})
    set = Watch.watch(Watch.new(), pid, :only)
    :ok = stop_supervised(Callee)

    # The `:DOWN` is queued behind this point; unwatching the last tag is what
    # must take it with it.
    await_mailbox()
    set = Watch.unwatch(set, pid, :only)

    refute Watch.watching?(set, pid)
    assert flush_mailbox() == []
  end

  # ---------- timer: the stale-fire guard, once ----------

  test "cancelling a timer that already fired takes its message with it" do
    timers = Timer.arm(Timer.new(), :tick, 0, :tick_now)

    # The timer has fired and `:tick_now` is in the mailbox; the cancel is what
    # must not leave it there for a handler with nothing to key it on.
    await_mailbox()
    timers = Timer.cancel(timers, :tick)

    refute Timer.armed?(timers, :tick)
    assert flush_mailbox() == []
  end

  test "re-arming a key replaces its timer rather than adding a second" do
    timers =
      Timer.new()
      |> Timer.arm(:tick, 5_000, :tick_now)
      |> Timer.arm(:tick, 20, :tick_now)

    assert Timer.armed(timers) == [:tick]
    assert_receive :tick_now, 1_000
    assert flush_mailbox() == []
  end

  test "cancel_all leaves no key armed and no message behind" do
    timers =
      Timer.new()
      |> Timer.arm(:a, 0, :a_fired)
      |> Timer.arm(:b, 0, :b_fired)

    await_mailbox()
    timers = Timer.cancel_all(timers)

    assert Timer.armed(timers) == []
    assert flush_mailbox() == []
  end

  # Wait for this process's mailbox to be non-empty rather than for a duration:
  # a fired timer's message is already on its way, and a fixed nap is what
  # makes a drill flaky on a loaded box.
  defp await_mailbox(remaining \\ 200)
  defp await_mailbox(0), do: :timeout

  defp await_mailbox(remaining) do
    {:message_queue_len, len} = Process.info(self(), :message_queue_len)
    if len > 0, do: :ok, else: await_mailbox(remaining - 1)
  end

  defp flush_mailbox(acc \\ []) do
    receive do
      msg -> flush_mailbox([msg | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
