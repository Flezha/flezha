defmodule Flezha.SDK.EntryIdleTest do
  @moduledoc """
  The two layers the Elixir SDK re-expresses: the stamp, the latch over it and
  the accessor — pure, on chosen instants — and the idle process's contained
  delivery, driven by explicit ticks. Channel names are unique per test because
  the cells are process-global by name.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Flezha.SDK.{EntryIdle, EntryInput}

  @second 1_000_000_000

  defp unique(name), do: "entry_idle_test_#{name}_#{System.unique_integer([:positive])}"

  defp watch(channel, threshold_ms),
    do: EntryIdle.watch(channel, EntryInput.cell_for(channel), threshold_ms, fn _ -> :ok end)

  test "a cold start never fires however long it lasts" do
    w = watch(unique("cold"), 1_000)
    assert {^w, nil} = EntryIdle.evaluate(w, 3600 * @second)
  end

  test "an episode fires once and a resume re-arms it" do
    channel = unique("episode")
    w = watch(channel, 1_000)
    EntryInput.stamp_at(channel, @second)

    assert {^w, nil} = EntryIdle.evaluate(w, @second + div(@second, 2))
    assert {w, {:idle, gap}} = EntryIdle.evaluate(w, 3 * @second)
    assert gap == 2 * @second
    assert {^w, nil} = EntryIdle.evaluate(w, 4 * @second)
    assert {^w, nil} = EntryIdle.evaluate(w, 60 * @second)

    EntryInput.stamp_at(channel, 60 * @second)
    assert {w, {:resumed, silence}} = EntryIdle.evaluate(w, 61 * @second)
    assert silence == 59 * @second
    assert {^w, nil} = EntryIdle.evaluate(w, 61 * @second)
    assert {_w, {:idle, later}} = EntryIdle.evaluate(w, 63 * @second)
    assert later == 3 * @second
  end

  test "the tick is a quarter of the threshold within its bounds" do
    assert EntryIdle.tick_period_ms(60_000) == 1_000
    assert EntryIdle.tick_period_ms(2_000) == 500
    assert EntryIdle.tick_period_ms(10) == 250
  end

  test "a name nothing declares reads absent and interns nothing" do
    channel = unique("never")
    assert EntryInput.last_delivery_nanos(channel) == nil
    assert :persistent_term.get({EntryInput, channel}, :absent) == :absent
  end

  test "the accessor is absent before the first frame and an instant from the gap after" do
    handle = Flezha.SDK.create_runner("entry_idle_test")
    assert Flezha.SDK.last_entry_delivery(handle, unique("never")) == nil

    # The instant is derived from the gap: two seconds of silence on the
    # monotonic clock read two seconds before the wall clock's now.
    wall_now = ~U[2026-01-01 00:00:02.000000Z]

    assert EntryInput.instant(3 * @second, 5 * @second, wall_now) ==
             ~U[2026-01-01 00:00:00.000000Z]

    channel = unique("fed")
    before = DateTime.utc_now()
    EntryInput.stamp_at(channel, EntryInput.now_nanos())
    at = Flezha.SDK.last_entry_delivery(handle, channel)
    later = DateTime.utc_now()

    assert %DateTime{} = at
    assert DateTime.compare(at, before) in [:gt, :eq]
    assert DateTime.compare(at, later) in [:lt, :eq]
  end

  test "a registration on a free handle is refused by name and otherwise accumulates" do
    handle = Flezha.SDK.create_runner("entry_idle_test")
    assert {:error, {:invalid_channel, ""}} = Flezha.SDK.on_entry_idle(handle, "", 1_000, & &1)

    assert {:error, {:invalid_idle_threshold, 0}} =
             Flezha.SDK.on_entry_idle(handle, "in", 0, & &1)

    assert {:error, {:invalid_idle_observer, :nope}} =
             Flezha.SDK.on_entry_idle(handle, "in", 1, :nope)

    armed = Flezha.SDK.on_entry_idle(handle, "in", 1_000, & &1)
    assert [{"in", 1_000, _fun}] = armed.idle_observers
  end

  test "a raising observer is logged and the next episode still reaches it" do
    channel = unique("raising")
    test_pid = self()
    {:ok, pid} = EntryIdle.start_link()

    observer = fn gap ->
      send(test_pid, {:idle, gap})
      raise "observer under test"
    end

    :ok = EntryIdle.register(pid, channel, EntryInput.cell_for(channel), 1_000, observer)
    # The suite logs at `:error`; the module level lets this one module's
    # warning through to the capture, and only while this test runs.
    :ok = :logger.set_module_level(EntryIdle, :warning)
    on_exit(fn -> :logger.unset_module_level(EntryIdle) end)

    # The earliest instant the clock has: the gap is the runtime's uptime,
    # past any threshold, without waiting for one.
    log =
      capture_log(fn ->
        EntryInput.stamp_at(channel, 1)
        send(pid, :tick)
        assert_receive {:idle, _}, 1_000
        # The warning is written inside the tick, which has finished once the
        # server answers a call.
        :sys.get_state(pid)
      end)

    assert log =~ "entry-idle observer raised on the idle episode of channel '#{channel}'"
    assert log =~ "observer under test"

    # A frame ends the episode; the next silence fires the observer again.
    EntryInput.stamp_at(channel, EntryInput.now_nanos())
    send(pid, :tick)
    :sys.get_state(pid)
    EntryInput.stamp_at(channel, 1)
    send(pid, :tick)
    assert_receive {:idle, _}, 1_000
    GenServer.stop(pid)
  end

  test "a module observer's resumed is called when it exists and skipped when it does not" do
    channel = unique("module")
    test_pid = self()
    Process.register(test_pid, :"#{channel}")
    {:ok, pid} = EntryIdle.start_link()
    :ok = EntryIdle.register(pid, channel, EntryInput.cell_for(channel), 1_000, __MODULE__.Probe)
    :persistent_term.put({__MODULE__.Probe, :target}, :"#{channel}")

    EntryInput.stamp_at(channel, 1)
    send(pid, :tick)
    assert_receive {:probe, :idle, gap_ms}, 1_000
    assert gap_ms > 0

    EntryInput.stamp_at(channel, EntryInput.now_nanos())
    send(pid, :tick)
    assert_receive {:probe, :resumed, _}, 1_000
    GenServer.stop(pid)
  end

  defmodule Probe do
    @moduledoc false
    @behaviour Flezha.SDK.IdleObserver

    @impl true
    def idle(gap_ms), do: notify({:probe, :idle, gap_ms})

    @impl true
    def resumed(gap_ms), do: notify({:probe, :resumed, gap_ms})

    defp notify(message) do
      target = :persistent_term.get({__MODULE__, :target})
      if pid = Process.whereis(target), do: send(pid, message)
    end
  end
end
