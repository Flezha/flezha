defmodule Flezha.Observability.OtpBackendTest do
  # async: false: writes Application env; the observability registry; registered singletons.
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Bus, Registry, RingBufferSink, Signal}
  alias Flezha.Observability.TestSinks

  alias Flezha.Observability.OtpBackend.{
    BeamVmSource,
    BoundedMailboxSource,
    HostSource,
    ProcessHubSource,
    ProcParser,
    TransportSource
  }

  alias Flezha.Transport.LivenessCounters

  @moduletag :observability_otp_backend

  setup do
    # Ensure tear-down resets any source that this test starts so
    # registry pollution doesn't cross test files.
    on_exit(fn ->
      for name <- [:beam_vm, :host, :processhub, :flezha_transport, :flezha_bounded_mailbox] do
        Registry.unregister_source(name)
      end
    end)

    :ok
  end

  describe "BeamVmSource" do
    test "registers and emits memory + run-queue + scheduler signals" do
      arm(:beam_vm)

      {:ok, pid} = BeamVmSource.start_link([])

      {:ok, %{name: :beam_vm, status: :active}} = Registry.lookup_source(:beam_vm)

      # Two polls so scheduler-utilisation has a delta.
      poll(pid, 2)

      mem_signals = RingBufferSink.query(:beam_vm, 0)

      assert Enum.any?(mem_signals, fn s -> s.name == :memory_total end)
      assert Enum.any?(mem_signals, fn s -> s.name == :memory_processes end)
      assert Enum.any?(mem_signals, fn s -> s.name == :run_queue_total end)
      assert Enum.any?(mem_signals, fn s -> s.name == :gc_count end)
      assert Enum.any?(mem_signals, fn s -> s.name == :scheduler_utilisation end)

      GenServer.stop(pid)
    end
  end

  describe "ProcParser" do
    test "parse_stat extracts utime/stime/threads/starttime/rss_pages" do
      line =
        "1 (cmd with spaces) S 0 0 0 0 -1 4194304 0 0 0 0 100 200 0 0 20 0 8 0 12345 99999 250 ..."

      {:ok, parsed} = ProcParser.parse_stat(line)

      assert parsed.utime == 100
      assert parsed.stime == 200
      assert parsed.num_threads == 8
      assert parsed.starttime == 12_345
      assert parsed.rss_pages == 250
    end

    test "parse_status extracts VmRSS + FDSize" do
      text = """
      Name:	beam.smp
      VmRSS:	  1024 kB
      FDSize:	256
      """

      {:ok, %{vm_rss_bytes: rss, fd_size: fds}} = ProcParser.parse_status(text)
      assert rss == 1024 * 1024
      assert fds == 256
    end

    test "parse_loadavg" do
      assert {:ok, %{load_1m: 0.50, load_5m: 1.25, load_15m: 2.0}} =
               ProcParser.parse_loadavg("0.50 1.25 2.00 1/250 99999\n")
    end

    test "parse_stat on malformed input returns error" do
      assert {:error, :unparseable} = ProcParser.parse_stat("nope")
    end
  end

  describe "HostSource" do
    test "registers and either emits rss + cpu signals on Linux or transitions to :degraded" do
      arm(:host)

      {:ok, pid} = HostSource.start_link([])

      {:ok, %{name: :host, status: :active}} = Registry.lookup_source(:host)

      poll(pid, 1)

      signals = RingBufferSink.query(:host, 0)

      if Enum.any?(signals, fn s -> s.name == :host_source_degraded end) do
        # Non-Linux — degraded path emits exactly one event and
        # nothing more.
        refute Enum.any?(signals, fn s -> s.kind == :metric end)
      else
        assert Enum.any?(signals, fn s -> s.name == :rss_bytes end)
        assert Enum.any?(signals, fn s -> s.name == :cpu_user_us end)
        assert Enum.any?(signals, fn s -> s.name == :load_avg_1m end)
      end

      GenServer.stop(pid)
    end
  end

  describe "ProcessHubSource" do
    test "registers and no-ops cleanly when no namespaces are up" do
      arm(:processhub)

      {:ok, pid} = ProcessHubSource.start_link([])

      {:ok, %{name: :processhub, status: :active}} = Registry.lookup_source(:processhub)

      # One poll; no namespaces => no signals, no raise.
      poll(pid, 1)
      :ok = GenServer.stop(pid)
    end
  end

  describe "TransportSource" do
    test "registers and polls without crashing when no transports are active" do
      arm(:flezha_transport)

      {:ok, pid} = TransportSource.start_link([])

      {:ok, %{name: :flezha_transport, status: :active}} =
        Registry.lookup_source(:flezha_transport)

      poll(pid, 1)
      :ok = GenServer.stop(pid)
    end

    test "an operative's parked-drop count is summed into the data-plane aggregates" do
      arm(:flezha_transport)

      operative = "obs_parked_#{System.unique_integer([:positive])}"
      ref = LivenessCounters.register_operative(operative)
      LivenessCounters.bump_operative(ref, :parked_drops)
      LivenessCounters.bump_operative(ref, :parked_drops)

      {:ok, pid} = TransportSource.start_link([])
      poll(pid, 1)
      :ok = GenServer.stop(pid)

      signals = RingBufferSink.query(:flezha_transport, 0)
      drops = Enum.find(signals, fn s -> s.name == :operative_parked_drops end)

      assert drops, "the new slot never reached a signal"
      assert drops.measurements[:count] >= 2
      assert drops.metadata[:scope] == :data_plane
    end

    # A liveness slot does not surface by itself: `poll_beam_counters/0` walks
    # a table with one row per metric and nothing iterates the layout, so a
    # heartbeat counter with no row there is readable only by a caller of
    # `snapshot_all/0` and reaches no signal, no sink and no exporter.
    test "the heartbeat's rebind-discard and timer counters reach the data-plane aggregates" do
      arm(:flezha_transport)

      runner = "obs_hb_#{System.unique_integer([:positive])}"
      ref = LivenessCounters.register_channel(runner, :secondary_1)

      for {slot, value} <- [
            hb_restore_discarded_inputs: 7,
            hb_timers_armed: 3,
            hb_timers_fired: 11
          ] do
        LivenessCounters.set_channel(ref, slot, value)
      end

      {:ok, pid} = TransportSource.start_link([])
      poll(pid, 1)
      :ok = GenServer.stop(pid)

      signals = RingBufferSink.query(:flezha_transport, 0)

      for {name, value} <- [restore_discarded_inputs: 7, timers_armed: 3, timers_fired: 11] do
        signal = Enum.find(signals, fn s -> s.name == name end)
        assert signal, "#{name} never reached a signal"
        assert signal.measurements[:count] >= value
        assert signal.metadata[:scope] == :data_plane
      end
    end

    # A heartbeat counter is one fact about the runner, and the runner sends
    # the same heartbeat on every secondary stream it holds, so the controller
    # mirrors one value into every channel row of that runner. Counted per
    # channel, a metric reads the runner's value times its stream-pool size.
    # The value is large so the check tells the collapse from the multiplier
    # under any small leftover another live channel contributes to the global
    # sum: once per runner reads in `[value, 2 * value)`; per channel reads
    # three times it.
    test "a runner-global heartbeat counter is counted once per runner, not once per channel" do
      arm(:flezha_transport)

      runner = "obs_pool_#{System.unique_integer([:positive])}"
      slots = [hb_timers_fired: 1_000_003, hb_blocked_parks: 2_000_003]

      for name <- [:secondary_1, :secondary_2, :secondary_3] do
        ref = LivenessCounters.register_channel(runner, name)
        for {slot, value} <- slots, do: LivenessCounters.set_channel(ref, slot, value)
      end

      {:ok, pid} = TransportSource.start_link([])
      poll(pid, 1)
      :ok = GenServer.stop(pid)

      signals = RingBufferSink.query(:flezha_transport, 0)

      for {name, value} <- [timers_fired: 1_000_003, mailbox_blocked: 2_000_003] do
        signal = Enum.find(signals, fn s -> s.name == name end)
        assert signal, "#{name} never reached a signal"

        assert signal.measurements[:count] in value..(2 * value - 1),
               "#{name} read #{signal.measurements[:count]} for one runner whose value is #{value}"
      end
    end
  end

  describe "BoundedMailboxSource" do
    test "registers and emits a one-shot degraded event on first poll" do
      arm(:flezha_bounded_mailbox)

      {:ok, pid} = BoundedMailboxSource.start_link([])

      {:ok, %{name: :flezha_bounded_mailbox, status: :active}} =
        Registry.lookup_source(:flezha_bounded_mailbox)

      poll(pid, 1)

      events = RingBufferSink.query(:flezha_bounded_mailbox, 0)
      assert Enum.any?(events, fn s -> s.name == :bounded_mailbox_unavailable end)
      :ok = GenServer.stop(pid)
    end
  end

  # `use Flezha.Observability.OtpBackend.Poller` writes every source's
  # registration, its first state and the `Flezha.Observability.Source`
  # contract it registers under. Read them back — from the registry, from
  # `init/1` itself and from the declared behaviours — so a change to the
  # macro that moves any of them fails here rather than producing a quietly
  # different source.
  @lifecycle [
    {BeamVmSource, :beam_vm, [:metric], 50, 1_000, [:last_scheduler_sample]},
    {HostSource, :host, [:metric, :event], 50, 5_000, [:prev_cpu_jiffies, :status]},
    {ProcessHubSource, :processhub, [:metric], 200, 1_000, []},
    {TransportSource, :flezha_transport, [:metric], 40, 1_000, []},
    {BoundedMailboxSource, :flezha_bounded_mailbox, [:metric, :event], 5_000, 1_000,
     [:announced?]}
  ]

  describe "source lifecycle" do
    test "each source registers what it declares and starts in its own state" do
      original = Application.get_env(:flezha, :observability, [])

      on_exit(fn ->
        Application.put_env(:flezha, :observability, original)
        :erlang.system_flag(:scheduler_wall_time, false)
      end)

      # An empty override map makes every source fall back to its declared
      # default, so the intervals below are the defaults and not a neighbour's
      # leftover `put_env`.
      Application.put_env(
        :flezha,
        :observability,
        Keyword.put(original, :otp_backend, poll_intervals: %{})
      )

      for {mod, source, kinds, budget, interval, extra} <- @lifecycle do
        Registry.unregister_source(source)

        assert Flezha.Observability.Source in declared_behaviours(mod)
        assert mod.config_schema() == %{}

        assert {:ok, state} = mod.init([])
        assert {:ok, row} = Registry.lookup_source(source)

        assert {row.name, row.kinds, row.flavour, row.cardinality_budget} ==
                 {source, kinds, :process_backed, budget}

        assert state.interval == interval
        assert Enum.sort(Map.keys(state)) == Enum.sort([:interval | extra])
      end
    end
  end

  describe "OtpBackend.Supervisor (already-running instance)" do
    test "the application instance starts with zero children when test config disables the OTP backend" do
      # In :test the runtime config sets otp_backend_enabled: false,
      # so the instance the application started should have no
      # children.
      children =
        Supervisor.which_children(Flezha.Observability.OtpBackend.Supervisor)

      assert children == []
    end

    test "source_modules/0 lists the five built-in sources" do
      assert Flezha.Observability.OtpBackend.Supervisor.source_modules() == [
               Flezha.Observability.OtpBackend.BeamVmSource,
               Flezha.Observability.OtpBackend.HostSource,
               Flezha.Observability.OtpBackend.ProcessHubSource,
               Flezha.Observability.OtpBackend.TransportSource,
               Flezha.Observability.OtpBackend.BoundedMailboxSource
             ]
    end
  end

  describe "Bus integration" do
    test "an emit from a built-in source flows through the ring buffer" do
      arm(:beam_vm)

      {:ok, pid} = BeamVmSource.start_link([])
      poll(pid, 1)

      signals = RingBufferSink.query(:beam_vm, 0)
      assert signals != []
      assert Enum.all?(signals, fn %Signal{source: :beam_vm} -> true end)

      _ = Bus
      :ok = GenServer.stop(pid)
    end
  end

  # Shrink one source's poll interval so `poll/2` drives it now instead of
  # waiting out its default second.
  defp arm(source) do
    Application.put_env(
      :flezha,
      :observability,
      Keyword.put(Application.get_env(:flezha, :observability, []), :otp_backend,
        poll_intervals: %{source => 25}
      )
    )
  end

  # Drive `n` polls of a source now rather than waiting out its interval, then
  # let the ring buffer take what they emitted: the poll is the source's own
  # timer message, and a call queued behind it answers once it ran.
  defp poll(source, n) do
    for _ <- 1..n do
      send(source, :poll)
      _ = :sys.get_state(source)
    end

    TestSinks.settle(RingBufferSink)
  end

  defp declared_behaviours(mod) do
    mod.module_info(:attributes) |> Keyword.get_values(:behaviour) |> List.flatten()
  end
end
