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

  alias Flezha.Identificator
  alias Flezha.Observability.Export.PrometheusRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.FleetStateHelper
  alias FlezhaTest.Helper.HubRegistryTestHelper

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

    test "the exposition carries a bound-relay count per runner, zero included" do
      ns = "bound_count_ns_#{System.unique_integer([:positive])}"
      [busy, idle] = ["bound_runner_busy", "bound_runner_idle"]

      fleet = :sys.get_state(FleetManager)
      on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet end) end)
      {:ok, _} = HubRegistryTestHelper.ensure_hub(ns)
      on_exit(fn -> HubManager.stop_hub(ns) end)
      HubRegistryTestHelper.seed_runners(ns, [busy, idle], "bound_factory")
      on_exit(fn -> FleetStateHelper.forget_established(ns, ["bound_factory"]) end)

      HubRegistryTestHelper.inject_relay(ns, "bound_op")
      HubManager.relay_set_runner(ns, "bound_op", busy)

      arm(:processhub)
      start_supervised!(PrometheusRegistry)
      {:ok, pid} = ProcessHubSource.start_link([])
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
      poll(pid, 1)

      hub = Identificator.hub(ns)

      counts =
        :processhub
        |> RingBufferSink.query(0)
        |> Enum.filter(&(&1.name == :runner_bound_relays and &1.metadata.hub == hub))
        |> Map.new(&{&1.metadata.runner, &1.measurements.value})

      # A missing series and a zero are not the same answer to a user.
      assert counts == %{busy => 1, idle => 0}

      for signal <- RingBufferSink.query(:processhub, 0), do: PrometheusRegistry.record(signal)
      TestSinks.settle(PrometheusRegistry)
      {body, _series} = PrometheusRegistry.render()
      text = IO.iodata_to_binary(body)

      assert text =~ ~s(runner="#{busy}")
      assert text =~ ~s(runner="#{idle}")
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

    # Nothing here names a metric. The layout declares which slots reach this
    # source, so the test walks the DECLARATION and asserts each row arrived —
    # a slot that stops being emitted fails here without anyone remembering to
    # write a test for it, which is the failure `report-parked-relay-
    # visibility` and `surface-the-runner-self-report` both landed as one-off
    # fixes.
    test "every metric the layout declares reaches a signal, under its declared measurement" do
      arm(:flezha_transport)

      runner = "obs_decl_#{System.unique_integer([:positive])}"
      relay = "obs_decl_rel_#{System.unique_integer([:positive])}"
      channel_ref = LivenessCounters.register_channel(runner, :secondary_1)
      relay_ref = LivenessCounters.register_relay(relay)

      # A distinct value per slot, so a row reading another row's value is a
      # failure rather than a coincidence.
      values =
        LivenessCounters.channel_rows()
        |> Enum.with_index(1)
        |> Map.new(fn {row, i} -> {Keyword.fetch!(row, :name), 1_000 + i} end)

      for {slot, value} <- values, do: LivenessCounters.set_channel(channel_ref, slot, value)

      for slot <- LivenessCounters.relay_slots(),
          do: LivenessCounters.bump_relay(relay_ref, slot)

      {:ok, pid} = TransportSource.start_link([])
      poll(pid, 1)
      :ok = GenServer.stop(pid)

      signals = RingBufferSink.query(:flezha_transport, 0)

      for {group, row, name} <- LivenessCounters.surfaces(:metric) do
        signal = Enum.find(signals, fn s -> s.name == name end)
        assert signal, "#{name} is declared a metric of this source and reached no signal"
        assert signal.metadata[:scope] == :data_plane

        key = Flezha.SDK.CounterTable.measurement(row)

        assert is_integer(signal.measurements[key]),
               "#{name} was emitted under #{inspect(Map.keys(signal.measurements))}, " <>
                 "not the #{inspect(key)} its row declares"

        if group == :channels do
          slot = Keyword.fetch!(row, :name)

          assert signal.measurements[key] >= Map.fetch!(values, slot),
                 "#{name} read #{signal.measurements[key]} for a slot set to #{values[slot]}"
        end
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
    {ProcessHubSource, :processhub, [:metric], 1_000, 1_000, []},
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
        # `:timers` is the poll tick the template arms for every source.
        assert Enum.sort(Map.keys(state)) == Enum.sort([:interval, :timers | extra])
      end
    end
  end

  describe "the declared source list" do
    # `@sources` is read by the supervisor and, before this, by nothing that
    # checked it against the tree: a module could be started without declaring
    # the contract, and the gap was invisible until a poll crashed.
    test "every declared source implements the source contract" do
      modules = Flezha.Observability.OtpBackend.Supervisor.source_modules()

      for mod <- modules do
        assert Flezha.Observability.Source in declared_behaviours(mod),
               "#{inspect(mod)} is started by the supervisor and declares no source contract"

        assert function_exported?(mod, :config_schema, 0),
               "#{inspect(mod)} declares the source contract and exports no config_schema/0"
      end
    end

    test "the lifecycle table names exactly the declared sources" do
      assert Enum.sort(for({mod, _, _, _, _, _} <- @lifecycle, do: mod)) ==
               Enum.sort(Flezha.Observability.OtpBackend.Supervisor.source_modules()),
             "the lifecycle table and the supervisor's source list have drifted apart"
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
