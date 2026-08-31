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
      Application.put_env(
        :flezha,
        :observability,
        Keyword.put(Application.get_env(:flezha, :observability, []), :otp_backend,
          poll_intervals: %{beam_vm: 25}
        )
      )

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
      Application.put_env(
        :flezha,
        :observability,
        Keyword.put(Application.get_env(:flezha, :observability, []), :otp_backend,
          poll_intervals: %{host: 25}
        )
      )

      {:ok, pid} = HostSource.start_link([])

      {:ok, %{name: :host, status: :active}} = Registry.lookup_source(:host)

      poll(pid, 1)

      signals = RingBufferSink.query(:host, 0)

      cond do
        Enum.any?(signals, fn s -> s.name == :host_source_degraded end) ->
          # Non-Linux — degraded path emits exactly one event and
          # nothing more.
          assert Enum.count(signals, fn s -> s.kind == :metric end) == 0

        true ->
          assert Enum.any?(signals, fn s -> s.name == :rss_bytes end)
          assert Enum.any?(signals, fn s -> s.name == :cpu_user_us end)
          assert Enum.any?(signals, fn s -> s.name == :load_avg_1m end)
      end

      GenServer.stop(pid)
    end
  end

  describe "ProcessHubSource" do
    test "registers and no-ops cleanly when no namespaces are up" do
      Application.put_env(
        :flezha,
        :observability,
        Keyword.put(Application.get_env(:flezha, :observability, []), :otp_backend,
          poll_intervals: %{processhub: 25}
        )
      )

      {:ok, pid} = ProcessHubSource.start_link([])

      {:ok, %{name: :processhub, status: :active}} = Registry.lookup_source(:processhub)

      # One poll; no namespaces => no signals, no raise.
      poll(pid, 1)
      :ok = GenServer.stop(pid)
    end
  end

  describe "TransportSource" do
    test "registers and polls without crashing when no transports are active" do
      Application.put_env(
        :flezha,
        :observability,
        Keyword.put(Application.get_env(:flezha, :observability, []), :otp_backend,
          poll_intervals: %{flezha_transport: 25}
        )
      )

      {:ok, pid} = TransportSource.start_link([])

      {:ok, %{name: :flezha_transport, status: :active}} =
        Registry.lookup_source(:flezha_transport)

      poll(pid, 1)
      :ok = GenServer.stop(pid)
    end
  end

  describe "BoundedMailboxSource" do
    test "registers and emits a one-shot degraded event on first poll" do
      Application.put_env(
        :flezha,
        :observability,
        Keyword.put(Application.get_env(:flezha, :observability, []), :otp_backend,
          poll_intervals: %{flezha_bounded_mailbox: 25}
        )
      )

      {:ok, pid} = BoundedMailboxSource.start_link([])

      {:ok, %{name: :flezha_bounded_mailbox, status: :active}} =
        Registry.lookup_source(:flezha_bounded_mailbox)

      poll(pid, 1)

      events = RingBufferSink.query(:flezha_bounded_mailbox, 0)
      assert Enum.any?(events, fn s -> s.name == :bounded_mailbox_unavailable end)
      :ok = GenServer.stop(pid)
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
      Application.put_env(
        :flezha,
        :observability,
        Keyword.put(Application.get_env(:flezha, :observability, []), :otp_backend,
          poll_intervals: %{beam_vm: 25}
        )
      )

      {:ok, pid} = BeamVmSource.start_link([])
      poll(pid, 1)

      signals = RingBufferSink.query(:beam_vm, 0)
      assert length(signals) > 0
      assert Enum.all?(signals, fn %Signal{source: :beam_vm} -> true end)

      _ = Bus
      :ok = GenServer.stop(pid)
    end
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
end
