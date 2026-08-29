defmodule Flezha.Shutdown.SignalHandlerTest do
  # async: false — reads the node-global `erl_signal_server` and
  # reconfigures the Logger level. The cases that would *mutate* the global
  # manager run in a throwaway BEAM instead (see "paths that end the VM").
  # async: false: the primary Logger.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog, only: [capture_log: 1, with_log: 1]
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Shutdown.Coordinator
  alias Flezha.Shutdown.SignalHandler
  alias Flezha.Shutdown.SignalHandler.Relay

  setup do
    prev = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: prev) end)
    :ok
  end

  # A private manager, so no unit test can disturb the signal handling of
  # the VM running the suite.
  defp own_manager() do
    # Unlinked: the manager must outlive the test process so `on_exit`
    # tears it down deterministically rather than racing an exit signal.
    {:ok, mgr} = :gen_event.start()

    on_exit(fn ->
      try do
        if Process.alive?(mgr), do: :gen_event.stop(mgr)
      catch
        :exit, _ -> :ok
      end
    end)

    mgr
  end

  # Starting a handler always logs its install line; capture it so only a
  # test that asserts on that line has to think about it.
  defp start_handler(mgr, name) do
    {pid, _log} =
      with_log(fn ->
        {:ok, pid} = SignalHandler.start_link(name: name, enabled: true, signal_server: mgr)
        # `handle_continue/2` logs the install line after `start_link/1`
        # returns; this call waits for it so the line lands inside the
        # capture instead of on the suite's console.
        GenServer.call(pid, :status)
        pid
      end)

    pid
  end

  defp run_in_fresh_beam(script) do
    paths = Path.wildcard("_build/test/lib/*/ebin")
    args = Enum.flat_map(paths, &["-pa", &1]) ++ ["-e", script]
    System.cmd("elixir", args, stderr_to_stdout: true)
  end

  describe "the kill switch" do
    test "graceful_shutdown_enabled=false installs nothing and leaves kernel's handler" do
      # Graceful shutdown defaults off under `:test`, so the application's
      # own SignalHandler is the negative case — and the suite's VM keeps
      # kernel's signal handling throughout.
      assert is_pid(Process.whereis(SignalHandler))
      assert :sys.get_state(Process.whereis(SignalHandler)).count == 0
      assert :erl_signal_handler in :gen_event.which_handlers(:erl_signal_server)
      refute Relay in :gen_event.which_handlers(:erl_signal_server)
    end
  end

  describe "install" do
    test "registers Relay, reports :ok, and never asks for sigint" do
      mgr = own_manager()

      {pid, log} =
        with_log(fn ->
          # Deliberately not `start_handler/2` — this case asserts on the
          # very log line that helper captures.
          {:ok, pid} =
            SignalHandler.start_link(name: :sh_install, enabled: true, signal_server: mgr)

          assert GenServer.call(pid, :status) == :ok
          assert Relay in :gen_event.which_handlers(mgr)
          pid
        end)

      assert log =~ "[shutdown] OS signal handlers installed (SIGTERM)"
      # sigint is never passed to `:os.set_signal/2`, so nothing raises.
      refute log =~ "os.set_signal unavailable"
      refute log =~ "badarg"

      GenServer.stop(pid)
    end

    test "the handler is supervised — it is removed when its owner dies" do
      mgr = own_manager()
      pid = start_handler(mgr, :sh_sup)
      assert Relay in :gen_event.which_handlers(mgr)

      ref = Process.monitor(pid)
      GenServer.stop(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 2_000

      assert wait_until(fn -> Relay not in :gen_event.which_handlers(mgr) end, 2_000)
    end

    test "a refused registration latches :degraded, warns, and still completes start" do
      # A manager that does not exist — `add_sup_handler` exits `:noproc`.
      log =
        capture_log(fn ->
          {:ok, pid} =
            SignalHandler.start_link(
              name: :sh_degraded,
              enabled: true,
              signal_server: :no_such_signal_server
            )

          assert GenServer.call(pid, :status) == :degraded
          GenServer.stop(pid)
        end)

      assert log =~ "os.set_signal unavailable"
      assert log =~ "falling back to release-mode SIGTERM"

      # `install/2` adds before it deletes, so a failed install never removes
      # the incumbent handler: the node is never left with neither.
      assert :erl_signal_handler in :gen_event.which_handlers(:erl_signal_server)
    end
  end

  describe "Relay" do
    test "install/2 takes over a live manager: ours in, the incumbent out" do
      mgr = own_manager()
      # The real kernel handler, on a private manager — never notified with
      # `:sigterm` here, since its `handle_event/2` calls `init:stop()`.
      :ok = :gen_event.add_handler(mgr, :erl_signal_handler, [])
      assert :erl_signal_handler in :gen_event.which_handlers(mgr)

      :ok = Relay.install(mgr, self())

      handlers = :gen_event.which_handlers(mgr)
      assert Relay in handlers
      refute :erl_signal_handler in handlers
    end

    test "forwards a signal to its owner" do
      mgr = own_manager()
      :ok = :gen_event.add_handler(mgr, Relay, {self(), mgr})

      :gen_event.notify(mgr, :sigterm)
      assert_receive :sigterm, 1_000
    end

    test "does not try to restore kernel's handler for a non-kernel manager" do
      mgr = own_manager()
      :ok = :gen_event.add_handler(mgr, Relay, {self(), mgr})
      :ok = :gen_event.delete_handler(mgr, Relay, [])

      # The global manager is untouched, and kernel's handler is not
      # duplicated onto it.
      assert Enum.count(
               :gen_event.which_handlers(:erl_signal_server),
               &(&1 == :erl_signal_handler)
             ) ==
               1
    end
  end

  describe "escalation ladder" do
    test "a second signal during shutdown is logged and not re-dispatched" do
      mgr = own_manager()
      pid = start_handler(mgr, :sh_second)
      :sys.replace_state(pid, fn s -> %{s | count: 1} end)

      log =
        capture_log(fn ->
          send(pid, :sigterm)
          :sys.get_state(pid)
        end)

      assert log =~ "already in progress, ignoring sigterm"
      assert :sys.get_state(pid).count == 2

      GenServer.stop(pid)
    end
  end

  describe "signal_handler_status is pulled, not cached" do
    test "Coordinator.status/0 reflects the handler's current state immediately" do
      handler = Process.whereis(SignalHandler)
      original = Coordinator.status().signal_handler_status

      :sys.replace_state(handler, fn s -> %{s | status: :degraded} end)
      assert Coordinator.status().signal_handler_status == :degraded

      :sys.replace_state(handler, fn s -> %{s | status: :ok} end)
      assert Coordinator.status().signal_handler_status == :ok

      :sys.replace_state(handler, fn s -> %{s | status: original} end)
    end
  end

  # These drive the real `erl_signal_server`, `:init.stop()` and
  # `:erlang.halt(143)` — each of which would end or degrade the VM running
  # the suite — so they run in a throwaway BEAM.
  describe "paths that end the VM" do
    @describetag :system

    test "on the real erl_signal_server it replaces kernel's handler and restores it on stop" do
      {out, code} =
        run_in_fresh_beam("""
        Logger.configure(level: :info)
        {:ok, pid} = Flezha.Shutdown.SignalHandler.start_link(enabled: true)
        IO.puts("STATUS=" <> to_string(Flezha.Shutdown.SignalHandler.status()))
        IO.puts("AFTER_INSTALL=" <> inspect(:gen_event.which_handlers(:erl_signal_server)))
        ref = Process.monitor(pid)
        GenServer.stop(pid)
        receive do
          {:DOWN, ^ref, _, _, _} -> :ok
        end
        # The restore of kernel's handler is spawned from terminate/2.
        FlezhaTest.Helper.NodeTestHelper.wait_until(
          fn -> :erl_signal_handler in :gen_event.which_handlers(:erl_signal_server) end,
          5_000
        )
        IO.puts("AFTER_STOP=" <> inspect(:gen_event.which_handlers(:erl_signal_server)))
        """)

      assert code == 0, "script failed (#{code}): #{out}"
      assert out =~ "STATUS=ok"

      install = Regex.run(~r/AFTER_INSTALL=(.*)/, out) |> List.last()
      assert install =~ "Relay"
      refute install =~ "erl_signal_handler"

      stopped = Regex.run(~r/AFTER_STOP=(.*)/, out) |> List.last()
      assert stopped =~ "erl_signal_handler"
      refute stopped =~ "Relay"
    end

    test "the first signal dispatches to the coordinator and stops the VM with status 0" do
      {out, code} =
        run_in_fresh_beam("""
        Logger.configure(level: :info)
        {:ok, pid} = Flezha.Shutdown.SignalHandler.start_link(enabled: false)
        send(pid, :sigterm)
        # The VM's exit status is the handler's to decide, so this script must
        # outlive it: park once the stop has begun, give up if it never does.
        if FlezhaTest.Helper.NodeTestHelper.wait_until(fn -> :init.get_status() != {:started, :started} end, 10_000) do
          FlezhaTest.Helper.NodeTestHelper.park()
        else
          IO.puts("NEVER_STOPPED")
        end
        """)

      assert code == 0, "expected a clean :init.stop(), got #{code}: #{out}"
      assert out =~ "[shutdown] received sigterm"
      # No Coordinator runs in this bare VM, so its `:noproc` exit is the
      # proof that the dispatch actually reached `Coordinator.shutdown/1`.
      assert out =~ "coordinator exit during signal handling"
      refute out =~ "NEVER_STOPPED"
    end

    test "the third signal force-halts the BEAM with status 143" do
      {out, code} =
        run_in_fresh_beam("""
        Logger.configure(level: :info)
        {:ok, pid} = Flezha.Shutdown.SignalHandler.start_link(enabled: false)
        :sys.replace_state(pid, fn s -> %{s | count: 2} end)
        send(pid, :sigterm)
        if FlezhaTest.Helper.NodeTestHelper.wait_until(fn -> :init.get_status() != {:started, :started} end, 10_000) do
          FlezhaTest.Helper.NodeTestHelper.park()
        else
          IO.puts("NEVER_HALTED")
        end
        """)

      assert code == 143, "expected the halt(143) escape hatch, got #{code}: #{out}"
      assert out =~ "forced halt requested via third sigterm"
      refute out =~ "NEVER_HALTED"
    end
  end
end
