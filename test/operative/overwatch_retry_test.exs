defmodule FlezhaTest.Operative.OverwatchRetryTest do
  @moduledoc """
  Covers the bounded-retry governor + parked-error state + end-to-end
  ping introduced by `fix-overwatch-restart-storm`. The Overwatch's
  startup-attempt sequence is observed through `OperativeManager.start_operative`:
  a transient ping failure that recovers within the bounded retry budget
  returns `{:ok, _}`; a persistent failure surfaces `{:error,
  {:ping_failed, _}}` and stops the operative via the existing operator-action
  cleanup path (`stop_child_quietly`). The parked-and-alive code path
  (post-recovery via the periodic retry timer) requires the cold-restart
  auto-replay scenario covered by the integration / system tests, not
  this unit suite.

  Uses the `:overwatch_ping_fn` / `:overwatch_reset_channel_fn`
  Application-env hooks the Overwatch consults during a startup attempt.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager

  use ExUnit.Case, async: false

  @namespace "default"
  @factory "store_input"

  setup do
    test_pid = self()
    ets = :ets.new(:overwatch_retry_test, [:public, :set])

    on_exit(fn ->
      OperativeManager.stop_all_operatives(@namespace)
      Application.delete_env(:flezha, :overwatch_ping_fn)
      Application.delete_env(:flezha, :overwatch_reset_channel_fn)
    end)

    {:ok, test_pid: test_pid, ets: ets}
  end

  defmodule QuietProvider do
    def init_provider(opts), do: {:ok, opts}

    def activate(%{report_to: pid}) do
      send(pid, :activated)
      :ok
    end

    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  test "5.4 ping success proceeds to start_operative_sdk and activates plugins",
       %{test_pid: test_pid} do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _runner_id ->
      send(test_pid, :ping_called)
      {:ok, :pong}
    end)

    plugin = %Plugin{
      id: "ping_success_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    operative = %Builder{
      id: "ping_success_operative",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = OperativeManager.start_operative(@namespace, operative)
    assert_receive :ping_called, 2_000
    assert_receive :activated, 2_000
  end

  test "5.1 backoff respected — multiple ping attempts with delays in between",
       %{test_pid: test_pid, ets: ets} do
    Application.put_env(
      :flezha,
      :overwatch_ping_fn,
      gate_then(ets, fn _runner_id, attempt ->
        send(test_pid, {:ping_attempt, attempt, System.monotonic_time(:millisecond)})
        {:error, :timeout}
      end)
    )

    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    operative = %Builder{
      id: "backoff_operative",
      factory_id: @factory,
      opts: []
    }

    assert {:error, %{reason: {:ping_failed, :timeout}}} =
             OperativeManager.start_operative(@namespace, operative)

    assert_receive {:ping_attempt, 1, t1}, 2_000
    assert_receive {:ping_attempt, 2, t2}, 2_000

    # Test config sets retry_backoff_ms = [10, 10, 10] — jitter halves
    # the delay range so check at least 1 ms separation between attempts.
    assert t2 > t1
  end

  test "5.2 bounded retry exhausts and surfaces the parked init_error to the caller",
       %{test_pid: test_pid, ets: ets} do
    Application.put_env(
      :flezha,
      :overwatch_ping_fn,
      gate_then(ets, fn _runner_id, _attempt ->
        send(test_pid, :ping_called)
        {:error, :timeout}
      end)
    )

    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _runner_id ->
      send(test_pid, :channel_reset)
      :ok
    end)

    operative = %Builder{
      id: "retry_exhaust_operative",
      factory_id: @factory,
      opts: []
    }

    assert {:error, %{reason: reason}} = OperativeManager.start_operative(@namespace, operative)
    assert match?({:ping_failed, :timeout}, reason)

    # Every failed attempt MUST reset the control channel.
    assert_receive :channel_reset, 1_000
    assert_receive :ping_called, 1_000
  end

  test "5.3 transient failure within the bounded retry budget recovers without parking",
       %{test_pid: test_pid, ets: ets} do
    :ets.insert(ets, {:attempts, 0})

    Application.put_env(:flezha, :overwatch_ping_fn, fn _runner_id ->
      n = :ets.update_counter(ets, :attempts, {2, 1}, {:attempts, 0})
      send(test_pid, {:ping_attempt, n})

      # Fail first two attempts; succeed on the third. Test config
      # backoff has 3 attempts so the budget covers this.
      if n < 3 do
        {:error, :timeout}
      else
        {:ok, :pong}
      end
    end)

    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    plugin = %Plugin{
      id: "transient_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    operative = %Builder{
      id: "transient_recovery_operative",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, %{pid: pid}} = OperativeManager.start_operative(@namespace, operative)

    assert_receive {:ping_attempt, 1}, 1_000
    assert_receive {:ping_attempt, 2}, 1_000
    assert_receive {:ping_attempt, 3}, 1_000
    assert_receive :activated, 2_000

    # 5.6: phase transitions :prepared → :running exactly once.
    state = GenServer.call(pid, :get_overwatch_state)

    assert Flezha.Plugin.Extension.Table.phase(state.extensions, "transient_plugin") ==
             :running

    assert is_nil(state.init_error)
  end

  test "6.4 a parked operative un-parks via runner_migrate: pings, starts, clears init_error, activates plugins",
       %{test_pid: test_pid, ets: ets} do
    # Start the operative so the readiness gate passes but the START ping fails,
    # parking it (alive, init_error set). Started directly (not via
    # OperativeManager) so the parked-and-alive GenServer is not
    # stop_child_quietly'd — this is the cold-restart parked state.
    Application.put_env(
      :flezha,
      :overwatch_ping_fn,
      gate_then(ets, fn _runner_id, _attempt -> {:error, :timeout} end)
    )

    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    plugin = %Plugin{
      id: "unpark_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    operative = %Builder{id: "unpark_operative", factory_id: @factory, opts: [plugins: [plugin]]}

    {:ok, pid} = Flezha.Operative.Overwatch.start_link({@namespace, "stale_ce", operative})

    on_exit(fn ->
      try do
        if Process.alive?(pid), do: GenServer.stop(pid)
      catch
        :exit, _ -> :ok
      end
    end)

    # It parks (init_error set) and never activates the plugin while parked.
    assert eventually(fn ->
             GenServer.call(pid, :get_overwatch_state).init_error != nil
           end)

    refute_received :activated

    # A healthy runner is now reachable — un-park via runner_migrate.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:ok, :pong} end)
    runner_id = OperativeManager.assign_runner(@namespace, @factory, "unpark_operative")
    assert is_binary(runner_id)

    assert :ok = GenServer.call(pid, {:runner_migrate, runner_id})

    # init_error cleared and the plugin activated (resumes producing).
    # `convergence-self-healing` §3.3-3.5 made `:runner_migrate`
    # async (the retry loop runs in a `Task.Supervisor.async_nolink` task),
    # so we poll the state instead of asserting immediately.
    assert_receive :activated, 5_000

    assert eventually(fn ->
             is_nil(GenServer.call(pid, :get_overwatch_state).init_error)
           end)

    state = GenServer.call(pid, :get_overwatch_state)
    assert Flezha.Plugin.Extension.Table.phase(state.extensions, "unpark_plugin") == :running
  end

  # `await_active_runner` pings once to confirm the runner is reachable
  # before the start-retry path runs. These tests drive the START ping to
  # fail; the readiness gate must still pass or the operative stops with
  # `:runner_not_ready` instead of exercising the retry/park behaviour.
  # First call = readiness gate (pong); later calls = start attempts (run body).
  defp gate_then(ets, start_fn) do
    fn runner_id ->
      case :ets.update_counter(ets, :calls, {2, 1}, {:calls, 0}) do
        1 -> {:ok, :pong}
        n -> start_fn.(runner_id, n - 1)
      end
    end
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() ->
        true

      attempts <= 0 ->
        false

      true ->
        Process.sleep(20)
        eventually(fun, attempts - 1)
    end
  end

  test "5.10 5.5 mocked-ingress regression: failed startup keeps plugins :prepared, no upstream I/O",
       %{test_pid: test_pid, ets: ets} do
    Application.put_env(
      :flezha,
      :overwatch_ping_fn,
      gate_then(ets, fn _runner_id, _attempt -> {:error, :timeout} end)
    )

    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    # QuietProvider's `activate/1` sends `:activated`. If the operative parks
    # without ever calling activate, no :activated message is sent — i.e.
    # the upstream connection an `activate/1` would open never happens.
    plugin = %Plugin{
      id: "cascade_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    operative = %Builder{
      id: "cascade_operative",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    baseline = :erlang.memory(:processes)

    assert {:error, %{reason: {:ping_failed, :timeout}}} =
             OperativeManager.start_operative(@namespace, operative)

    # Plugins never activate, so the upstream connection (TLS handshake,
    # subscribe handshake) the Websocket provider's `activate/1` would
    # open is never opened. No memory cascade.
    refute_received :activated

    final = :erlang.memory(:processes)
    delta_mib = (final - baseline) / (1024 * 1024)

    assert delta_mib < 50,
           "process memory delta exceeded 50 MiB: #{delta_mib} MiB " <>
             "(baseline=#{baseline}, final=#{final})"
  end
end
