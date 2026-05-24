defmodule FlezhaTest.Relay.OverwatchRetryTest do
  @moduledoc """
  Covers the bounded-retry governor + parked-error state + end-to-end
  ping introduced by `fix-overwatch-restart-storm`. The Overwatch's
  startup-attempt sequence is observed through `RelayManager.start_relay`:
  a transient ping failure that recovers within the bounded retry budget
  returns `{:ok, _}`; a persistent failure surfaces `{:error,
  {:ping_failed, _}}` and stops the relay via the existing operator-action
  cleanup path (`stop_child_quietly`). The parked-and-alive code path
  (post-recovery via the periodic retry timer) requires the cold-restart
  auto-replay scenario covered by the integration / system tests, not
  this unit suite.

  Uses the `:overwatch_ping_fn` / `:overwatch_reset_channel_fn`
  Application-env hooks the Overwatch consults during a startup attempt.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Relay.Builder
  alias Flezha.Service.RelayManager

  use ExUnit.Case, async: false

  @namespace "default"
  @factory "store_input"

  setup do
    test_pid = self()
    ets = :ets.new(:overwatch_retry_test, [:public, :set])

    on_exit(fn ->
      RelayManager.stop_all_relays(@namespace)
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

  test "5.4 ping success proceeds to start_relay_sdk and activates plugins",
       %{test_pid: test_pid} do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _executor_id ->
      send(test_pid, :ping_called)
      {:ok, :pong}
    end)

    plugin = %Plugin{
      id: "ping_success_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    relay = %Builder{
      id: "ping_success_relay",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = RelayManager.start_relay(@namespace, relay)
    assert_receive :ping_called, 2_000
    assert_receive :activated, 2_000
  end

  test "5.1 backoff respected — multiple ping attempts with delays in between",
       %{test_pid: test_pid, ets: ets} do
    :ets.insert(ets, {:attempts, 0})

    Application.put_env(:flezha, :overwatch_ping_fn, fn _executor_id ->
      ts = System.monotonic_time(:millisecond)
      n = :ets.update_counter(ets, :attempts, {2, 1}, {:attempts, 0})
      send(test_pid, {:ping_attempt, n, ts})
      {:error, :timeout}
    end)

    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    relay = %Builder{
      id: "backoff_relay",
      factory_id: @factory,
      opts: []
    }

    assert {:error, %{reason: {:ping_failed, :timeout}}} =
             RelayManager.start_relay(@namespace, relay)

    assert_receive {:ping_attempt, 1, t1}, 2_000
    assert_receive {:ping_attempt, 2, t2}, 2_000

    # Test config sets retry_backoff_ms = [10, 10, 10] — jitter halves
    # the delay range so check at least 1 ms separation between attempts.
    assert t2 > t1
  end

  test "5.2 bounded retry exhausts and surfaces the parked init_error to the caller",
       %{test_pid: test_pid} do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _executor_id ->
      send(test_pid, :ping_called)
      {:error, :timeout}
    end)

    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _executor_id ->
      send(test_pid, :channel_reset)
      :ok
    end)

    relay = %Builder{
      id: "retry_exhaust_relay",
      factory_id: @factory,
      opts: []
    }

    assert {:error, %{reason: reason}} = RelayManager.start_relay(@namespace, relay)
    assert match?({:ping_failed, :timeout}, reason)

    # Every failed attempt MUST reset the control channel.
    assert_receive :channel_reset, 1_000
    assert_receive :ping_called, 1_000
  end

  test "5.3 transient failure within the bounded retry budget recovers without parking",
       %{test_pid: test_pid, ets: ets} do
    :ets.insert(ets, {:attempts, 0})

    Application.put_env(:flezha, :overwatch_ping_fn, fn _executor_id ->
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

    relay = %Builder{
      id: "transient_recovery_relay",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, %{pid: pid}} = RelayManager.start_relay(@namespace, relay)

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

  test "6.4 a parked relay un-parks via executor_migrate: pings, starts, clears init_error, activates plugins",
       %{test_pid: test_pid} do
    # Start the relay with a failing ping so it parks (alive, init_error set).
    # Started directly (not via RelayManager) so the parked-and-alive GenServer
    # is not stop_child_quietly'd — this is the cold-restart parked state.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :timeout} end)
    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    plugin = %Plugin{
      id: "unpark_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    relay = %Builder{id: "unpark_relay", factory_id: @factory, opts: [plugins: [plugin]]}

    {:ok, pid} = Flezha.Relay.Overwatch.start_link({@namespace, "stale_ce", relay})

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

    # A healthy executor is now reachable — un-park via executor_migrate.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:ok, :pong} end)
    executor_id = RelayManager.assign_executor(@namespace, @factory)
    assert is_binary(executor_id)

    assert :ok = GenServer.call(pid, {:executor_migrate, executor_id})

    # init_error cleared and the plugin activated (resumes producing).
    assert_receive :activated, 5_000
    state = GenServer.call(pid, :get_overwatch_state)
    assert is_nil(state.init_error)

    assert Flezha.Plugin.Extension.Table.phase(state.extensions, "unpark_plugin") == :running
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
       %{test_pid: test_pid} do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _executor_id ->
      {:error, :timeout}
    end)

    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    # QuietProvider's `activate/1` sends `:activated`. If the relay parks
    # without ever calling activate, no :activated message is sent — i.e.
    # the upstream connection an `activate/1` would open never happens.
    plugin = %Plugin{
      id: "cascade_plugin",
      provider: QuietProvider,
      opts: %{report_to: test_pid},
      subscriptions: [{:on_input, [position: 0]}]
    }

    relay = %Builder{
      id: "cascade_relay",
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    baseline = :erlang.memory(:processes)

    assert {:error, %{reason: {:ping_failed, :timeout}}} =
             RelayManager.start_relay(@namespace, relay)

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
