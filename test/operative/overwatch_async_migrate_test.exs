defmodule FlezhaTest.Operative.OverwatchAsyncMigrateTest do
  @moduledoc """
  Phase-3 (consume-recovery-mode-and-self-heal §3.3-3.5) async
  `:runner_migrate` task contract. Exercises the
  `stop_operative-during-in-flight-migrate` invariant: the bounded retry
  loop now runs in a `Task.Supervisor.async_nolink/3` task, so the
  Overwatch's `:stop_operative` call returns within its normal handle_call
  window even mid-retry.
  """

  use ExUnit.Case, async: false

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.OperativeManager

  @namespace "default"
  @factory "store_input"

  setup do
    test_pid = self()
    ets = :ets.new(:overwatch_async_migrate_test, [:public, :set])

    on_exit(fn ->
      OperativeManager.stop_all_operatives(@namespace)
      Application.delete_env(:flezha, :overwatch_ping_fn)
      Application.delete_env(:flezha, :overwatch_reset_channel_fn)
    end)

    {:ok, test_pid: test_pid, ets: ets}
  end

  # `await_active_runner` pings once to confirm the runner is reachable
  # before the start-retry path runs. To reach the PARKED state these tests
  # need, the readiness gate (first ping) must pass while the START pings
  # fail — otherwise the operative stops with `:runner_not_ready`.
  defp park_after_gate(ets) do
    fn _runner_id ->
      case :ets.update_counter(ets, :calls, {2, 1}, {:calls, 0}) do
        1 -> {:ok, :pong}
        _ -> {:error, :timeout}
      end
    end
  end

  test "stop_operative returns :ok within 1s while a parked-unpark migrate task is in flight",
       %{test_pid: test_pid, ets: ets} do
    # The async migrate path lives on the parked-unpark branch — that's
    # where the bounded retry loop with the 19 s aggregate budget runs.
    # Boot a parked operative (start ping fails after the readiness gate) and
    # then trigger un-park with a ping that blocks for several seconds,
    # simulating the spec's "~10 s into the backoff schedule" scenario.
    Application.put_env(:flezha, :overwatch_ping_fn, park_after_gate(ets))
    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    operative = %Builder{id: "stop_during_migrate", factory_id: @factory, opts: []}

    {:ok, overwatch_pid} =
      Flezha.Operative.Overwatch.start_link({@namespace, "stale_ce", operative})

    on_exit(fn ->
      if Process.alive?(overwatch_pid) do
        try do
          GenServer.stop(overwatch_pid, :normal, 1_000)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    # Wait for the operative to park.
    assert eventually(fn ->
             GenServer.call(overwatch_pid, :get_overwatch_state).init_error != nil
           end)

    # Replace ping with a blocking one so the un-park retry task runs
    # for ~5 s.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _runner_id ->
      send(test_pid, :ping_started)
      :timer.sleep(5_000)
      {:ok, :pong}
    end)

    # Fire the un-park migrate. The Overwatch must return `:ok`
    # immediately (the retry task is spawned out-of-band).
    :ok = GenServer.call(overwatch_pid, {:runner_migrate, "fake_new_runner"}, 1_000)

    assert_receive :ping_started, 500

    # Now stop the operative. This MUST return within 1 s even though the
    # migrate task is blocked inside the 5 s sleep.
    started = System.monotonic_time(:millisecond)
    assert :ok = GenServer.call(overwatch_pid, :stop_operative, 1_500)
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 1_000,
           "stop_operative returned in #{elapsed} ms; required < 1 s while migrate task is in flight"
  end

  test "starting a second parked-unpark migrate while one is in flight returns :busy",
       %{test_pid: test_pid, ets: ets} do
    Application.put_env(:flezha, :overwatch_ping_fn, park_after_gate(ets))
    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    operative = %Builder{id: "busy_migrate_test", factory_id: @factory, opts: []}

    {:ok, overwatch_pid} =
      Flezha.Operative.Overwatch.start_link({@namespace, "stale_ce", operative})

    on_exit(fn ->
      if Process.alive?(overwatch_pid) do
        try do
          GenServer.stop(overwatch_pid, :normal, 1_000)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    assert eventually(fn ->
             GenServer.call(overwatch_pid, :get_overwatch_state).init_error != nil
           end)

    # First un-park migrate: long blocking ping.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _runner_id ->
      send(test_pid, :ping_started)
      :timer.sleep(2_000)
      {:ok, :pong}
    end)

    :ok = GenServer.call(overwatch_pid, {:runner_migrate, "first"}, 1_000)
    assert_receive :ping_started, 500

    # Second migrate must be rejected as :busy.
    assert {:error, :busy} =
             GenServer.call(overwatch_pid, {:runner_migrate, "second"}, 1_000)
  end

  # fails when: the running-migrate branch commits the binding before the runner
  # answers. A cross-node heal target is unreachable by construction (the runner
  # registry is node-local), so the failed attempt would leave the operative
  # naming a LIVE runner it cannot reach — `Convergence.Diff` then reads it as
  # bound-and-healthy, drops it from `misbound_operatives`, and the wedge becomes
  # permanently invisible.
  test "a failed running-migrate leaves the binding untouched" do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :no_control_unit} end)

    state = running_state()

    assert {:reply, {:error, {:ping_failed, :no_control_unit}}, unchanged} =
             Overwatch.handle_call({:runner_migrate, "runner_b"}, {self(), make_ref()}, state)

    assert unchanged.runner_id == "runner_a"
    assert unchanged.channel == :chan_a
  end

  # The success half — the binding IS committed once the runner answers — is
  # covered end-to-end against a real runner by `flezha_test.exs` "runner
  # disconnect migrate op".

  # fails when: `alter_handover_state` marks `arrived_via_hotswap?` even for a
  # handover that carried nothing. The flag suppresses the per-Segment WAL
  # restore at bind time AND the explicit `kick_restore` repair, so a migration
  # away from a DEAD runner — where the state source is gone and the handover is
  # empty by construction — starts fresh and can never be replayed back.
  test "an empty handover does not suppress the WAL restore" do
    handover = %{
      id: "handover_guard",
      namespace: @namespace,
      factory_id: @factory,
      init_values: %{},
      plugins: []
    }

    state = running_state()

    # The struct has no reachable control unit, so the deserialization dispatch
    # logs an error it is designed to tolerate; the flag is the assertion.
    ExUnit.CaptureLog.capture_log(fn ->
      refute Overwatch.alter_handover_state(state, {handover, nil}).arrived_via_hotswap?
      refute Overwatch.alter_handover_state(state, {handover, <<>>}).arrived_via_hotswap?
      assert Overwatch.alter_handover_state(state, {handover, "sdk-state"}).arrived_via_hotswap?
    end)
  end

  defp running_state() do
    id = "migrate_bind_guard"

    %Overwatch{
      id: id,
      namespace: @namespace,
      runner_id: "runner_a",
      factory_id: @factory,
      channel: :chan_a,
      builder: %Builder{id: id, factory_id: @factory, opts: []},
      extensions: Table.new(Overwatch.__extensions_catalog__())
    }
  end

  defp eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(25)
        do_eventually(fun, deadline)
    end
  end
end
