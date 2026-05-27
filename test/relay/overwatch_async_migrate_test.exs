defmodule FlezhaTest.Relay.OverwatchAsyncMigrateTest do
  @moduledoc """
  Phase-3 (consume-recovery-mode-and-self-heal §3.3-3.5) async
  `:executor_migrate` task contract. Exercises the
  `stop_relay-during-in-flight-migrate` invariant: the bounded retry
  loop now runs in a `Task.Supervisor.async_nolink/3` task, so the
  Overwatch's `:stop_relay` call returns within its normal handle_call
  window even mid-retry.
  """

  use ExUnit.Case, async: false

  alias Flezha.Relay.Builder
  alias Flezha.Service.RelayManager

  @namespace "default"
  @factory "store_input"

  setup do
    test_pid = self()

    on_exit(fn ->
      RelayManager.stop_all_relays(@namespace)
      Application.delete_env(:flezha, :overwatch_ping_fn)
      Application.delete_env(:flezha, :overwatch_reset_channel_fn)
    end)

    {:ok, test_pid: test_pid}
  end

  test "stop_relay returns :ok within 1s while a parked-unpark migrate task is in flight",
       %{test_pid: test_pid} do
    # The async migrate path lives on the parked-unpark branch — that's
    # where the bounded retry loop with the 19 s aggregate budget runs.
    # Boot a parked relay (failing ping) and then trigger un-park with a
    # ping that blocks for several seconds, simulating the spec's
    # "~10 s into the backoff schedule" scenario.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :timeout} end)
    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    relay = %Builder{id: "stop_during_migrate", factory_id: @factory, opts: []}
    {:ok, overwatch_pid} = Flezha.Relay.Overwatch.start_link({@namespace, "stale_ce", relay})

    on_exit(fn ->
      if Process.alive?(overwatch_pid) do
        try do
          GenServer.stop(overwatch_pid, :normal, 1_000)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    # Wait for the relay to park.
    assert eventually(fn ->
             GenServer.call(overwatch_pid, :get_overwatch_state).init_error != nil
           end)

    # Replace ping with a blocking one so the un-park retry task runs
    # for ~5 s.
    Application.put_env(:flezha, :overwatch_ping_fn, fn _executor_id ->
      send(test_pid, :ping_started)
      :timer.sleep(5_000)
      {:ok, :pong}
    end)

    # Fire the un-park migrate. The Overwatch must return `:ok`
    # immediately (the retry task is spawned out-of-band).
    :ok = GenServer.call(overwatch_pid, {:executor_migrate, "fake_new_executor"}, 1_000)

    assert_receive :ping_started, 500

    # Now stop the relay. This MUST return within 1 s even though the
    # migrate task is blocked inside the 5 s sleep.
    started = System.monotonic_time(:millisecond)
    assert :ok = GenServer.call(overwatch_pid, :stop_relay, 1_500)
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 1_000,
           "stop_relay returned in #{elapsed} ms; required < 1 s while migrate task is in flight"
  end

  test "starting a second parked-unpark migrate while one is in flight returns :busy",
       %{test_pid: test_pid} do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :timeout} end)
    Application.put_env(:flezha, :overwatch_reset_channel_fn, fn _ -> :ok end)

    relay = %Builder{id: "busy_migrate_test", factory_id: @factory, opts: []}
    {:ok, overwatch_pid} = Flezha.Relay.Overwatch.start_link({@namespace, "stale_ce", relay})

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
    Application.put_env(:flezha, :overwatch_ping_fn, fn _executor_id ->
      send(test_pid, :ping_started)
      :timer.sleep(2_000)
      {:ok, :pong}
    end)

    :ok = GenServer.call(overwatch_pid, {:executor_migrate, "first"}, 1_000)
    assert_receive :ping_started, 500

    # Second migrate must be rejected as :busy.
    assert {:error, :busy} =
             GenServer.call(overwatch_pid, {:executor_migrate, "second"}, 1_000)
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
