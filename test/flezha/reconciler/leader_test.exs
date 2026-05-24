defmodule Flezha.Reconciler.LeaderTest do
  @moduledoc """
  Orchestration-leadership tracking (self-healing-control-plane, Phase 4 / D2).

  The pure-logic tests drive the `Leader` GenServer callbacks directly with
  constructed state (no elector / `:global`), covering `is_leader?`/`leader`
  semantics, transition tracking, and the transition telemetry. The integration
  tests restart the running singleton with `:leadership_enabled` on to exercise
  real single-node election and the watcher-death `Process.monitor` fallback.

  Cross-node re-election is covered by ProcessHub's own `:elector`-backed
  leadership suite and the Docker multi-node verification (§13.4).
  """
  use ExUnit.Case, async: false

  alias Flezha.Reconciler.Leader

  describe "is_leader?/leader semantics (pure callbacks)" do
    test "enabled + elected → leader" do
      state = %Leader{enabled: true, leader: node(), am_i_leader: true}
      assert {:reply, true, ^state} = Leader.handle_call(:is_leader?, self(), state)
      assert {:reply, leader, ^state} = Leader.handle_call(:leader, self(), state)
      assert leader == node()
    end

    test "enabled + not elected → not leader" do
      state = %Leader{enabled: true, leader: :other@host, am_i_leader: false}
      assert {:reply, false, _} = Leader.handle_call(:is_leader?, self(), state)
    end

    test "disabled → solo leader so cluster-wide reconcile still runs" do
      state = %Leader{enabled: false}
      assert {:reply, true, _} = Leader.handle_call(:is_leader?, self(), state)
      assert {:reply, :disabled, _} = Leader.handle_call(:leader, self(), state)
    end
  end

  describe "leadership transition handling (pure callback)" do
    test "tracks a transition and flips am_i_leader" do
      state = %Leader{enabled: true, leader: :none, am_i_leader: false}

      msg = {:processhub_leadership, %{leader: node(), previous: :none, am_i_leader: true}}
      assert {:noreply, new_state} = Leader.handle_info(msg, state)

      assert new_state.leader == node()
      assert new_state.am_i_leader == true
    end

    test "emits [:flezha, :leadership, :transition] telemetry on leader change" do
      handler_id = "leader-transition-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:flezha, :leadership, :transition],
        fn _event, measurements, metadata, _ ->
          send(test_pid, {:transition, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      state = %Leader{enabled: true, leader: :none, am_i_leader: false}
      msg = {:processhub_leadership, %{leader: node(), previous: :none, am_i_leader: true}}
      assert {:noreply, _} = Leader.handle_info(msg, state)

      assert_receive {:transition, %{count: 1}, %{leader: leader, am_i_leader: true}}
      assert leader == node()
    end

    test "no telemetry when the leader is unchanged" do
      handler_id = "leader-noop-#{System.unique_integer([:positive])}"
      test_pid = self()

      :telemetry.attach(
        handler_id,
        [:flezha, :leadership, :transition],
        fn _e, m, md, _ -> send(test_pid, {:transition, m, md}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      state = %Leader{enabled: true, leader: node(), am_i_leader: true}
      msg = {:processhub_leadership, %{leader: node(), previous: node(), am_i_leader: true}}
      assert {:noreply, _} = Leader.handle_info(msg, state)

      refute_receive {:transition, _, _}, 100
    end
  end

  describe "running singleton (disabled default test config)" do
    test "reports solo leader" do
      assert Leader.is_leader?() == true
      assert Leader.leader() == :disabled
    end
  end

  describe "real leadership (single-node election + watcher fallback)" do
    setup do
      Application.put_env(:flezha, :leadership_enabled, true)
      restart_leader()

      on_exit(fn ->
        Application.put_env(:flezha, :leadership_enabled, false)

        try do
          ProcessHub.stop_leadership()
        catch
          _, _ -> :ok
        end

        restart_leader()
      end)

      :ok
    end

    test "starts leadership and the sole node becomes leader" do
      assert eventually(fn -> Leader.is_leader?() end)
      assert Leader.leader() == node()
    end

    test "recovers when the leadership watcher dies (Process.monitor fallback)" do
      assert eventually(fn -> Leader.is_leader?() end)

      watcher = ProcessHub.Service.Leadership.Watcher.whereis()
      assert is_pid(watcher)
      Process.exit(watcher, :kill)

      # Leader detects the DOWN, re-subscribes, and re-derives leadership.
      assert eventually(fn -> Leader.is_leader?() end)
    end
  end

  defp restart_leader() do
    :ok = Supervisor.terminate_child(Flezha.Supervisor, Leader)

    case Supervisor.restart_child(Flezha.Supervisor, Leader) do
      {:ok, _} -> :ok
      {:ok, _, _} -> :ok
    end
  end

  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts <= 0 ->
        false

      true ->
        Process.sleep(50)
        eventually(fun, attempts - 1)
    end
  end
end
