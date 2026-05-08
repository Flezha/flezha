defmodule FlezhaTest.Executor.FleetManagerTest do
  alias Flezha.Executor.FleetManager
  alias Flezha.Service.HubManager

  use ExUnit.Case, async: false
  use Mimic

  setup :verify_on_exit!

  defp cspec(relay_id, factory_id) do
    %{
      id: relay_id,
      start:
        {Flezha.Relay.Overwatch, :start_link,
         [
           {"ns1", "old_ce",
            %Flezha.Relay.Builder{
              id: relay_id,
              factory_id: factory_id,
              init_values: %{},
              opts: []
            }}
         ]}
    }
  end

  defp entry(relay_id, factory_id, tag, pid) do
    {relay_id, {cspec(relay_id, factory_id), [{node(), pid}], %{tag: tag}}}
  end

  defp fake_overwatch(test_pid) do
    spawn_link(fn -> overwatch_loop(test_pid) end)
  end

  defp overwatch_loop(test_pid) do
    receive do
      {:"$gen_call", from, {:executor_migrate, _new_ce_id} = msg} ->
        send(test_pid, {:overwatch_called, self(), msg})
        GenServer.reply(from, :ok)
        overwatch_loop(test_pid)

      _ ->
        overwatch_loop(test_pid)
    end
  end

  describe "reissue_local_orphans/5" do
    test "reissues AddRelay for orphans whose factory matches the connecting executor" do
      test_pid = self()
      pid_p1 = fake_overwatch(test_pid)

      registry = Map.new([entry("p1", "f1", "dead_ce", pid_p1)])

      HubManager
      |> expect(:get_registry, fn "ns1" -> {:ok, registry} end)
      |> expect(:relay_set_executor, fn "ns1", "p1", "new_ce" -> :ok end)

      :ok =
        FleetManager.reissue_local_orphans(
          "ns1",
          "new_ce",
          [%{"id" => "f1"}],
          MapSet.new(),
          MapSet.new(["new_ce"])
        )

      assert_receive {:overwatch_called, ^pid_p1, {:executor_migrate, "new_ce"}}, 1_000
    end

    test "does not double-issue relays already claimed by try_migrate" do
      test_pid = self()
      pid_p1 = fake_overwatch(test_pid)
      pid_p2 = fake_overwatch(test_pid)

      registry =
        Map.new([
          entry("p1", "f1", "dead_ce", pid_p1),
          entry("p2", "f1", "dead_ce", pid_p2)
        ])

      HubManager
      |> expect(:get_registry, fn "ns1" -> {:ok, registry} end)
      |> expect(:relay_set_executor, fn "ns1", "p2", "new_ce" -> :ok end)

      :ok =
        FleetManager.reissue_local_orphans(
          "ns1",
          "new_ce",
          [%{"id" => "f1"}],
          MapSet.new(["p1"]),
          MapSet.new(["new_ce"])
        )

      assert_receive {:overwatch_called, ^pid_p2, {:executor_migrate, "new_ce"}}, 1_000
      refute_receive {:overwatch_called, ^pid_p1, _}, 200
    end

    test "skips relays whose factory_id is not declared by the connecting executor" do
      test_pid = self()
      pid_p1 = fake_overwatch(test_pid)
      pid_p2 = fake_overwatch(test_pid)

      registry =
        Map.new([
          entry("p1", "f1", "dead_ce", pid_p1),
          entry("p2", "f_other", "dead_ce", pid_p2)
        ])

      HubManager
      |> expect(:get_registry, fn "ns1" -> {:ok, registry} end)
      |> expect(:relay_set_executor, fn "ns1", "p1", "new_ce" -> :ok end)

      :ok =
        FleetManager.reissue_local_orphans(
          "ns1",
          "new_ce",
          [%{"id" => "f1"}],
          MapSet.new(),
          MapSet.new(["new_ce"])
        )

      assert_receive {:overwatch_called, ^pid_p1, _}, 1_000
      refute_receive {:overwatch_called, ^pid_p2, _}, 200
    end

    test "no-op when the hub registry is unavailable" do
      HubManager
      |> expect(:get_registry, fn "ns1" -> {:error, "hub not alive"} end)
      |> reject(:relay_set_executor, 3)

      :ok =
        FleetManager.reissue_local_orphans(
          "ns1",
          "new_ce",
          [%{"id" => "f1"}],
          MapSet.new(),
          MapSet.new(["new_ce"])
        )
    end
  end
end
