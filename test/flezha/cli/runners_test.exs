defmodule Flezha.Service.Cli.RunnersTest do
  # async: false: Mimic stubs; the auth stores are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [
      {:auth_stores,
       prefix: "flz_cli_runners",
       stores: [Flezha.AuthZ.Replica, Flezha.Runner.DropMarks],
       enabled: false}
    ],
    async: false

  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Runner.DropMarks
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Cli

  setup do
    stub(FleetManager, :global_runner_pids, fn "trading" ->
      {:ok, [{"trading_2bfeedb5e74f", 48_213}, {"trading_d4c7e284bc33", nil}]}
    end)

    :ok
  end

  # fails when: the list drops the process id, so a user who wants to crash a
  # runner has nothing to kill it by.
  test "a runner is listed with its process id, and one whose transport cannot say without" do
    output = capture_io(fn -> Cli.call("Fleet.runners", Jason.encode!(["trading"]), []) end)

    assert output =~ "  - trading_2bfeedb5e74f  pid 48213\n"
    assert output =~ "  - trading_d4c7e284bc33\n"
  end

  describe "drops" do
    test "flz runner drop prints the mark it placed" do
      stub(FleetManager, :find_session, fn "trading", "trading_2bfeedb5e74f" ->
        %{stable_id: "stable-a", incarnation: "i1", pid: 48_213, node: :flezha@host}
      end)

      output =
        capture_io(fn ->
          Cli.call("Fleet.drop_runner", Jason.encode!(["trading", "trading_2bfeedb5e74f"]), [])
        end)

      assert %{"stable_id" => "stable-a", "incarnation" => "i1", "pid" => 48_213} =
               Jason.decode!(output)
    end

    test "flz runner dropped lists each mark, with a dash for what is not known" do
      {:ok, _} = DropMarks.put(mark("stable-a", 48_213, "ops-caller", 1_791_590_400))
      {:ok, _} = DropMarks.put(mark("stable-b", nil, nil, 1_791_590_460))

      output =
        capture_io(fn -> Cli.call("Fleet.dropped_runners", Jason.encode!(["trading"]), []) end)

      assert output =~ "Dropped runners in namespace 'trading':"
      assert [a, b] = output |> String.split("\n") |> Enum.filter(&(&1 =~ "stable-"))
      assert a =~ ~r/stable-a\s+48213\s+flezha@host\s+2026-10-10T00:00:00Z\s+ops-caller$/
      assert b =~ ~r/stable-b\s+-\s+flezha@host\s+\S+\s+-$/

      empty = capture_io(fn -> Cli.call("Fleet.dropped_runners", ~s(["other"]), []) end)
      assert empty == "No dropped runners in namespace 'other'.\n"
    end
  end

  test "the JSON form carries each runner with its process id, or none" do
    output =
      capture_io(fn -> Cli.call("Fleet.runners", Jason.encode!(["trading"]), output: "json") end)

    assert Jason.decode!(output) == [
             %{"id" => "trading_2bfeedb5e74f", "pid" => 48_213},
             %{"id" => "trading_d4c7e284bc33", "pid" => nil}
           ]
  end

  defp mark(stable_id, pid, dropped_by, at) do
    %{
      namespace: "trading",
      stable_id: stable_id,
      incarnation: "i1",
      pid: pid,
      node: :flezha@host,
      dropped_at: at,
      dropped_by: dropped_by
    }
  end
end
