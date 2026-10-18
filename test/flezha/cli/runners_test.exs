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
    stub(FleetManager, :global_runner_rows, fn "trading" ->
      {:ok,
       [
         %{
           id: "trading_2bfeedb5e74f",
           node: :flezha@host,
           connected_at: 1_791_590_400,
           pid: 48_213,
           silent_ms: 4_000,
           late?: false
         },
         %{
           id: "trading_d4c7e284bc33",
           node: :flezha@other,
           connected_at: nil,
           pid: nil,
           silent_ms: nil,
           late?: true
         }
       ]}
    end)

    :ok
  end

  # fails when: the list stops saying which process a runner is, where it is
  # held or since when, so a user who wants to crash it has nothing to kill.
  test "a runner is listed with its pid, node, connect time and lateness, a dash where unknown" do
    output = capture_io(fn -> Cli.call("Fleet.runners", Jason.encode!(["trading"]), []) end)

    assert output =~ "Runners in namespace 'trading':"

    assert output =~
             ~r/trading_2bfeedb5e74f\s+48213\s+flezha@host\s+2026-10-10T00:00:00Z\s+no\n/

    assert output =~ ~r/trading_d4c7e284bc33\s+-\s+flezha@other\s+-\s+yes\n/
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

  test "the JSON form carries each runner's row whole" do
    output =
      capture_io(fn -> Cli.call("Fleet.runners", Jason.encode!(["trading"]), output: "json") end)

    assert Jason.decode!(output) == [
             %{
               "id" => "trading_2bfeedb5e74f",
               "node" => "flezha@host",
               "connected_at" => 1_791_590_400,
               "pid" => 48_213,
               "silent_ms" => 4_000,
               "late?" => false
             },
             %{
               "id" => "trading_d4c7e284bc33",
               "node" => "flezha@other",
               "connected_at" => nil,
               "pid" => nil,
               "silent_ms" => nil,
               "late?" => true
             }
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
