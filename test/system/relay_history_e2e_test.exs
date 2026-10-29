defmodule FlezhaTest.System.RelayHistoryE2ETest do
  @moduledoc """
  The relay history against a live runner: every deliberate start writes one
  `started` record and every end for good one `ended` record, read back through
  `Flezha.Service.Logs`, while a crash restart writes nothing.

      mix test --include system test/system/relay_history_e2e_test.exs
  """

  use FlezhaTest.SystemCase, capability: :steps

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, ensure_port_dead: 1]

  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Logs
  alias Flezha.Service.Relay
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  @namespace "relay_history_e2e"
  @operator %{source: :cli, request_id: "req-history"}

  setup_all do
    {port, _runner_id} = NodeTestHelper.start_runner_port(@namespace)

    on_exit(fn ->
      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end

      ensure_port_dead(port)
    end)

    :ok
  end

  setup do
    on_exit(fn ->
      try do
        RelayManager.stop_all_relays(@namespace)
      catch
        _, _ -> :ok
      end
    end)
  end

  test "a steps run an operator started leaves one started and one ended record" do
    id = "history_echo_#{System.unique_integer([:positive])}"

    assert {:ok, %{id: ^id}} =
             Relay.start(@namespace, [factory_id: "echo_steps", id: id, text: "hi"], @operator)

    assert wait_until(fn -> length(records(id)) == 2 end, 10_000),
           "the run left #{inspect(records(id))}"

    [ended, started] = records(id)

    assert %{"event" => "started", "factory" => "echo_steps", "run" => "steps"} = started

    assert %{"kind" => "operator", "source" => "cli", "request_id" => "req-history"} =
             started["origin"]

    assert %{"event" => "ended", "outcome" => "completed", "halted" => false} = ended
    assert ended["first_started_at"] == started["first_started_at"]
    assert is_integer(ended["duration_ms"]) and ended["duration_ms"] >= 0
    assert ended["node"] == Atom.to_string(node())
  end

  test "a caller's own origin never reaches a record" do
    id = "history_forged_#{System.unique_integer([:positive])}"
    forged = %{"origin" => %{"kind" => "schedule"}, "first_started_at" => 1}

    assert {:ok, _} =
             Relay.start(
               @namespace,
               [factory_id: "echo_steps", id: id, text: "hi", opts: forged],
               @operator
             )

    assert wait_until(fn -> length(records(id)) == 2 end, 10_000)
    assert Enum.all?(records(id), &(&1["origin"]["kind"] == "operator"))
    assert Enum.all?(records(id), &(&1["first_started_at"] != 1))
  end

  test "a rolled-back run ends undone and a run with nothing to roll back ends failed" do
    undone = "history_undo_#{System.unique_integer([:positive])}"
    failed = "history_failed_#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             Relay.start(
               @namespace,
               [factory_id: "failing_turn_undo_all_steps", id: undone],
               @operator
             )

    for turn <- ~w(hello boom), do: send_turn(@namespace, undone, turn)

    assert {:ok, _} =
             Relay.start(
               @namespace,
               [factory_id: "failed_stage_steps", id: failed, text: "b"],
               @operator
             )

    assert %{"outcome" => "undone", "error" => error} = ended(undone)
    assert error =~ "boom"
    assert %{"outcome" => "failed", "halted" => false} = ended(failed)
  end

  test "an operator's stop of a running loop ends it stopped, not timed out" do
    id = start_loop("history_stop")
    assert {:ok, %{stopped: :cooperative}} = Relay.stop(@namespace, id, @operator)
    assert %{"outcome" => "stopped", "halted" => false} = ended(id)
  end

  test "a halted steps instance is stopped outright and ends stopped, halted" do
    id = start_loop("history_halted")
    {:ok, pid} = RelayManager.get_relay_pid(@namespace, id)
    send(pid, {:restore_refused, :history_drill})
    assert wait_until(fn -> :sys.get_state(pid).phase == :halted end, 5_000)

    assert {:ok, %{stopped: :stopped}} = Relay.stop(@namespace, id, @operator)
    refute Process.alive?(pid)
    assert {:error, _} = RelayManager.get_relay_pid(@namespace, id)
    assert %{"outcome" => "stopped", "halted" => true} = ended(id)
  end

  test "a spawned run names the relay whose tasklet asked for it" do
    parent = start_loop("history_parent", "spawn_once_requester")
    send_turn(@namespace, parent, "ticket")

    assert wait_until(fn -> children(parent) != [] end, 30_000)
    [child] = children(parent)
    assert %{"factory" => "typed_once_steps", "origin" => origin} = child
    assert %{"kind" => "spawn", "parent_relay" => ^parent, "principal" => "runner@" <> _} = origin
    # A flow parent takes no input-log offset, so its spawn names no identity.
    refute Map.has_key?(child, "spawn_identity")
    assert %{"outcome" => "completed"} = ended(child["relay"])
  end

  # The killed Overwatch's supervisor reports its restart.
  @tag :capture_log
  test "a crash restart, a duplicate start and an unannounced start write nothing" do
    id = start_loop("history_quiet")
    {:ok, pid} = RelayManager.get_relay_pid(@namespace, id)
    Process.exit(pid, :kill)

    assert wait_until(
             fn ->
               match?({:ok, new} when new != pid, RelayManager.get_relay_pid(@namespace, id)) and
                 match?(
                   {:ok, %{runner_id: runner}} when is_binary(runner),
                   RelayManager.get_overwatch_state(@namespace, id)
                 )
             end,
             15_000
           )

    assert {:ok, %{outcome: :already_running}} =
             Relay.start(@namespace, [factory_id: "chat_loop_steps", id: id], @operator)

    quiet = "history_unannounced_#{System.unique_integer([:positive])}"
    Telemetry.capture([:flezha, :relay, :started], filter: &(&1.relay == quiet))
    builder = %Builder{id: quiet, factory_id: "chat_loop_steps"}

    assert {:ok, _} =
             RelayManager.start_relay(@namespace, builder, announce: false, record?: false)

    assert [%{"event" => "started"}] = records(id)
    assert records(quiet) == []
    refute_received {:telemetry, [:flezha, :relay, :started], _, _}
  end

  defp start_loop(prefix, factory \\ "chat_loop_steps") do
    id = "#{prefix}_#{System.unique_integer([:positive])}"
    assert {:ok, _} = Relay.start(@namespace, [factory_id: factory, id: id], @operator)
    assert wait_until(fn -> length(records(id)) == 1 end, 10_000)
    id
  end

  defp ended(id) do
    assert wait_until(fn -> Enum.any?(records(id), &(&1["event"] == "ended")) end, 60_000),
           "#{id} never ended: #{inspect(records(id))}"

    Enum.find(records(id), &(&1["event"] == "ended"))
  end

  defp children(parent) do
    {:ok, %{records: records}} =
      Logs.read(
        %{kind: "relay_history", filters: %{event: "started", factory: "typed_once_steps"}},
        %{source: :internal}
      )

    Enum.filter(records, &(&1["origin"]["parent_relay"] == parent))
  end

  defp send_turn(namespace, id, text),
    do: {:ok, _} = Relay.send(namespace, id, text, "in", [], @operator)

  defp records(id) do
    {:ok, %{records: records}} =
      Logs.read(%{kind: "relay_history", filters: %{relay: id}}, %{source: :internal})

    records
  end
end
