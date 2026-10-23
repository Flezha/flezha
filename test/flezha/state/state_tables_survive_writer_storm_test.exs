defmodule Flezha.State.StateTablesSurviveWriterStormTest do
  # async: false: exhausts the restart budget of the app tree's writer supervisor.
  use FlezhaTest.StateCase, async: false

  alias Flezha.Relay.ClassRegistry
  alias Flezha.State.{Supervisor, TestHelpers}
  alias FlezhaTest.Helper.NodeTestHelper

  @tables [
    :flezha_relay_class_registry,
    :flezha_state_namespaces,
    :flezha_input_log_watermarks,
    :flezha_remote_gate_owed
  ]

  test "the data-plane tables outlive a writer restart storm that exhausts the writers' supervisor" do
    class = "StormRelay" <> uniq()
    cell_id = TestHelpers.sample_cell_id("storm_" <> uniq())
    :ok = ClassRegistry.put_overwatch(class, self(), "identity-storm")
    on_exit(fn -> ClassRegistry.delete(class) end)
    owners = Map.new(@tables, &{&1, :ets.info(&1, :owner)})

    sup = Process.whereis(Supervisor)
    {:ok, first} = Supervisor.find_or_start(cell_id)
    # A writer failing on every write (a full disk) is restarted at once; past
    # the intensity its supervisor gives up.
    NodeTestHelper.with_only_logged(
      [
        {NodeTestHelper.crash_report(cell_id), :always},
        # A kill that lands while the restarted writer is still in init.
        {report(cell_id, "failed to start", "killed"), :maybe},
        {report(cell_id, "caused shutdown", ":reached_max_restart_intensity"), :always},
        {NodeTestHelper.crash_report(Supervisor, "shutdown"), :always}
      ],
      fn ->
        storm(cell_id, first, sup, 0)

        assert NodeTestHelper.wait_until(
                 fn -> Process.whereis(Supervisor) not in [nil, sup] end,
                 5_000
               )
      end
    )

    {:ok, writer} = Supervisor.find_or_start(cell_id)
    on_exit(fn -> TestHelpers.close_and_await(writer, cell_id) end)

    for table <- @tables do
      assert :ets.info(table, :owner) == owners[table],
             "#{table} was lost or recreated by a new owner after the storm"
    end

    assert {:ok, self(), "identity-storm"} == ClassRegistry.owner(class)
  end

  defp storm(_cell_id, _pid, _sup, kills) when kills > 150,
    do: flunk("the supervisor never gave up")

  defp storm(cell_id, pid, sup, kills) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

    # Either the supervisor restarted the writer or it gave up; the bound only
    # stops a hung run.
    assert NodeTestHelper.wait_until(
             fn -> not Process.alive?(sup) or restarted?(cell_id, pid) end,
             5_000,
             1
           )

    if Process.alive?(sup) do
      {:ok, next} = Supervisor.lookup(cell_id)
      storm(cell_id, next, sup, kills + 1)
    else
      :ok
    end
  end

  defp report(child, event, reason),
    do: ~r/ #{event}\n\*\* \(exit\) #{reason}\n(?s:.*)#{Regex.escape(inspect(child))}/

  defp restarted?(cell_id, pid), do: match?({:ok, p} when p != pid, Supervisor.lookup(cell_id))
end
