defmodule FlezhaTest.System.BindRestoreBackpressureE2ETest do
  @moduledoc """
  A bind restores every persisted Cell even when the restores outrun the
  control stream's send buffer.

  A bind ships one `RestoreInitState` per Cell, concurrently, on the control
  stream. Two Cells here are each larger than that stream's high-water mark
  (80 % of its 8 MiB buffer), so whichever goes first fills the buffer and the
  other is offered to a full one. The first bind follows a restart of the
  runner and of the controller's Cell writers, with the runner held still
  (`SIGSTOP`) while the restores go out — a slow host still decoding a large
  Cell. The second is the plain rebind the convergence loop issues onto the
  same runner.

  After each bind every Cell must hold what it held before, and its WAL must
  advance on the next input: a Cell bound on empty state restarts its seq below
  the WAL's, and every Change it ships after that is dropped as a duplicate.

  Tagged `:system` (boots a real subprocess). Run with
  `mix test --include system test/system/bind_restore_backpressure_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :dynamic_cell
  use Mimic

  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Payloader
  alias Flezha.Relay.Builder
  alias Flezha.Runner.ControlChannel
  alias Flezha.Service.RelayManager
  alias Flezha.State.Commands
  alias Flezha.State.WalWriter
  alias FlezhaTest.Helper.TierE2EHarness

  @namespace "bind_restore_backpressure"
  @factory "dynamic_cell"
  @tasklet "dynamic_cell_tasklet"

  # Inputs per Cell, 100 kB each: a large Cell's restore (7.2 MB) crosses the
  # 6.7 MB high-water mark on its own.
  @cells %{"big1" => 72, "big2" => 72, "s1" => 3, "s2" => 3, "s3" => 3, "s4" => 3}
  @keys Map.keys(@cells)
  @padding String.duplicate("x", 100_000)
  @stall_ms 1_500

  setup :set_mimic_global_until_exit

  setup do
    on_exit(fn -> TierE2EHarness.stop_namespace(@namespace) end)
  end

  # fails when: a controller restart or a relay move binds the market relay
  # with some indicator Cells on empty state, and their WAL stops advancing
  # (`relay stuck: wal_frozen`) while the runner keeps producing, through
  # every rebind the convergence loop tries.
  @tag reference_runtime: true
  @tag timeout: 180_000
  test "every Cell is restored when the restores fill the control stream's send buffer" do
    id_path = TierE2EHarness.unique_id_path()
    {_port_a, runner_a} = TierE2EHarness.boot_runner(@namespace, id_path)

    relay_id = "restore_bp_#{System.unique_integer([:positive])}"
    builder = %Builder{id: relay_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, _info} = RelayManager.start_relay(@namespace, builder)

    TierE2EHarness.send_inputs(
      relay_id,
      @namespace,
      for({key, inputs} <- @cells, n <- 1..inputs, do: "#{key}:#{n}:#{@padding}")
    )

    assert wait_until(
             fn -> Enum.all?(@cells, fn {key, n} -> last_seq(relay_id, key) >= n - 1 end) end,
             30_000
           ),
           "every Change must be on disk before the bind restores from it"

    TierE2EHarness.sigkill(ControlChannel.peer_pid(runner_a))
    # A controller restart boots every Cell writer from its file, owing nothing
    # to the runner that wrote it.
    for key <- @keys, do: TierE2EHarness.crash_writer({relay_id, @tasklet, key})
    stall_runner_on_first_restore()
    {_port_b, runner_b} = TierE2EHarness.boot_runner(@namespace, id_path)

    assert wait_until(fn -> bound_runner(relay_id) == runner_b end, 60_000),
           "the relay never rebound from #{runner_a} onto #{runner_b}"

    assert_restored(relay_id, @cells)

    TierE2EHarness.force_fresh_rebind(@namespace, relay_id)
    assert_restored(relay_id, Map.new(@cells, fn {key, n} -> {key, n + 1} end))
  end

  # Every Cell holds `expected` inputs, and the next input advances its WAL.
  defp assert_restored(relay_id, expected) do
    assert wait_until(fn -> seen_counts(relay_id) == expected end, 30_000),
           "a Cell came back without its state: #{inspect(seen_counts(relay_id))}"

    before = Map.new(@keys, &{&1, last_seq(relay_id, &1)})
    TierE2EHarness.send_inputs(relay_id, @namespace, Enum.map(@keys, &"#{&1}:next"))

    assert wait_until(fn -> Enum.all?(@keys, &(last_seq(relay_id, &1) > before[&1])) end, 30_000),
           "a Cell's WAL stopped advancing: before=#{inspect(before)} " <>
             "after=#{inspect(Map.new(@keys, &{&1, last_seq(relay_id, &1)}))}"
  end

  # Every restore of the bind finds the runner stopped, and the runner resumes
  # `@stall_ms` after the first one arrived: the restores are offered while the
  # first is stuck in the send buffer.
  defp stall_runner_on_first_restore do
    # Slot 1: a restore claimed the stop. Slot 2: the runner is stopped.
    stall = :atomics.new(2, [])

    stub(Commands, :await_restore_init_state, fn runner_id, cell_id ->
      if :atomics.compare_exchange(stall, 1, 0, 1) == :ok do
        os_pid = ControlChannel.peer_pid(runner_id)
        TierE2EHarness.signal(os_pid, "-STOP")
        :atomics.put(stall, 2, 1)
        spawn(fn -> Process.sleep(@stall_ms) && TierE2EHarness.signal(os_pid, "-CONT") end)
      end

      await_stopped(stall)
      call_original(Commands, :await_restore_init_state, [runner_id, cell_id])
    end)
  end

  defp await_stopped(stall) do
    if :atomics.get(stall, 2) == 0, do: Process.sleep(1) && await_stopped(stall)
  end

  defp bound_runner(relay_id) do
    case RelayManager.get_overwatch_state(@namespace, relay_id) do
      {:ok, %{runner_id: runner_id}} -> runner_id
      _ -> nil
    end
  end

  # One Cell at a time: the whole relay's state is too large for one read.
  defp seen_counts(relay_id), do: Map.new(@keys, &{&1, seen_count(relay_id, &1)})

  defp seen_count(relay_id, key) do
    query = %{tasklet: @tasklet, cell_key: key}

    with {:ok, json, _seq} <- RelayManager.get_cell_state(@namespace, relay_id, query),
         %{@tasklet => %{^key => %{"seen" => seen}}} <- Payloader.json_to_map(json) do
      length(seen)
    else
      _ -> 0
    end
  end

  defp last_seq(relay_id, key) do
    {:ok, writer} = Flezha.State.Supervisor.find_or_start({relay_id, @tasklet, key})
    WalWriter.stall_status(writer).last_persisted_seq
  end
end
