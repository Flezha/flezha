defmodule Flezha.State.RecoveryTest do
  @moduledoc """
  `Flezha.State.Recovery`: the three ways a desynced Cell is put back on a
  consistent point, and what each one needs to know first.
  """

  # async: false: writes Application env; Mimic stubs; a shared on-disk root.
  use FlezhaTest.Case, async: false
  use Mimic

  import Flezha.State.TestHelpers, only: [uniq: 0]

  alias Flezha.Operator.Builder
  alias Flezha.Service.HubManager
  alias Flezha.State.{Recovery, Restore, Supervisor, TestHelpers, WalWriter}

  describe "resync/2" do
    setup do
      FlezhaTest.Case.install([:tmp_app_data, :state_tree])

      parent = self()

      stub(Flezha.Runner.ControlChannel, :send_control, fn runner_id, {tag, _cmd} ->
        send(parent, {:control_sent, runner_id, tag})
        {:ok, make_ref()}
      end)

      %{header: TestHelpers.stub_header()}
    end

    test "no persisted state resyncs via CompactCell pull, never an empty RestoreInitState" do
      cell_id = TestHelpers.sample_cell_id("nodata_" <> uniq())
      # The writer's lazily-created zero-byte WAL is not persisted state.
      {:ok, _} = Supervisor.find_or_start(cell_id)

      refute Restore.has_persisted_state?(cell_id)

      assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: :seq_gap)

      assert_receive {:control_sent, "exec-1", :compact_cell}, 1_000
      refute_received {:control_sent, _, :restore_init_state}
    end

    test "persisted WAL records resync via RestoreInitState push", %{header: header} do
      cell_id = TestHelpers.sample_cell_id("data_" <> uniq())
      {:ok, _} = Supervisor.find_or_start(cell_id)
      {:ok, writer} = Supervisor.lookup(cell_id)
      :ok = WalWriter.append(writer, 0, header, "x")

      assert Restore.has_persisted_state?(cell_id)

      assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: :seq_gap)

      assert_receive {:control_sent, "exec-1", :restore_init_state}, 1_000
      refute_received {:control_sent, _, :compact_cell}
    end
  end

  describe "resync_for_overwatch/2" do
    defmodule SpyOverwatch do
      use GenServer

      def start_link(forward_to), do: GenServer.start_link(__MODULE__, forward_to)

      @impl true
      def init(forward_to), do: {:ok, %{forward_to: forward_to}}

      @impl true
      def handle_cast({:kick_restore, runner_id}, state) do
        send(state.forward_to, {:kick_restore_received, runner_id})
        {:noreply, state}
      end
    end

    test "casts :kick_restore with the runner_id" do
      {:ok, spy} = SpyOverwatch.start_link(self())

      assert :ok = Recovery.resync_for_overwatch(spy, "default_exec_1")

      assert_receive {:kick_restore_received, "default_exec_1"}, 500
    end

    test "takes a pid and a runner_id, and consults no registry" do
      # A class->runner lookup through HubManager / ProcessHub.tag_query would
      # crash with no stub present, so running clean here is what proves the
      # bind-driven trigger needs neither. No `Mimic.stub` and no Application env.
      {:ok, spy} = SpyOverwatch.start_link(self())

      assert :ok = Recovery.resync_for_overwatch(spy, "exec_9")
      assert_receive {:kick_restore_received, "exec_9"}, 500
    end
  end

  describe "resolve_runner/1" do
    test "resolves the runner tag for an operator whose :operator_class opt matches" do
      operator = %Builder{id: "btc_kraken", factory_id: "f", opts: [operator_class: "market"]}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"btc_kraken" => entry(operator, "exec_123")}}
      end)

      assert Recovery.resolve_runner({"market", "0", "p1"}) == {:ok, "exec_123"}
    end

    test "falls back to the operator id when no :operator_class opt is set" do
      operator = %Builder{id: "market", factory_id: "f", opts: []}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"market" => entry(operator, "exec_9")}}
      end)

      assert Recovery.resolve_runner({"market", "0", "p1"}) == {:ok, "exec_9"}
    end

    test "errors when no registered operator owns the operator_class" do
      operator = %Builder{id: "eth", factory_id: "f", opts: [operator_class: "other"]}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"eth" => entry(operator, "exec_1")}}
      end)

      assert {:error, msg} = Recovery.resolve_runner({"market", "0", "p1"})
      assert msg =~ "no active runner owns operator_class=market"
    end

    test "tolerates a {cspec, node_pids} 2-tuple row with no metadata (no crash)" do
      operator = %Builder{id: "market", factory_id: "f", opts: []}
      spec = cspec(operator)

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"market" => {spec, [{node(), self()}]}}}
      end)

      # No metadata -> no tag -> not a match, and the 2-tuple shape must not crash.
      assert {:error, _} = Recovery.resolve_runner({"market", "0", "p1"})
    end

    test "surfaces a registry error" do
      stub(HubManager, :get_registry, fn "default" -> {:error, "hub not alive"} end)

      assert Recovery.resolve_runner({"market", "0", "p1"}) == {:error, "hub not alive"}
    end
  end

  # The durable-registry entry shape `resolve_runner/1` matches:
  # `{operator_id, {cspec, node_pids, metadata}}`, runner = `metadata.tag`,
  # operator_class = the operator's `:operator_class` opt (default: its id),
  # carried in the cspec's start args.
  defp cspec(operator) do
    %{start: {Flezha.Operator.Overwatch, :start_link, [{"default", operator}]}}
  end

  defp entry(operator, tag) do
    {cspec(operator), [{node(), self()}], %{tag: tag, health: :healthy}}
  end
end
