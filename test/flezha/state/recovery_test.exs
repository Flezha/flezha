defmodule Flezha.State.RecoveryTest do
  @moduledoc """
  `Flezha.State.Recovery`: the three ways a desynced Entity is put back on a
  consistent point, and what each one needs to know first.
  """

  # async: false: writes Application env; Mimic stubs; a shared on-disk root.
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Operative.Builder
  alias Flezha.Service.HubManager
  alias Flezha.State.{Recovery, Restore, Supervisor, TestHelpers, WalWriter}

  describe "resync/2" do
    setup do
      base = TestHelpers.tmp_app_data_dir()

      TestHelpers.start_state_tree()

      parent = self()

      stub(Flezha.Runner.ControlChannel, :send_control, fn runner_id, {tag, _cmd} ->
        send(parent, {:control_sent, runner_id, tag})
        {:ok, make_ref()}
      end)

      on_exit(fn ->
        Application.delete_env(:flezha, :state_root)
        File.rm_rf!(base)
      end)

      %{header: TestHelpers.stub_header()}
    end

    test "no persisted state resyncs via CompactEntity pull, never an empty RestoreInitState" do
      entity_id = TestHelpers.sample_entity_id("nodata_" <> uniq())
      # The writer's lazily-created zero-byte WAL is not persisted state.
      {:ok, _} = Supervisor.find_or_start(entity_id)

      refute Restore.has_persisted_state?(entity_id)

      assert {:ok, _ref} = Recovery.resync(entity_id, runner_id: "exec-1", reason: :seq_gap)

      assert_receive {:control_sent, "exec-1", :compact_entity}, 1_000
      refute_received {:control_sent, _, :restore_init_state}
    end

    test "persisted WAL records resync via RestoreInitState push", %{header: header} do
      entity_id = TestHelpers.sample_entity_id("data_" <> uniq())
      {:ok, _} = Supervisor.find_or_start(entity_id)
      {:ok, writer} = Supervisor.lookup(entity_id)
      :ok = WalWriter.append(writer, 0, header, "x")

      assert Restore.has_persisted_state?(entity_id)

      assert {:ok, _ref} = Recovery.resync(entity_id, runner_id: "exec-1", reason: :seq_gap)

      assert_receive {:control_sent, "exec-1", :restore_init_state}, 1_000
      refute_received {:control_sent, _, :compact_entity}
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
    test "resolves the runner tag for an operative whose :operative_class opt matches" do
      operative = %Builder{id: "btc_kraken", factory_id: "f", opts: [operative_class: "market"]}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"btc_kraken" => entry(operative, "exec_123")}}
      end)

      assert Recovery.resolve_runner({"market", "0", "p1"}) == {:ok, "exec_123"}
    end

    test "falls back to the operative id when no :operative_class opt is set" do
      operative = %Builder{id: "market", factory_id: "f", opts: []}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"market" => entry(operative, "exec_9")}}
      end)

      assert Recovery.resolve_runner({"market", "0", "p1"}) == {:ok, "exec_9"}
    end

    test "errors when no registered operative owns the operative_class" do
      operative = %Builder{id: "eth", factory_id: "f", opts: [operative_class: "other"]}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"eth" => entry(operative, "exec_1")}}
      end)

      assert {:error, msg} = Recovery.resolve_runner({"market", "0", "p1"})
      assert msg =~ "no active runner owns operative_class=market"
    end

    test "tolerates a {cspec, node_pids} 2-tuple row with no metadata (no crash)" do
      operative = %Builder{id: "market", factory_id: "f", opts: []}
      spec = cspec(operative)

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
  # `{operative_id, {cspec, node_pids, metadata}}`, runner = `metadata.tag`,
  # operative_class = the operative's `:operative_class` opt (default: its id),
  # carried in the cspec's start args.
  defp cspec(operative) do
    %{start: {Flezha.Operative.Overwatch, :start_link, [{"default", operative}]}}
  end

  defp entry(operative, tag) do
    {cspec(operative), [{node(), self()}], %{tag: tag, health: :healthy}}
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
