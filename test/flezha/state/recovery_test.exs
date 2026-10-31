defmodule Flezha.State.RecoveryTest do
  @moduledoc """
  `Flezha.State.Recovery`: a desynced Cell is put back on a consistent point
  by pulling the live Cell's state, and the runner it is pulled from.
  """

  # async: false: writes Application env; Mimic stubs; a shared on-disk root.
  use FlezhaTest.Case, async: false
  use Mimic

  import Flezha.State.TestHelpers, only: [uniq: 0]

  alias Flezha.Relay.Builder
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

    # fails when: a resync with state on disk pushes `RestoreInitState`,
    # rewinding a live Cell to a copy behind it.
    test "every resync pulls, with or without persisted state", %{header: header} do
      bare = TestHelpers.sample_cell_id("nodata_" <> uniq())
      {:ok, _} = Supervisor.find_or_start(bare)
      refute Restore.has_persisted_state?(bare)

      held = TestHelpers.sample_cell_id("data_" <> uniq())
      {:ok, writer} = Supervisor.find_or_start(held)
      :ok = WalWriter.append(writer, 0, header, "x")
      assert Restore.has_persisted_state?(held)

      for cell_id <- [bare, held],
          reason <- [:seq_gap, :shed, :baseline_seed, :unsynced, :compaction_gave_up] do
        assert {:ok, _ref} = Recovery.resync(cell_id, runner_id: "exec-1", reason: reason)
        assert_receive {:control_sent, "exec-1", :compact_cell}, 1_000
      end

      refute_received {:control_sent, _, :restore_init_state}
    end
  end

  describe "resolve_runner/1" do
    setup do
      stub(HubManager, :get_running_namespaces, fn -> ["default"] end)
      :ok
    end

    test "resolves the runner tag for a relay whose :relay_class opt matches" do
      relay = %Builder{id: "btc_feed", factory_id: "f", opts: [relay_class: "market"]}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"btc_feed" => entry(relay, "exec_123")}}
      end)

      assert Recovery.resolve_runner({"market", "0", "p1"}) == {:ok, "exec_123"}
    end

    test "falls back to the relay id when no :relay_class opt is set" do
      relay = %Builder{id: "market", factory_id: "f", opts: []}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"market" => entry(relay, "exec_9")}}
      end)

      assert Recovery.resolve_runner({"market", "0", "p1"}) == {:ok, "exec_9"}
    end

    test "errors when no registered relay owns the relay_class" do
      relay = %Builder{id: "eth", factory_id: "f", opts: [relay_class: "other"]}

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"eth" => entry(relay, "exec_1")}}
      end)

      assert {:error, {:no_owning_runner, "market"}} =
               Recovery.resolve_runner({"market", "0", "p1"})
    end

    test "tolerates a {cspec, node_pids} 2-tuple row with no metadata (no crash)" do
      relay = %Builder{id: "market", factory_id: "f", opts: []}
      spec = cspec(relay)

      stub(HubManager, :get_registry, fn "default" ->
        {:ok, %{"market" => {spec, [{node(), self()}]}}}
      end)

      # No metadata -> no tag -> not a match, and the 2-tuple shape must not crash.
      assert {:error, _} = Recovery.resolve_runner({"market", "0", "p1"})
    end

    test "resolves the runner of a relay in a namespace other than default" do
      # fails when: only the default namespace's registry is read, so a relay
      # bound in any other namespace answers that no runner owns it.
      relay = %Builder{id: "bob", factory_id: "f", opts: [relay_class: "desk"]}

      stub(HubManager, :get_running_namespaces, fn -> ["default", "team-b"] end)

      stub(HubManager, :get_registry, fn
        "default" -> {:ok, %{}}
        "team-b" -> {:ok, %{"bob" => entry(relay, "exec_b", "team-b")}}
      end)

      assert Recovery.resolve_runner({"desk", "0", "p1"}) == {:ok, "exec_b"}
    end

    test "a namespace that stops after the listing binds no runner and fails no other" do
      # fails when: one namespace torn down between the two reads fails the
      # whole answer, so recovery resolves no runner in any namespace.
      relay = %Builder{id: "bob", factory_id: "f", opts: [relay_class: "desk"]}

      stub(HubManager, :get_running_namespaces, fn -> ["gone", "team-b"] end)

      stub(HubManager, :get_registry, fn
        "gone" -> {:error, {:hub_down, "gone"}}
        "team-b" -> {:ok, %{"bob" => entry(relay, "exec_b", "team-b")}}
      end)

      assert Recovery.resolve_runner({"desk", "0", "p1"}) == {:ok, "exec_b"}
    end

    test "surfaces a registry error" do
      stub(HubManager, :get_registry, fn "default" -> {:error, "hub not alive"} end)

      assert Recovery.resolve_runner({"market", "0", "p1"}) == {:error, "hub not alive"}
    end
  end

  # The durable-registry entry shape `resolve_runner/1` matches:
  # `{relay_id, {cspec, node_pids, metadata}}`, runner = `metadata.tag`,
  # relay_class = the relay's `:relay_class` opt (default: its id),
  # carried in the cspec's start args.
  defp cspec(relay, namespace \\ "default") do
    %{start: {Flezha.Relay.Overwatch, :start_link, [{namespace, relay}]}}
  end

  defp entry(relay, tag, namespace \\ "default") do
    {cspec(relay, namespace), [{node(), self()}], %{tag: tag, health: :healthy}}
  end
end
