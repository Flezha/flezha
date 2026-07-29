defmodule FlezhaTest.SickProvider do
  @moduledoc false
  # Convergence plugin-health test double: the health verdict and the
  # activation count are controlled through :persistent_term.
  def on_output(_ctx, _opts), do: :ok

  def activate(_opts) do
    :persistent_term.put({__MODULE__, :activations}, activations() + 1)
    :ok
  end

  def health(_opts), do: :persistent_term.get({__MODULE__, :health}, :healthy)

  def set_health(verdict), do: :persistent_term.put({__MODULE__, :health}, verdict)
  def activations(), do: :persistent_term.get({__MODULE__, :activations}, 0)

  def reset() do
    :persistent_term.put({__MODULE__, :health}, :healthy)
    :persistent_term.put({__MODULE__, :activations}, 0)
  end
end

defmodule FlezhaTest do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Fixtures.OperativeFixture
  alias Flezha.Service.PluginManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.OperativeTestHelper
  alias FlezhaTest.Helper.MessageTestHelper

  use ExUnit.Case
  use Flezha.Constant

  doctest Flezha

  @default_namespace "default"
  @temp_namespace "test_temp_namespace"
  @node_name NodeTestHelper.node_name()
  @testnet_ws_uri "ws://localhost:4500"

  @test_store_input_factory "store_input"
  @test_operative_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"

  # WS-dependent tests below connect to a Node.js testnet daemon at
  # `@testnet_ws_uri` (the bundle lives at `test/apps/testnet/dist/index.js`,
  # built by `mix test.setup`; the daemon is started by
  # `npm --prefix test/apps/testnet start`). The daemon is not always
  # running, so these tests are tagged `:testnet` and excluded from the
  # default `mix test` run via `test/test_helper.exs`. Run them with:
  #     mix test.setup
  #     npm --prefix test/apps/testnet start &
  #     mix test --include testnet

  setup_all do
    %{}
  end

  setup _context do
    # Restore now rehydrates a operative from its WAL on (re)bind, so leftover
    # per-Segment state from an earlier test that reused the same operative_id
    # would leak in. Start each test from empty segments.
    Flezha.State.TestHelpers.reset_all_segments()

    on_exit(fn ->
      OperativeManager.stop_all_operatives(@default_namespace)
      FleetManager.stop_namespace(@temp_namespace)
    end)

    :ok
  end

  test "runner node connect" do
    res = Flezha.Runner.FleetManager.active_runners(@default_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  @tag :operative_start
  test "operative adding" do
    operative = %Flezha.Operative.Builder{
      id: "default_operative_test",
      factory_id: @test_operative_factory
    }

    {status, info} = OperativeManager.start_operative(@default_namespace, operative)

    assert status === :ok,
           "start_operative failed: #{inspect(info)}"

    assert info[:operative_id] === "default_operative_test"
    assert info[:node] === @node_name
    assert is_pid(info[:pid])
    assert is_binary(info[:runner_id])
  end

  test "operative removing" do
    operative_id = "default_operative_stop_test"

    {:ok, %{pid: _operative_pid}} =
      operative_id
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start()

    assert :ok === OperativeManager.stop_operative(@default_namespace, operative_id)

    # TODO: add hook instead to listen to process removal.
    Process.sleep(100)

    assert OperativeManager.exist?(@default_namespace, operative_id) === false
  end

  test "operative adding non existing factory" do
    operative_id = "non_existing_factory_err"

    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: "non_existing_factory"
    }

    {status, %{operative_id: pid, node: node, reason: reason}} =
      OperativeManager.start_operative(@default_namespace, operative)

    assert status === :error
    assert pid === operative_id
    assert node === @node_name
    assert reason === "Factory with id non_existing_factory does not exist"
  end

  @tag :websocket_plugin
  @tag :testnet
  test "websocket plugin connection start" do
    operative_id = "websocket_plugin_test"

    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: @test_operative_factory
    }

    {:ok, _} = OperativeManager.start_operative(@default_namespace, operative)

    # TODO: add failures also.
    res =
      PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
        id: "websocket_plugin",
        namespace: @default_namespace,
        operative_id: operative_id,
        provider: Flezha.Plugin.Provider.Websocket,
        opts: %{
          uri: @testnet_ws_uri,
          post_connect: %{
            payload: %{
              "type" => "text",
              "message" => ~s(
                {
                  "event_type": "subscribe",
                  "event_data": {
                    "default_message": "hello world",
                    "interval": 1000
                  }
                }
              )
            }
          }
        },
        subscriptions: [{:on_terminate, []}]
      })

    assert res === {:ok, "plugin enabled"}
  end

  @tag :testnet
  test "beam msg forward plugin connection start" do
    operative_id = "beam_msg_forw_plugin_test"

    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: @test_operative_factory
    }

    {:ok, _} = OperativeManager.start_operative(@default_namespace, operative)

    res =
      PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
        id: "beam_msg_forward",
        namespace: @default_namespace,
        operative_id: operative_id,
        provider: Flezha.Plugin.Provider.BeamMsgForward,
        opts: %{recv_pid: self()},
        subscriptions: [{:on_output, []}]
      })

    assert res === {:ok, "plugin enabled"}
  end

  @tag :operative_flow_msg_forward
  @tag :testnet
  test "operative flow generation -> beam msg forward" do
    operative_id = "data_flow_test"

    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: @test_compare_filter_factory,
      init_values: %{
        "compare_filter_operation" => %{"comparable" => "hello world", "inverted" => false}
      }
    }

    {:ok, _} = OperativeManager.start_operative(@default_namespace, operative)

    PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
      id: "beam_msg_forward",
      namespace: @default_namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self()},
      subscriptions: [{:on_output, []}]
    })

    PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
      id: "websocket_plugin",
      namespace: @default_namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.Websocket,
      opts: %{
        uri: @testnet_ws_uri,
        post_connect: %{
          "payload" => %{
            "type" => "text",
            "message" => ~s(
              {
                "event_type": "subscribe",
                "event_data": {
                  "default_message": "hello world",
                  "interval": 1000
                }
              }
          )
          }
        }
      },
      subscriptions: [{:on_terminate, []}]
    })

    for _ <- 1..1 do
      receive do
        {:message_forward, msg} ->
          assert msg === "hello world"
      after
        2000 -> assert false
      end
    end
  end

  @tag :get_segment_state
  test "get operative state" do
    operative_id = "get_state_test"

    {:ok, %{pid: _operative_pid}} =
      operative_id
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start()
      |> OperativeFixture.attach_output_msg()

    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "state_1", %{})
    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "state_2", %{})

    {:ok, json_state} =
      OperativeManager.get_segment_state(@default_namespace, operative_id)

    segment_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert segment_state === %{
             "store_input_operation" => %{
               "binary_inputs" => [],
               "text_inputs" => ["state_1", "state_2"]
             }
           }
  end

  # # # # TODO: add test to handle large control requests and stream requests.

  @tag :operative_migration
  test "operative migration" do
    # TODO: we should spawn a new elixir node, wait till the process gets migrated there(using dynamic hooks) and
    # then get the state of the new migrated process to see if it matches the state1 string.

    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    operative_id = "operative_migration_test"

    {:ok, %{pid: _operative_pid, node: pnode}} =
      operative_id
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start()
      |> OperativeFixture.attach_output_msg()

    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "test_state_1", %{})
    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "test_state_2", %{})

    test_state = %{
      "store_input_operation" => %{
        "binary_inputs" => [],
        "text_inputs" => ["test_state_1", "test_state_2"]
      }
    }

    PluginManager.enable_plugin("stdout_plugin", %Flezha.Plugin{
      id: "stdout_plugin",
      namespace: @default_namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.StdOut,
      opts: %{},
      subscriptions: [{:on_output, []}]
    })

    {:ok, json_state} =
      OperativeManager.get_segment_state(@default_namespace, operative_id)

    {:ok, old_overwatch_state} =
      OperativeManager.get_overwatch_state(@default_namespace, operative_id)

    segment_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert segment_state === test_state

    test_pid = self()

    ProcessHub.register_hook_handlers(
      Identificator.hub(@default_namespace),
      ProcessHub.Constant.Hook.post_children_start(),
      [
        %ProcessHub.Service.HookManager{
          id: "migration_wait",
          m: Process,
          f: :send,
          a: [test_pid, :migration_complete, []]
        }
      ]
    )

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)

    if pnode == peer_node do
      receive do
        :migration_complete -> :ok
      after
        5000 -> flunk("Migration did not complete within 5 seconds")
      end
    end

    ProcessHub.cancel_hook_handlers(
      Identificator.hub(@default_namespace),
      ProcessHub.Constant.Hook.post_children_start(),
      ["migration_wait"]
    )

    {:ok, json_state} =
      OperativeManager.get_segment_state(@default_namespace, operative_id)

    {:ok, new_overwatch_state} =
      OperativeManager.get_overwatch_state(@default_namespace, operative_id)

    segment_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert segment_state === test_state

    old_plugins = Map.keys(old_overwatch_state.extensions.plugins) |> Enum.sort()
    new_plugins = Map.keys(new_overwatch_state.extensions.plugins) |> Enum.sort()
    assert new_plugins === old_plugins
    assert new_overwatch_state.init_values === old_overwatch_state.init_values
  end

  @tag :drain_migration
  test "graceful shutdown drains an operative to a peer through live handover" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_node1)

    operative_id = start_operative_on_peer(peer_node, "drain_mig")
    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "before_drain", %{})

    assert :ok = shutdown_peer(peer_node, 30_000)

    assert_receive {[:flezha, :shutdown, :drain_progress], _ref, m, _meta}, 10_000
    assert m.migrated >= 1
    assert m.forced == 0

    {:ok, ow_state} = OperativeManager.get_overwatch_state(@default_namespace, operative_id)

    assert ow_state.arrived_via_hotswap?,
           "expected live HotSwap handover, not a WAL cold restore"

    {:ok, json_state} = OperativeManager.get_segment_state(@default_namespace, operative_id)
    state = Flezha.Service.Payloader.json_to_map(json_state)
    assert "before_drain" in get_in(state, ["store_input_operation", "text_inputs"])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  @tag :drain_migration_forced
  test "a deferring operative is force-migrated at the drain deadline without wedging" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:drain_node2)

    operative_id = start_operative_on_peer(peer_node, "drain_forced")
    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "seed", %{})

    # Hold the busy signal: a correction that never leaves its debounce
    # window, so `migration_consent/1` answers :defer for the whole drain.
    :ok =
      :erpc.call(peer_node, Application, :put_env, [:flezha, :correction_debounce_ms, 600_000])

    hub_id = Identificator.hub(@default_namespace)

    {^operative_id, ow_pid} =
      :erpc.call(peer_node, ProcessHub, :process_list, [hub_id, :local])
      |> Enum.find(fn {id, _pid} -> id == operative_id end)

    GenServer.cast(
      ow_pid,
      {:correction_request, {operative_id, "store_input_operation", "poke"}, 1}
    )

    # Sync barrier: the cast is folded into state before the drain begins.
    _ = :sys.get_state(ow_pid)

    test_pid = self()

    :erpc.call(peer_node, ProcessHub, :register_hook_handlers, [
      hub_id,
      ProcessHub.Constant.Hook.migration_deferred(),
      [
        %ProcessHub.Service.HookManager{
          id: "deferred_wait",
          m: Process,
          f: :send,
          a: [test_pid, :deferred, []]
        }
      ]
    ])

    assert :ok = shutdown_peer(peer_node, 6_000)

    assert_receive :deferred, 1_000
    assert_receive {[:flezha, :shutdown, :drain_progress], _ref, m, _meta}, 5_000
    assert m.forced >= 1

    # Forced onto this node through the best-effort handover: state carried,
    # protocol still live.
    {:ok, ow_state} = OperativeManager.get_overwatch_state(@default_namespace, operative_id)
    assert ow_state.arrived_via_hotswap?

    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "after_force", %{})
    {:ok, json_state} = OperativeManager.get_segment_state(@default_namespace, operative_id)
    state = Flezha.Service.Payloader.json_to_map(json_state)
    assert "seed" in get_in(state, ["store_input_operation", "text_inputs"])
    assert "after_force" in get_in(state, ["store_input_operation", "text_inputs"])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # Attach the peer's drain telemetry (relayed here as messages), then drive
  # its graceful shutdown to completion.
  defp shutdown_peer(peer_node, timeout_ms) do
    :erpc.call(peer_node, :telemetry_test, :attach_event_handlers, [
      self(),
      [[:flezha, :shutdown, :drain_progress]]
    ])

    :erpc.call(
      peer_node,
      Flezha.Shutdown.Coordinator,
      :shutdown,
      [:cli, [timeout_ms: timeout_ms]],
      timeout_ms + 15_000
    )
  end

  # The consistent-hash ring decides placement by id, so probe candidate ids
  # until one lands on the peer; the rejects are stopped again. Placement can
  # only reach the peer after it has joined the hub cluster, hence the wait.
  defp start_operative_on_peer(peer_node, prefix) do
    NodeTestHelper.wait_until(
      fn -> peer_node in HubManager.get_hub_nodes(@default_namespace, false) end,
      10_000
    ) || flunk("peer #{inspect(peer_node)} never joined the hub cluster")

    Enum.reduce_while(1..20, nil, fn i, _acc ->
      id = "#{prefix}_#{i}"

      {:ok, %{node: pnode}} =
        id
        |> OperativeFixture.get(@test_store_input_factory)
        |> OperativeFixture.start()
        |> OperativeFixture.attach_output_msg()

      if pnode == peer_node do
        {:halt, id}
      else
        OperativeFixture.stop(id)
        {:cont, nil}
      end
    end) || flunk("no operative landed on #{inspect(peer_node)} in 20 attempts")
  end

  @tag :runner_disconnect
  test "runner disconnect" do
    namespace = @temp_namespace
    runner_id = NodeTestHelper.start_runner(namespace)

    NodeTestHelper.stop_runner(namespace, runner_id)

    assert Flezha.Runner.FleetManager.active_runners(namespace) === []
  end

  @tag :runner_disconnect_migrate_op
  test "runner disconnect migrate op" do
    namespace = @temp_namespace
    operative_id_1 = "operative_1"
    operative_id_2 = "operative_2"

    # Start 2 runners under the same namespace.
    runner_id_1 = NodeTestHelper.start_runner(namespace)
    runner_id_2 = NodeTestHelper.start_runner(namespace)

    # Start a few operatives and store some state on them.
    {:ok, %{pid: _operative_pid_1, runner_id: pipe1_op}} =
      operative_id_1
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start(namespace)
      |> OperativeFixture.attach_output_msg(namespace)

    {:ok, %{pid: _operative_pid_2, runner_id: _pipe2_op}} =
      operative_id_2
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start(namespace)
      |> OperativeFixture.attach_output_msg(namespace)

    OperativeTestHelper.send_sync_io(operative_id_1, namespace, "state_1", %{})
    OperativeTestHelper.send_sync_io(operative_id_2, namespace, "state_2", %{})

    OperativeManager.create_snapshot(namespace, operative_id_1)
    OperativeManager.create_snapshot(namespace, operative_id_2)

    active_runner =
      if pipe1_op === runner_id_1, do: runner_id_2, else: runner_id_1

    # Trigger the runner disconnect.
    NodeTestHelper.stop_runner(namespace, pipe1_op)

    # Make sure all the operatives are migrated to the other runner.
    OperativeManager.operative_registry(namespace)
    |> Enum.each(fn {_operative_id, {_cspec, _cnodes, %{tag: new_runner}}} ->
      assert new_runner === active_runner
    end)

    # Make sure the states have been migrated.
    {:ok, json_state_1} = OperativeManager.get_segment_state(namespace, operative_id_1)
    {:ok, json_state_2} = OperativeManager.get_segment_state(namespace, operative_id_2)

    assert json_state_1 ===
             "{\"store_input_operation\":{\"binary_inputs\":[],\"text_inputs\":[\"state_1\"]}}"

    assert json_state_2 ===
             "{\"store_input_operation\":{\"binary_inputs\":[],\"text_inputs\":[\"state_2\"]}}"
  end

  @tag :runner_disconnect_preserve
  test "runner disconnect preserve" do
    namespace = @temp_namespace
    num_operatives = 15
    operative_ids = for i <- 1..num_operatives, do: "operative_#{i}"

    # Fresh-process runner takeover (an :inactive peer's bindings
    # are migrated onto the freshly-connected runner) is driven by
    # `Flezha.Convergence.Loop`, which is OFF in the default test config
    # to avoid global-tick races. Start it locally for this test, gated
    # on `:runner_connected_event` so the rebind fires within ~ms of
    # `runner_id_3` connecting.
    # The Loop's recovery gate (`Probe.await_recovery_complete/2`) requires
    # every hub the always-on Snapshot has ever observed to be settled. That
    # cache accumulates across test files and is never cleared, so a hub left
    # mid-recovery by an earlier test would block this Loop's first tick
    # forever. Reset it so the gate only weighs this test's hubs.
    Flezha.Convergence.Snapshot.reset_recovery_states()

    # Short periodic tick so the reseat reconciles within the assertion
    # budget even if the single `:runner_connected_event` is missed under
    # in-suite contention (other tests also drive the global EventManager).
    {:ok, _} =
      start_supervised({Flezha.Convergence.Loop, tick_ms: 1_000})

    runner_id_1 = NodeTestHelper.start_runner(namespace)
    runner_id_2 = NodeTestHelper.start_runner(namespace)

    # Start operatives and store some state on them.

    operative_infos =
      for operative_id <- operative_ids do
        {:ok, info} =
          operative_id
          |> OperativeFixture.get(@test_store_input_factory)
          |> OperativeFixture.start(namespace)
          |> OperativeFixture.attach_output_msg(namespace)

        {operative_id, info}
      end

    # Send state to all operatives
    for {operative_id, _info} <- operative_infos do
      OperativeTestHelper.send_sync_io(operative_id, namespace, "state_#{operative_id}", %{})
    end

    # Create snapshots for all operatives
    for {operative_id, _info} <- operative_infos do
      OperativeManager.create_snapshot(namespace, operative_id)
    end

    # Trigger the runner disconnect.
    NodeTestHelper.stop_runner(namespace, runner_id_1)
    NodeTestHelper.stop_runner(namespace, runner_id_2)

    # Start a new runner under the same namespace.
    runner_id_3 = NodeTestHelper.start_runner(namespace)

    # The convergence Loop reseats every operative tagged to a dead runner
    # onto `runner_id_3`. In production a periodic tick + the
    # `:runner_connected_event` drive this; in-suite, async tick delivery
    # and the global recovery gate are non-deterministic, so drive the real
    # reconcile path synchronously via `tick_now/0` on each poll and wait
    # for the reseat to land.
    assert eventually(
             fn ->
               Flezha.Convergence.Loop.tick_now()

               OperativeManager.operative_registry(namespace)
               |> Enum.all?(fn {_operative_id, {_cspec, _cnodes, %{tag: tag}}} ->
                 tag === runner_id_3
               end)
             end,
             15_000
           ),
           "operatives did not reseat to runner_id_3 within timeout"

    # The rebind tag flips before the WAL restore finishes replaying the
    # change-log onto the reseated runner, so poll the migrated state
    # rather than reading it the instant the tag lands.
    expected_state = fn operative_id ->
      "{\"store_input_operation\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{operative_id}\"]}}"
    end

    # The convergence Loop dispatches the WAL restore once, inside the rebind
    # heal — but once the tag is `runner_id_3` the operative is no longer
    # misbound, so a later tick won't re-dispatch. Re-drive the documented,
    # idempotent restore re-dispatch path per operative each poll so a restore
    # that raced the new runner's readiness retries until state converges.
    assert eventually(
             fn ->
               for {operative_id, _info} <- operative_infos do
                 case HubManager.get_pid(namespace, operative_id) do
                   {:ok, pid} when is_pid(pid) ->
                     Flezha.Operative.Overwatch.kick_restore_for_bound_runner(
                       pid,
                       runner_id_3
                     )

                   _ ->
                     :ok
                 end
               end

               Enum.all?(operative_infos, fn {operative_id, _info} ->
                 OperativeManager.get_segment_state(namespace, operative_id) ===
                   {:ok, expected_state.(operative_id)}
               end)
             end,
             15_000
           ),
           "operative states did not migrate to runner_id_3 within timeout"

    # Stop the new runner.
    NodeTestHelper.stop_runner(namespace, runner_id_3)

    # Adjust the runner cleanup interval and trigger the cleanup.
    Application.put_env(:flezha, :runner_keepalive, "0")
    GenServer.call(FleetManager, :runner_cleanup)

    # Reset the runner keepalive to its default value.
    Application.put_env(:flezha, :runner_keepalive, "30000")

    # Cleanup reaps the dead runner ref but must NOT tear the hub down while it
    # still holds the 15 registered operatives — they stay addressable and
    # reseat onto the next runner (bounded-operative.md, hub-outlives-runner).
    assert HubManager.registered_operatives?(namespace)
    assert HubManager.hub_alive?(namespace) === true
  end

  @tag :runner_disconnect_migrate_hub
  test "runner disconnect migrate hub" do
    namespace = @temp_namespace
    num_operatives = 15
    operative_ids = for i <- 1..num_operatives, do: "operative_#{i}"

    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    # Start the runners.
    runner_id_1 = NodeTestHelper.start_runner(namespace)
    rem_runner_1 = NodeTestHelper.start_runner_remote(peer_node, namespace)
    rem_runner_2 = NodeTestHelper.start_runner_remote(peer_node, namespace)

    # Wait for peer node to join the ProcessHub hub cluster
    hub_id = Identificator.hub(namespace)
    test_pid = self()

    ProcessHub.register_hook_handlers(
      hub_id,
      ProcessHub.Constant.Hook.post_node_join(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_node_join_wait",
          m: Process,
          f: :send,
          a: [test_pid, :peer_node_joined, []]
        }
      ]
    )

    if length(HubManager.get_hub_nodes(namespace, true)) < 2 do
      receive do
        :peer_node_joined -> :ok
      after
        5_000 -> flunk("Peer node did not join the hub cluster within 5 seconds")
      end
    end

    ProcessHub.cancel_hook_handlers(hub_id, ProcessHub.Constant.Hook.post_node_join(), [
      "hub_node_join_wait"
    ])

    self = self()
    _node = node()

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      ProcessHub.Constant.Hook.child_registered(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_insert_hook_test",
          m: Process,
          f: :send,
          a: [self, :hook_insert_recv, []]
        }
      ]
    )

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      ProcessHub.Constant.Hook.post_children_start(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_pstartup_hook_test",
          m: Process,
          f: :send,
          a: [self, :hook_pstartup_recv, []]
        }
      ]
    )

    # Start operatives and store some state on them.
    operative_infos =
      for operative_id <- operative_ids do
        {:ok, info} =
          operative_id
          |> OperativeFixture.get(@test_store_input_factory)
          |> OperativeFixture.start(namespace)
          |> OperativeFixture.attach_output_msg(namespace)

        {operative_id, info}
      end

    runner_ids =
      Enum.map(operative_infos, fn {_id, %{runner_id: ce_id}} -> ce_id end)

    # Make sure that atleast one of the operatives is running on the remote runner.
    assert Enum.any?(runner_ids, &(&1 !== runner_id_1))

    # Send state to all operatives
    for {operative_id, _info} <- operative_infos do
      OperativeTestHelper.send_sync_io(operative_id, namespace, "state_#{operative_id}", %{})
    end

    # Check the states before runner migrations.
    for {operative_id, _info} <- operative_infos do
      {:ok, json_state} = OperativeManager.get_segment_state(namespace, operative_id)

      assert json_state ===
               "{\"store_input_operation\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{operative_id}\"]}}"
    end

    # Create snapshots for all operatives
    for {operative_id, _info} <- operative_infos do
      OperativeManager.create_snapshot(namespace, operative_id)
    end

    # Trigger the runner disconnect.
    NodeTestHelper.stop_runner_remote(peer_node, namespace, rem_runner_1)
    NodeTestHelper.stop_runner_remote(peer_node, namespace, rem_runner_2)

    # Wait for cross-node ProcessHub sync via hook messages (pure message passing).
    # Each :hook_insert_recv arrives when the local registry syncs a tag update.
    # We check the condition after each message — no polling, no sleeping.
    MessageTestHelper.recv_until(:hook_insert_recv, fn ->
      OperativeManager.operative_registry(namespace)
      |> Enum.all?(fn {_, {_, _, %{tag: tag}}} -> tag === runner_id_1 end)
    end)

    MessageTestHelper.flush(:hook_pstartup_recv)

    # The tag converges when the registry row moves; the destination Overwatch
    # still has to finish binding and take the handover before it can answer for
    # its Segments. Poll that out — migrated-with-state is an eventual property,
    # and reading it at the first converged tag races the bind.
    assert eventually(
             fn ->
               Enum.all?(operative_infos, fn {operative_id, _info} ->
                 OperativeManager.get_segment_state(namespace, operative_id) ===
                   {:ok,
                    "{\"store_input_operation\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{operative_id}\"]}}"}
               end)
             end,
             15_000
           ),
           "operative states did not survive the cross-controller migration"

    assert length(HubManager.get_hub_nodes(namespace, true)) === 1

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  test "pluginless running operative re-attaches its declared plugins (2026-06-12 incident shape)" do
    namespace = @temp_namespace
    Flezha.Convergence.Snapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Flezha.Convergence.Loop, tick_ms: 60_000})

    runner_id = NodeTestHelper.start_runner(namespace)

    {:ok, %{operative_id: operative_id}} =
      "self_heal_operative"
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start(namespace)
      |> OperativeFixture.attach_output_msg(namespace)

    plugin_id = operative_id <> "_output_msg_handler"
    {:ok, ow} = HubManager.get_pid(namespace, operative_id)
    assert subscribed_plugin?(ow, plugin_id)

    # Direct Overwatch unsubscribe bypasses PluginManager, so the declared
    # cspec manifest keeps the plugin: running-but-pluginless, exactly the
    # warm-restart incident state.
    :ok = GenServer.call(ow, {:plugin_unsubscribe, plugin_id})
    refute subscribed_plugin?(ow, plugin_id)

    assert eventually(
             fn ->
               Flezha.Convergence.Loop.tick_now()
               subscribed_plugin?(ow, plugin_id)
             end,
             15_000
           ),
           "loop did not re-attach the declared plugin"

    NodeTestHelper.stop_runner(namespace, runner_id)
  end

  test "declared plugin self-reporting unhealthy is restarted by the loop" do
    namespace = @temp_namespace
    Flezha.Convergence.Snapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Flezha.Convergence.Loop, tick_ms: 60_000})

    runner_id = NodeTestHelper.start_runner(namespace)

    {:ok, %{operative_id: operative_id}} =
      "sick_plugin_operative"
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start(namespace)

    plugin_id = operative_id <> "_sick"
    FlezhaTest.SickProvider.reset()

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(plugin_id, %Flezha.Plugin{
        id: plugin_id,
        namespace: namespace,
        operative_id: operative_id,
        provider: FlezhaTest.SickProvider,
        opts: %{},
        subscriptions: [{:on_output, []}]
      })

    {:ok, ow} = HubManager.get_pid(namespace, operative_id)
    assert subscribed_plugin?(ow, plugin_id)
    activations_before = FlezhaTest.SickProvider.activations()

    # Healthy plugin: a tick must not restart it.
    Flezha.Convergence.Loop.tick_now()
    assert FlezhaTest.SickProvider.activations() == activations_before

    FlezhaTest.SickProvider.set_health({:unhealthy, :test_sick})

    assert eventually(
             fn ->
               Flezha.Convergence.Loop.tick_now()
               FlezhaTest.SickProvider.activations() > activations_before
             end,
             15_000
           ),
           "loop did not restart the unhealthy plugin"

    assert subscribed_plugin?(ow, plugin_id)

    # Verdict cleared: no further restarts once healthy again.
    FlezhaTest.SickProvider.set_health(:healthy)
    Flezha.Convergence.Loop.tick_now()
    stable = FlezhaTest.SickProvider.activations()
    Flezha.Convergence.Loop.tick_now()
    assert FlezhaTest.SickProvider.activations() == stable

    NodeTestHelper.stop_runner(namespace, runner_id)
  end

  defp subscribed_plugin?(ow, plugin_id) do
    case GenServer.call(ow, :get_overwatch_state) do
      %{extensions: %{plugins: plugins}} -> Map.has_key?(plugins, plugin_id)
      _ -> false
    end
  end

  defp eventually(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    do_eventually = fn do_self, fun, deadline ->
      cond do
        fun.() ->
          true

        System.monotonic_time(:millisecond) >= deadline ->
          false

        true ->
          Process.sleep(25)
          do_self.(do_self, fun, deadline)
      end
    end

    do_eventually.(do_eventually, fun, deadline)
  end
end
