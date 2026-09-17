defmodule Flezha.SDK.CommandsTest do
  @moduledoc """
  The persistence-family commands and the credit window, driven the way the
  RunnerServer drives them — through `Commands.dispatch/4` over a server state
  built by hand — and the shutdown sequence over a native stream pair whose
  controller side the test plays. No controller, no real transport.
  """
  use ExUnit.Case, async: true

  import FlezhaTest.Helper.SdkOperativeHarness

  alias Flezha.SDK.{Codec, Commands, Counters, Credit, Entity, Hold, Park, Record}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.SDK.RunnerServer.State, as: ServerState
  alias Flezha.SDK.Wire
  alias FlezhaTest.Helper.NodeTestHelper

  alias FlezhaTest.Helper.SdkPersistenceFixtures.{
    CounterOp,
    CounterV2Op,
    DynCounterOp,
    StaticCounterOp,
    TtlOp
  }

  defp credit(counters \\ Counters.new()) do
    {:ok, pid} = Credit.start_link(counters: counters)
    {pid, counters}
  end

  defp server_state(op, credit),
    do: %ServerState{operatives: %{"op-1" => {op, "f"}}, credit: credit}

  defp respond do
    me = self()
    fn status, errors, raw, meta -> send(me, {:replied, status, errors, raw, meta}) end
  end

  describe "the credit window" do
    test "no session never waits; a session debits, parks in order, resumes on a covering grant, drops a stale one" do
      {credit, counters} = credit()
      assert :ok = Credit.acquire(credit, 10)
      Credit.arm(credit, "s1", 100)
      assert :ok = Credit.acquire(credit, 60)

      parked = Task.async(fn -> Credit.acquire(credit, 60) end)
      assert Task.yield(parked, 50) == nil
      Credit.grant(credit, "stale", 1_000)
      assert Task.yield(parked, 50) == nil
      # 50 available: the head still does not fit, one more iteration.
      Credit.grant(credit, "s1", 10)
      assert Task.yield(parked, 50) == nil
      Credit.grant(credit, "s1", 10)
      assert :ok = Task.await(parked, 1_000)
      assert Counters.credit_parked(counters) == 2
      assert Credit.session(credit) == "s1"
    end

    test "a disarm releases parked senders un-metered; no window, or a window that is gone, returns at once" do
      {credit, _counters} = credit()
      Credit.arm(credit, "s1", 1)
      parked = Task.async(fn -> Credit.acquire(credit, 5) end)
      assert Task.yield(parked, 50) == nil
      Credit.disarm(credit)
      assert :ok = Task.await(parked, 1_000)
      assert :ok = Credit.acquire(nil, 5)

      {gone, _} = credit()
      GenServer.stop(gone)
      assert :ok = Credit.acquire(gone, 5)
    end

    test "an Entity parks on the window and its record leaves on the grant; the heartbeat counter moves" do
      {credit, counters} = credit()
      Credit.arm(credit, "s1", 1)
      %{op: op} = start_operative(CounterOp, credit: credit)
      Inbox.input(op, "in", "1", %{})
      assert NodeTestHelper.wait_until(fn -> Counters.credit_parked(counters) == 1 end, 2_000)
      refute_received {:flz_data, _, "entity_state_change", _, _}

      Credit.grant(credit, "s1", 10_000)
      assert [{"entity_state_change", _, _}, {"op-1", "1", _}] = frames(2)
    end
  end

  describe "the commands" do
    test "the live read answers the Decision-10 shape with the seq of one Entity, and refuses by name" do
      %{op: op} = start_operative(CounterOp)
      Inbox.input(op, "in", "3", %{})
      [_, _] = frames(2)
      s = server_state(op, nil)

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-1", procedure: "", entity_key: ""},
        respond(),
        s
      )

      assert_receive {:replied, "success", [], json, %{"flz.change_seq" => "1"}}, 2_000
      assert Jason.decode!(json) == %{"counter" => %{"value" => 3}}

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-1", procedure: "", entity_key: "k"},
        respond(),
        s
      )

      assert_receive {:replied, "error", ["entity_key_without_procedure: " <> _], <<>>, %{}},
                     2_000

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-1", procedure: "nope", entity_key: ""},
        respond(),
        s
      )

      assert_receive {:replied, "error", ["procedure_not_found: " <> _], <<>>, %{}}, 2_000

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-1", procedure: "counter", entity_key: "k"},
        respond(),
        s
      )

      assert_receive {:replied, "error", ["entity_not_found: " <> _], <<>>, %{}}, 2_000

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-9", procedure: "", entity_key: ""},
        respond(),
        s
      )

      assert_receive {:replied, "error", ["StreamOperative instance with id op-9 not found"],
                      <<>>, %{}},
                     2_000
    end

    test "a keyed node reads as a map by key, narrowed by the selector, with the seq only for one Entity" do
      %{op: op} = start_operative(DynCounterOp)
      for input <- ~w(1 2 3), do: Inbox.input(op, "in", input, %{})
      [_, _, _, _, _, _] = frames(6)
      s = server_state(op, nil)

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-1", procedure: "", entity_key: ""},
        respond(),
        s
      )

      assert_receive {:replied, "success", [], json, %{}}, 2_000

      assert Jason.decode!(json) == %{
               "counter" => %{"k0" => %{"value" => 2}, "k1" => %{"value" => 4}}
             }

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-1", procedure: "counter", entity_key: "k1"},
        respond(),
        s
      )

      assert_receive {:replied, "success", [], json, %{"flz.change_seq" => "2"}}, 2_000
      assert Jason.decode!(json) == %{"counter" => %{"k1" => %{"value" => 4}}}

      Commands.dispatch(:migration_serialization, %{operative_id: "op-1"}, respond(), s)
      assert_receive {:replied, "success", [], json, %{}}, 2_000
      assert Jason.decode!(json)["counter"]["k0"] == %{"value" => 2}
    end

    test "a restore that migrated answers migrated, and the compaction behind it snapshots at the running version" do
      %{op: op} = start_operative(CounterV2Op)
      s = server_state(op, nil)
      identity = Park.identity("op-1", "counter", "")
      snapshot = Record.encode_body(%{value: 4})

      Commands.dispatch(
        :restore_init_state,
        %{
          entity_id: identity,
          snapshot_header: Record.header(snapshot, 1),
          snapshot_bytes: snapshot,
          change_log_bytes: change_record(2, {:delta, 3}, 1),
          terminal_seq: 2
        },
        respond(),
        s
      )

      assert_receive {:replied, "success", [], raw, %{}}, 2_000

      assert {:ok, %{terminal_seq: 2, migrated: true}} =
               RunnerCommands.RestoreInitStateResponse.decode(raw)

      Commands.dispatch(:compact_entity, %{entity_id: identity}, respond(), s)
      assert_receive {:flz_data, _, "entity_state_snapshot", frame, _}, 2_000

      assert {:ok, %{seq: 2, header: <<3, _::32, 2::32-big, _::binary>>, body: body}} =
               Wire.decode_frame(frame)

      assert {:ok, %{value: 7, unit: "n"}} = Record.decode_body(body)
      assert_receive {:replied, "success", [], _raw, %{}}, 2_000
    end

    test "the live read of an evicted key names eviction, and of a never-seen key names absence" do
      %{op: op} = start_operative(TtlOp)
      Inbox.input(op, "in", "5", %{})
      [_, _] = frames(2)
      assert_receive {:flz_data, _, "delete_entity_state", _, _}, 2_000
      s = server_state(op, nil)

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-1", procedure: "ttl", entity_key: "k5"},
        respond(),
        s
      )

      assert_receive {:replied, "error", [reason], <<>>, %{}}, 2_000

      assert reason ==
               "entity_evicted: Entity `k5` of procedure `ttl` was idle-evicted; eviction is " <>
                 "destructive, so there is no durable value to serve"

      Commands.dispatch(
        :entity_state,
        %{operative_id: "op-1", procedure: "ttl", entity_key: "zz"},
        respond(),
        s
      )

      assert_receive {:replied, "error", ["entity_not_found: " <> _], <<>>, %{}}, 2_000
    end

    test "restore and compaction answer through the commands with their protobuf replies" do
      %{op: op} = start_operative(CounterOp)
      s = server_state(op, nil)
      id = Park.identity("op-1", "counter", "")
      snap = Record.encode_body(%{value: 10})

      cmd = %{
        entity_id: id,
        snapshot_header: Record.header(snap, 1),
        snapshot_bytes: snap,
        change_log_bytes: change_record(4, {:delta, 5}),
        terminal_seq: 3
      }

      Commands.dispatch(:restore_init_state, cmd, respond(), s)
      assert_receive {:replied, "success", [], raw, %{}}, 2_000

      assert {:ok,
              %RunnerCommands.RestoreInitStateResponse{
                entity_id: ^id,
                terminal_seq: 4,
                migrated: false
              }} =
               RunnerCommands.RestoreInitStateResponse.decode(raw)

      # a refused fold answers its reason and the zero outcome
      bad = %{
        cmd
        | snapshot_bytes: <<>>,
          snapshot_header: <<>>,
          change_log_bytes: change_record(9, {:delta, 1}, 7)
      }

      Commands.dispatch(:restore_init_state, bad, respond(), s)

      assert_receive {:replied, "error",
                      ["restore_init_state: record stamped schema_version 7" <> _], raw, %{}},
                     2_000

      assert {:ok, %{terminal_seq: 0}} = RunnerCommands.RestoreInitStateResponse.decode(raw)

      # no live Entity: acked at the controller's own terminal
      Commands.dispatch(
        :restore_init_state,
        %{cmd | entity_id: Park.identity("op-9", "counter", "")},
        respond(),
        s
      )

      assert_receive {:replied, "success", [], raw, %{}}, 2_000
      assert {:ok, %{terminal_seq: 3}} = RunnerCommands.RestoreInitStateResponse.decode(raw)

      Commands.dispatch(:compact_entity, %{entity_id: id}, respond(), s)
      assert_receive {:replied, "success", [], raw, %{}}, 2_000

      assert {:ok,
              %RunnerCommands.CompactEntityResponse{snapshot_seq: 4, snapshot_bytes_size: size}} =
               RunnerCommands.CompactEntityResponse.decode(raw)

      assert [{"entity_state_snapshot", frame, _}] = frames(1)
      assert {:ok, %{seq: 4, body: body}} = Wire.decode_frame(frame)
      assert byte_size(body) == size

      Commands.dispatch(
        :compact_entity,
        %{entity_id: Park.identity("op-9", "counter", "")},
        respond(),
        s
      )

      assert_receive {:replied, "error", ["compact_entity: " <> _], raw, %{}}, 2_000

      assert {:ok, %{snapshot_seq: 0, snapshot_bytes_size: 0}} =
               RunnerCommands.CompactEntityResponse.decode(raw)
    end

    test "init_values and a migration's state apply through state_from_json, per declared key" do
      %{op: op} = start_operative(StaticCounterOp)

      assert :ok =
               Commands.apply_json(
                 op,
                 "op-1",
                 ~s({"scounter":{"a":{"value":5},"zz":{"value":1}},"nope":{}})
               )

      assert {:ok, %{value: 5}, 0} = Entity.read(entity!(op, "scounter", "a"), 1_000)
      assert {:ok, %{value: 0}, 0} = Entity.read(entity!(op, "scounter", "b"), 1_000)
      assert :ok = Commands.apply_json(op, "op-1", "")
      assert {:error, "init_values: not a JSON object"} = Commands.apply_json(op, "op-1", "[1]")

      %{op: single} = start_operative(CounterOp)
      s = server_state(single, nil)

      Commands.dispatch(
        :entity_apply_state,
        %{operative_id: "op-1", factory_id: "f", state_binary: ~s({"counter":{"value":8}})},
        respond(),
        s
      )

      assert_receive {:replied, "success", [], <<>>, %{}}, 2_000
      assert {:ok, %{value: 8}, 0} = Entity.read(entity!(single, "counter"), 1_000)
    end

    test "the credit commands arm and grant the window and ack the session" do
      {credit, _counters} = credit()
      s = %ServerState{operatives: %{}, credit: credit}

      Commands.dispatch(
        :credit_window_init,
        %{stream_id: "s7", window_bytes: 64, low_watermark_bytes: 32},
        respond(),
        s
      )

      assert_receive {:replied, "success", [], raw, %{}}

      assert {:ok, %RunnerCommands.CreditWindowAck{stream_id: "s7"}} =
               RunnerCommands.CreditWindowAck.decode(raw)

      assert Credit.session(credit) == "s7"

      Commands.dispatch(:credit_grant, %{stream_id: "s7", bytes: 100}, respond(), s)
      assert_receive {:replied, "success", [], <<>>, %{}}
      assert :ok = Credit.acquire(credit, 150)

      Commands.dispatch(:entity_mailbox_status, %{entity_id: <<>>, status: "full"}, respond(), s)
      assert_receive {:replied, "success", [], <<>>, %{}}
    end
  end

  # A controller side for the shutdown: acks every notification it decodes, or none.
  defp controller_side(ctrl, ack?) do
    test = self()

    spawn_link(fn ->
      :ok = Flezha.Transport.Stream.controlling_process(ctrl, self())
      :ok = Flezha.Transport.Stream.set_active(ctrl)
      controller_loop(ctrl, ack?, test)
    end)
  end

  defp controller_loop(ctrl, ack?, test) do
    receive do
      {:flz_data, _id, "control_command", bin, _meta} ->
        {:ok, %{reference_id: ref, command: {:runner_shutdown_notification, notif}}} =
          Codec.decode_command(bin)

        send(test, {:notified, notif.operative_id, notif.serialized_state})

        if ack? do
          ack =
            {:runner_shutdown_ack,
             %RunnerCommands.RunnerShutdownAck{operative_id: notif.operative_id}}

          Flezha.Transport.Stream.send_frame(
            ctrl,
            "control_command",
            Codec.encode_request(ref, ack),
            %{}
          )
        end

        controller_loop(ctrl, ack?, test)
    end
  end

  defp runner_side do
    {sdk, ctrl} = Flezha.SDK.LocalStream.open()
    :ok = Flezha.Transport.Stream.controlling_process(sdk, self())
    :ok = Flezha.Transport.Stream.set_active(sdk)
    {sdk, ctrl}
  end

  describe "the shutdown" do
    test "a stop ships every operative's Decision-10 state, waits for the acks and returns" do
      %{op: op} = start_operative(CounterOp)
      Inbox.input(op, "in", "3", %{})
      [_, _] = frames(2)
      {sdk, ctrl} = runner_side()
      controller_side(ctrl, true)

      started = System.monotonic_time(:millisecond)

      assert :ok =
               Commands.shutdown(
                 %ServerState{
                   control_handle: sdk,
                   control: Hold.new(),
                   operatives: %{"op-1" => {op, "f"}}
                 },
                 2_000
               )

      assert System.monotonic_time(:millisecond) - started < 1_500
      assert_received {:notified, "op-1", json}
      assert Jason.decode!(json) == %{"counter" => %{"value" => 3}}
    end

    test "an ack that never comes ends the wait at the deadline with the reference's line" do
      :ok = :logger.set_module_level(Commands, :warning)
      on_exit(fn -> :logger.unset_module_level(Commands) end)
      %{op: op} = start_operative(CounterOp)
      {sdk, ctrl} = runner_side()
      controller_side(ctrl, false)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok =
                   Commands.shutdown(
                     %ServerState{
                       control_handle: sdk,
                       control: Hold.new(),
                       operatives: %{"op-1" => {op, "f"}}
                     },
                     200
                   )
        end)

      assert log =~ "RunnerShutdownAck timeout for op-1"
      assert_received {:notified, "op-1", _json}
    end

    test "with no connection or no operative there is nothing to ship" do
      assert :ok =
               Commands.shutdown(
                 %ServerState{control_handle: nil, operatives: %{"x" => {self(), "f"}}},
                 10
               )

      assert :ok = Commands.shutdown(%ServerState{control_handle: :closed, operatives: %{}}, 10)
    end
  end
end
