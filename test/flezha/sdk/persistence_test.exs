defmodule Flezha.SDK.PersistenceTest do
  @moduledoc """
  The SDK's own persistence core: the record it writes, the fold it reads
  back, the surface a procedure declares, and what an Entity does with them —
  shipping, the slot, restore, compaction. No controller, no real transport:
  frames are read off a native `LocalStream` the test owns.
  """
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  import FlezhaTest.Helper.SdkOperativeHarness

  alias Flezha.SDK.{
    Codec,
    Counters,
    Ctx,
    Entity,
    Operative,
    Park,
    Procedure,
    Record,
    Timers,
    Topology
  }

  alias Flezha.SDK.Operative.Inbox
  alias Flezha.SDK.Wire

  alias FlezhaTest.Helper.SdkPersistenceFixtures.{
    CappedOp,
    CounterOp,
    CounterProc,
    CounterV2Op,
    CounterV2Proc,
    DynCounterOp,
    EchoOp,
    EchoProc,
    EventTimeOp,
    FlooredProc,
    GatedTtlOp,
    KeyedOp,
    LogicalClockOp,
    MixedSinksOp,
    NowOp,
    OptedOutCounterOp,
    OptedOutOp,
    SingleKeyedOp,
    ThriftyProc,
    TimerOp,
    TtlOp,
    TwoCountersOp
  }

  @identity Park.identity("op-1", "counter", "")

  describe "the header" do
    test "is sixteen bytes: language 3, the SDK's version, the schema, etf, zeros, the body's crc" do
      body = Record.encode_body(%{value: 1})
      assert <<3, vsn::32-big, 1::32-big, 7, 0, 0, crc::32-big>> = Record.header(body, 1)
      assert vsn == Record.sdk_version()
      assert crc == :erlang.crc32(body)
      assert Codec.sdk_lang() == 3 and Record.format_id() == 7
    end

    test "the version packs major, minor and patch into one word" do
      [major, minor, patch] =
        Application.spec(:flezha, :vsn)
        |> to_string()
        |> String.split(".")
        |> Enum.map(&String.to_integer/1)

      assert Record.sdk_version() == major * 65_536 + minor * 256 + patch
    end
  end

  describe "a frame" do
    test "round-trips through the controller's decoder with its identity, seq, header and body" do
      body = Record.encode_body({:delta, 3})
      frame = Record.frame(@identity, 5, body, 1)

      assert {:ok, %{entity_id: {"op-1", "counter", ""}, seq: 5, header: header, body: ^body}} =
               Wire.decode_frame(frame)

      assert header == Record.header(body, 1)
      assert {:ok, {:delta, 3}} = Record.decode_body(body)
    end
  end

  describe "the fold" do
    test "a plain log folds in seq order and reports the terminal" do
      log =
        change_record(0, {:delta, 1}) <>
          change_record(1, {:delta, 2}) <> change_record(2, {:delta, 4})

      assert {:ok, %{state: %{value: 7}}, 2, false} =
               Record.fold(CounterProc, nil, <<>>, <<>>, log, 0)
    end

    test "a snapshot seeds the state and the tail folds over it" do
      snap = Record.encode_body(%{value: 10})
      log = change_record(4, {:delta, 5})

      assert {:ok, %{state: %{value: 15}}, 4, false} =
               Record.fold(CounterProc, nil, Record.header(snap, 1), snap, log, 3)
    end

    test "a torn tail ends the walk with what was whole" do
      whole = change_record(0, {:delta, 1})
      torn = binary_part(change_record(1, {:delta, 2}), 0, 20)

      assert {:ok, %{state: %{value: 1}}, 0, false} =
               Record.fold(CounterProc, nil, <<>>, <<>>, whole <> torn, 0)
    end

    test "a snapshot-only payload takes the controller's terminal" do
      snap = Record.encode_body(%{value: 3})

      assert {:ok, %{state: %{value: 3}}, 9, false} =
               Record.fold(CounterProc, nil, Record.header(snap, 1), snap, <<>>, 9)
    end

    test "an empty payload is init_state at the hint" do
      assert {:ok, %{state: %{value: 0}}, 0, false} =
               Record.fold(CounterProc, nil, <<>>, <<>>, <<>>, 0)
    end

    test "a foreign language is refused before its format is looked at" do
      body = Record.encode_body({:delta, 1})
      rust_postcard = <<1, 0::32, 1::32, 1, 0, 0, :erlang.crc32(body)::32>>

      assert {:error, reason} =
               Record.fold(CounterProc, nil, <<>>, <<>>, disk_record(0, rust_postcard, body), 0)

      assert reason ==
               "sdk_lang mismatch: this SDK is elixir (3), the record was written by rust (1); " <>
                 "durable bodies are per-SDK and are never decoded across languages"
    end

    test "a format this SDK does not write is refused by name" do
      body = Record.encode_body({:delta, 1})
      msgpack = <<3, 0::32, 1::32, 5, 0, 0, :erlang.crc32(body)::32>>

      assert {:error,
              "format_id 5 is not one this SDK writes (2, 3, 7): " <>
                "the body cannot be decoded"} =
               Record.fold(CounterProc, nil, <<>>, <<>>, disk_record(0, msgpack, body), 0)
    end

    test "a record from the future is refused with the core's wording" do
      assert {:error, reason} =
               Record.fold(CounterProc, nil, <<>>, <<>>, change_record(0, {:delta, 1}, 2), 0)

      assert reason ==
               "record stamped schema_version 2 is above the running procedure's 1: " <>
                 "older code cannot read newer state"
    end

    test "a record below the floor is refused" do
      assert {:error, reason} =
               Record.fold(CounterProc, nil, <<>>, <<>>, change_record(0, {:delta, 1}, 0), 0)

      assert reason ==
               "record stamped schema_version 0 is below the readable floor 1 " <>
                 "the running procedure declares"
    end

    test "between the floor and the running version a record migrates; a floor above running reads running only" do
      body = Record.encode_body({:delta, 1})
      assert {:ok, {:migrate, 1}} = Record.resolve(Record.header(body, 1), {2, 1})
      assert {:ok, :decode} = Record.resolve(Record.header(body, 2), {2, 1})
      assert {:ok, :decode} = Record.resolve(Record.header(body, 2), {2, 5})
    end

    test "a crc that does not cover header and body is refused with the frame seam's refusal" do
      body = Record.encode_body({:delta, 1})
      header = Record.header(body, 1)

      corrupt =
        <<3::64-big, 16 + byte_size(body)::32-big, header::binary, body::binary, 0::32-big>>

      # The seam's own sentence, not a second one phrased in the SDK's words: a
      # payload has no torn tail to repair, so a crc failure is refused however
      # few bytes follow it.
      log =
        capture_log(fn ->
          assert {:error, reason} = Record.fold(CounterProc, nil, <<>>, <<>>, corrupt, 0)
          assert reason =~ "durable record refused (crc_mismatch) in change log at byte offset 0"
        end)

      assert log =~ "durable record refused (crc_mismatch)"
    end

    test "a short snapshot header is a malformed header" do
      snap = Record.encode_body(%{value: 1})

      assert {:error, "malformed restoration header (truncated or invalid)"} =
               Record.fold(CounterProc, nil, <<1, 2, 3>>, snap, <<>>, 0)
    end

    test "a fold that raises inside apply_change is a refusal, not a crash" do
      assert {:error, "restore fold failed: " <> _} =
               Record.fold(CounterProc, nil, <<>>, <<>>, change_record(0, :not_a_delta), 0)
    end

    test "a timer-ops record folds into the pending set and leaves the state untouched" do
      log =
        change_record(0, {:delta, 1}) <>
          timer_ops_record(1, [
            Timers.register_op(:processing_time, 9_000),
            Timers.register_op(:event_time, 100)
          ]) <> timer_ops_record(2, [Timers.cancel_op(:event_time, 100)])

      assert {:ok, %{state: %{value: 1}, timers: timers}, 2, false} =
               Record.fold(CounterProc, nil, <<>>, <<>>, log, 0)

      assert Timers.all(timers, :processing_time) == [9_000]
      assert Timers.all(timers, :event_time) == []
    end

    test "a timer-ops header on a snapshot is malformed; a timer-ops record at a migrated schema reports migrated" do
      snap = Record.encode_body(%{value: 1})

      assert {:error, "malformed restoration header (truncated or invalid)"} =
               Record.fold(
                 CounterProc,
                 nil,
                 Record.header(snap, 1, Record.timer_ops_format_id()),
                 snap,
                 <<>>,
                 0
               )

      log = timer_ops_record(0, [Timers.register_op(:processing_time, 9_000)], 1)

      assert {:ok, %{state: %{value: 0, unit: "n"}, timers: timers}, 0, true} =
               Record.fold(CounterV2Proc, nil, <<>>, <<>>, log, 0)

      assert Timers.contains?(timers, :processing_time, 9_000)
    end

    test "an effect record folds its enclosed changes, its timers, its offset and its event time" do
      log =
        change_record(0, {:delta, 1}) <>
          effect_record(1, %{
            changes: [Record.encode_body({:delta, 2}), Record.encode_body({:delta, 4})],
            outputs: [%{channel: "out", key: "k1", metadata: [], payload: "k1=a"}],
            consumed_offset: 9,
            dlq: nil,
            event_time: 400,
            timers: [Timers.register_op(:processing_time, 9_000)]
          })

      assert {:ok, restored, 1, false} = Record.fold(CounterProc, nil, <<>>, <<>>, log, 0)
      assert restored.state == %{value: 7}
      assert Timers.all(restored.timers, :processing_time) == [9_000]
      assert restored.floor == 9
      assert restored.high_water == 400
    end

    test "a log with no effect record has no floor and no high-water" do
      assert {:ok, restored, 0, false} =
               Record.fold(CounterProc, nil, <<>>, <<>>, change_record(0, {:delta, 1}), 0)

      assert restored.floor == -1
      assert restored.high_water == nil
    end

    test "an effect-record header on a snapshot is malformed; an effect record at a migrated schema reports migrated" do
      snap = Record.encode_body(%{value: 1})

      assert {:error, "malformed restoration header (truncated or invalid)"} =
               Record.fold(
                 CounterProc,
                 nil,
                 Record.header(snap, 1, Record.effect_record_format_id()),
                 snap,
                 <<>>,
                 0
               )

      log =
        effect_record(
          0,
          %{
            changes: [Record.encode_body({:delta, 3})],
            outputs: [],
            consumed_offset: nil,
            dlq: nil,
            event_time: 0,
            timers: []
          },
          1
        )

      assert {:ok, %{state: %{value: 3, unit: "n"}}, 0, true} =
               Record.fold(CounterV2Proc, nil, <<>>, <<>>, log, 0)
    end
  end

  describe "the clocks" do
    test "Ctx.now/1 serves the declared clock: the seq the dispatch started at, the input's event time, or the epoch millisecond" do
      %{op: op} = start_operative(LogicalClockOp)
      for n <- [5, 6, 7], do: Inbox.input(op, "in", Integer.to_string(n), %{})
      assert for({"op-1", out, _} <- frames(6), do: out) == ["0", "1", "2"]

      %{op: op} = start_operative(EventTimeOp, operative_id: "op-2")
      Inbox.input(op, "in", "1720000000000", %{})
      assert [{"op-2", "1720000000000", _}] = frames(1)

      %{op: op} = start_operative(NowOp, operative_id: "op-3")
      before = System.system_time(:millisecond)
      Inbox.input(op, "in", "x", %{})
      assert [{"op-3", now, _}] = frames(1)
      assert String.to_integer(now) >= before
    end
  end

  describe "timers" do
    test "a registration ships one timer-ops record after the dispatch's change records, at the next seq" do
      %{op: op} = start_operative(TimerOp)
      before = System.system_time(:millisecond)
      Inbox.input(op, "in", "3", %{})
      [r0, r1, out] = frames(3)
      assert {0, {:delta, 3}, _} = record(r0)

      assert {1, [%{kind: :processing_time, register: true, scheduled_at: at}], decoded} =
               timer_ops(r1)

      assert <<3, _::32, 1::32, 3, 0, 0, _::32>> = decoded.header
      assert at >= before + 3_000
      assert {"op-1", "3", _} = out

      Inbox.input(op, "in", "4", %{})
      [_r2, r3, _out] = frames(3)
      assert {3, [%{register: true}], _} = timer_ops(r3)
      assert {:ok, %{value: 7}, 4} = Entity.read(entity!(op, "timer"), 1_000)
    end

    test "compaction re-states the pending set above the snapshot's seq, and the slot carries it" do
      %{op: op} = start_operative(TimerOp)
      Inbox.input(op, "in", "1", %{})
      [_r0, r1, _out] = frames(3)
      {1, [%{scheduled_at: at}], _} = timer_ops(r1)
      pid = entity!(op, "timer")

      assert {:ok, 1, _size} = Entity.snapshot(pid)
      [{"entity_state_snapshot", _, _}, restated] = frames(2)

      assert {2, [%{kind: :processing_time, register: true, scheduled_at: ^at}], _} =
               timer_ops(restated)

      assert {:ok, %{value: 1}, 3} = Entity.read(pid, 1_000)

      identity = Park.identity("op-1", "timer", "")

      assert [{^identity, held}] = :ets.lookup(:sys.get_state(op).slots, identity)
      assert %{state: %{value: 1}, seq: 3, floor: -1} = held
      assert Timers.all(held.timers, :processing_time) == [at]
    end
  end

  describe "the surface" do
    test "apply folds through apply_change and records the change; a discard drops it" do
      ctx = Ctx.new(CounterProc, nil, 0)
      assert Ctx.apply(ctx, %{value: 1}, {:delta, 2}) == %{value: 3}
      assert Ctx.take_changes(ctx) == [{:delta, 2}]

      ctx = Ctx.new(CounterProc, nil, 0)
      Ctx.apply(ctx, %{value: 0}, {:delta, 1})
      Ctx.discard(ctx)
      assert Ctx.take_changes(ctx) == []
    end

    test "changeful is what exports apply_change/2; the knobs default and override" do
      assert Procedure.changeful?(CounterProc)
      refute Procedure.changeful?(EchoProc)
      assert Procedure.compact_after_bytes(CounterProc) == Procedure.default_compact_after_bytes()
      assert Procedure.compact_after_bytes(ThriftyProc) == 1_048_576
      assert Procedure.state_from_json(EchoProc, %{"value" => 1}) == %{"value" => 1}
      assert Procedure.state_from_json(CounterProc, %{"value" => 1}) == %{value: 1}
    end

    test "the topology carries the class's persistence facts" do
      assert {:ok, topo} = Topology.build(OptedOutOp)
      refute topo.persistence_enabled?
      assert topo.compact_after_bytes == 1_048_576
    end

    test "two nodes of one changeful module would share a WAL, so the class is refused" do
      assert {:error, {:invalid_topology, issues}} = Topology.build(TwoCountersOp)
      assert {:DuplicatePersistentProcedure, "counter"} in issues
    end
  end

  describe "keyed sinks" do
    @label "op-1/stamped/k0"

    test "a keyed sink stamps flz.entity, flz.seq and flz.key after user metadata; the ordinal advances per plugin output and restarts per item" do
      %{op: op} = start_operative(KeyedOp)
      Inbox.input(op, "in", "4", %{})

      assert [{"entity_state_change", _, _}, {"op-1", "a", first}, {"op-1", "b", second}] =
               frames(3)

      assert first == %{
               "channel" => "c",
               "tag" => "x",
               "flz.entity" => @label,
               "flz.seq" => "0.0",
               "flz.key" => "k0"
             }

      assert second == %{
               "channel" => "c",
               "flz.entity" => @label,
               "flz.seq" => "0.1",
               "flz.key" => "k0"
             }

      # The next item: the handout seq moved to 1 and the ordinal restarted.
      Inbox.input(op, "in", "6", %{})

      assert [_record, {"op-1", "a", %{"flz.seq" => "1.0"}}, {"op-1", "b", %{"flz.seq" => "1.1"}}] =
               frames(3)
    end

    test "a plain sink beside a keyed one advances the ordinal without being stamped" do
      %{op: op} = start_operative(MixedSinksOp)
      Inbox.input(op, "in", "4", %{})

      assert [
               _record,
               {"op-1", "a", keyed_a},
               {"op-1", "a", plain_a},
               {"op-1", "b", keyed_b},
               {"op-1", "b", plain_b}
             ] =
               frames(5)

      assert keyed_a["flz.seq"] == "0.0" and keyed_b["flz.seq"] == "0.2"
      assert plain_a == %{"channel" => "c", "tag" => "x"} and plain_b == %{"channel" => "c"}
    end

    test "a keyless emit from a Single Entity on a keyed sink is refused at emit time, nothing emitted" do
      %{op: op, counters: counters} = start_operative(SingleKeyedOp)
      Inbox.input(op, "in", "x", %{})

      assert_receive {:"$gen_cast", {:park, _pid, _identity, %{error: error}}}, 2_000
      assert error =~ "key_required_on_keyed_channel"
      assert error =~ "\"r\""
      # Emits flush only on `{:ok, _}`, and no dispatch completed.
      assert Counters.dispatches(counters) == 0
    end
  end

  describe "dynamic entities" do
    setup do
      id = {__MODULE__, make_ref()}
      :telemetry.attach(id, [:flezha, :sdk, :dynamic], &__MODULE__.forward_dynamic/4, self())
      on_exit(fn -> :telemetry.detach(id) end)
      :ok
    end

    def forward_dynamic(_event, measurements, metadata, pid),
      do: send(pid, {:dynamic, measurements, metadata})

    test "a dynamic node with max_entities 2 mints two Entities and drops the third key's input, counting it" do
      %{op: op, counters: counters} = start_operative(CappedOp)
      for n <- ~w(1 2 3), do: Inbox.input(op, "in", n, %{})
      # Two dispatches: two records and two outputs, and nothing for k3.
      [_, _, _, _] = frames(4)
      assert [{"capped", :dynamic, [{"k1", _}, {"k2", _}]}] = Operative.entities(op)
      assert_receive {:dynamic, %{count: 1}, %{operative_id: "op-1", event: :capped}}, 1_000
      assert Counters.dispatches(counters) == 2

      # A live key is unaffected by the cap.
      Inbox.input(op, "in", "1", %{})
      assert [{"entity_state_change", _, _}, {"op-1", "2", _}] = frames(2)
    end

    test "an Entity idle past its TTL is evicted on the sweep: delete_entity_state follows its record, the slot is gone, the key reads evicted, the next input cold-starts at seq 0" do
      %{op: op} = start_operative(TtlOp)
      Inbox.input(op, "in", "5", %{})
      assert [{"entity_state_change", _, _}, {"op-1", "5", _}] = frames(2)
      {:ok, pid} = Operative.lookup(op, "ttl", "k5")

      # A sweep right after the push keeps it: not idle past the TTL yet.
      send(op, :reap)
      assert {:ok, ^pid} = Operative.lookup(op, "ttl", "k5")

      assert_evicted(pid, {"op-1", "ttl", "k5"})
      assert_receive {:dynamic, %{count: 1}, %{operative_id: "op-1", event: :evicted}}, 1_000
      assert {:error, :entity_evicted} = Operative.lookup(op, "ttl", "k5")
      assert {:error, :entity_not_found} = Operative.lookup(op, "ttl", "never")
      assert [{"ttl", :dynamic, []}] = Operative.entities(op)

      # A cold start for the same key: `init_state/1` at seq 0, so the value
      # reads 5 rather than the evicted 10.
      Inbox.input(op, "in", "5", %{})
      assert [{"entity_state_change", _, _} = r, {"op-1", "5", _}] = frames(2)
      assert {0, {:delta, 5}, _} = record(r)
      assert {:ok, fresh} = Operative.lookup(op, "ttl", "k5")
      assert fresh != pid
    end

    test "a busy Entity is skipped by the sweep and evicted once it is idle" do
      %{op: op} = start_operative(GatedTtlOp)
      Inbox.input(op, "in", "x", %{})
      {:ok, pid} = Operative.lookup(op, "gated_ttl", "x")

      # Blocked inside run/3: the probe finds it busy and the sweep moves on.
      send(op, :reap)
      assert {:ok, ^pid} = Operative.lookup(op, "gated_ttl", "x")
      assert Process.alive?(pid)

      ref = Process.monitor(pid)
      send(pid, :go)
      assert_receive {:flz_data, _, "op-1", "x", _}, 2_000
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 2_000
      assert_receive {:dynamic, %{count: 1}, %{operative_id: "op-1", event: :evicted}}, 1_000
      assert {:error, :entity_evicted} = Operative.lookup(op, "gated_ttl", "x")
    end
  end

  describe "schema evolution" do
    test "a procedure declaring schema_version 2 stamps 2; with a floor of 1 a v1 snapshot and a v1 change fold through its migrations and the fold reports migrated" do
      %{op: op} = start_operative(CounterV2Op)
      Inbox.input(op, "in", "1", %{})
      assert [{"entity_state_change", frame, _}, {"op-1", "1", _}] = frames(2)
      assert {:ok, %{header: <<3, _::32, 2::32-big, 7, _::binary>>}} = Wire.decode_frame(frame)

      snapshot = Record.encode_body(%{value: 4})

      assert {:ok, %{state: %{value: 7, unit: "n"}}, 0, true} =
               Record.fold(
                 CounterV2Proc,
                 nil,
                 Record.header(snapshot, 1),
                 snapshot,
                 change_record(0, {:delta, 3}, 1),
                 0
               )

      # A record at the running version decodes directly, and reports no migration.
      assert {:ok, %{state: %{value: 2, unit: "n"}}, 0, false} =
               Record.fold(
                 CounterV2Proc,
                 nil,
                 <<>>,
                 <<>>,
                 change_record(0, {:delta, 2, "x"}, 2),
                 0
               )
    end

    test "a failing migration refuses naming its reason and the version; a floor without a migration keeps the present wording" do
      snapshot = Record.encode_body(:not_a_counter)

      assert {:error, "migration from schema_version 1 failed (snapshot): bad shape"} =
               Record.fold(CounterV2Proc, nil, Record.header(snapshot, 1), snapshot, <<>>, 0)

      assert {:error, "no migration from schema_version 1: the procedure declares none (change)"} =
               Record.fold(FlooredProc, nil, <<>>, <<>>, change_record(0, {:delta, 1}, 1), 0)
    end
  end

  describe "the Entity" do
    test "a changeful dispatch ships one record per change, each before its own output frame" do
      %{op: op} = start_operative(CounterOp)
      Inbox.input(op, "in", "3", %{})
      Inbox.input(op, "in", "4", %{})

      # The Entity writes its records to the stream itself and hands its
      # outputs to the Operative, so a later dispatch's record may overtake an
      # earlier dispatch's output: the invariant is per dispatch.
      four = frames(4)
      ids = Enum.map(four, &elem(&1, 0))
      assert Enum.sort(ids) == ["entity_state_change", "entity_state_change", "op-1", "op-1"]
      [r0, r1] = for {"entity_state_change", _, _} = r <- four, do: record(r)
      assert {0, {:delta, 3}, %{entity_id: {"op-1", "counter", ""}}} = r0
      assert {1, {:delta, 4}, _} = r1

      assert [{"op-1", "3", %{"channel" => "c"}}, {"op-1", "7", _}] =
               for({"op-1", _, _} = o <- four, do: o)

      # Per dispatch, both of them: the running sum names which dispatch an
      # output came from, so each is compared with its own record rather than
      # the two firsts with each other.
      seen =
        Enum.map(four, fn
          {"entity_state_change", _, _} = f -> {:change, elem(record(f), 0)}
          {"op-1", value, _} -> {:output, value}
        end)

      for {seq, value} <- [{0, "3"}, {1, "7"}] do
        assert Enum.find_index(seen, &(&1 == {:change, seq})) <
                 Enum.find_index(seen, &(&1 == {:output, value})),
               "the record of change #{seq} must precede the output it produced"
      end
    end

    test "no output leaves ahead of the change that produced it, in a burst and one dispatch at a time" do
      %{op: op} = start_operative(KeyedOp)

      # Even inputs all key to `k0`, so one Entity holds one seq line and a
      # dispatch's handout seq — the `flz.seq` its keyed outputs carry — is the
      # seq of the change record that same dispatch shipped. That stamp is the
      # join: it says which change produced an output, rather than leaving the
      # test to infer it from arrival order.
      #
      # A burst first: twelve inputs before a frame is read, so later records
      # overtake earlier outputs and the frames of one dispatch are not
      # adjacent. The per-dispatch order must hold through that.
      burst = for n <- 1..12, do: n * 2
      for n <- burst, do: Inbox.input(op, "in", Integer.to_string(n), %{})
      assert_change_before_output(Enum.map(frames(3 * length(burst)), &kind/1), length(burst))

      # Then one dispatch at a time, its frames read before the next input is
      # sent. The Operative is idle when the emits reach it, so an output
      # released ahead of its record reaches the stream first every time rather
      # than only when the Entity loses a race — which is what makes this half
      # able to fail.
      settled =
        Enum.flat_map(26..40//2, fn n ->
          Inbox.input(op, "in", Integer.to_string(n), %{})
          Enum.map(frames(3), &kind/1)
        end)

      assert_change_before_output(settled, 8)
    end

    test "a procedure without apply_change/2 ships nothing and holds no seq" do
      %{op: op} = start_operative(EchoOp)
      Inbox.input(op, "in", "hi", %{})
      assert [{"op-1", "hi", _}] = frames(1)
      refute_received {:flz_data, _, "entity_state_change", _, _}
      assert {:ok, nil, 0} = Entity.read(entity!(op, "echo"), 1_000)
    end

    test "with the class's persistence off the seq advances and nothing ships" do
      %{op: op} = start_operative(OptedOutCounterOp)
      Inbox.input(op, "in", "1", %{})
      Inbox.input(op, "in", "2", %{})
      assert [{"op-1", "1", _}, {"op-1", "3", _}] = frames(2)
      refute_received {:flz_data, _, "entity_state_change", _, _}
      assert {:ok, %{value: 3}, 2} = Entity.read(entity!(op, "counter"), 1_000)
    end

    test "an Entity killed untrappably comes back on its seq line from the slot" do
      %{op: op} = start_operative(CounterOp)
      Inbox.input(op, "in", "1", %{})
      Inbox.input(op, "in", "2", %{})
      [_, _, _, _] = frames(4)
      first = kill!(op, "counter")

      Inbox.input(op, "in", "4", %{})
      [r, out] = frames(2)
      assert {2, {:delta, 4}, _} = record(r)
      assert {"op-1", "7", _} = out
      assert entity!(op, "counter") != first
    end

    test "the Entity of a stopped Operative is dead, and of a killed one too" do
      %{op: op} = start_operative(CounterOp)
      stopped = entity!(op, "counter")
      ref = Process.monitor(stopped)
      Operative.stop(op)
      assert_receive {:DOWN, ^ref, :process, ^stopped, :normal}, 2_000

      %{op: op} = start_operative(CounterOp, operative_id: "op-2")
      killed = entity!(op, "counter")
      ref = Process.monitor(killed)
      Process.unlink(op)
      Process.exit(op, :kill)
      # Quietly, whatever ended the Operative: the Entity did not crash.
      assert_receive {:DOWN, ^ref, :process, ^killed, :normal}, 2_000
    end

    test "a restore re-seats the state and the seq, and dispatch continues from terminal + 1" do
      %{op: op} = start_operative(CounterOp)
      pid = entity!(op, "counter")
      snap = Record.encode_body(%{value: 10})
      log = change_record(4, {:delta, 5})

      assert {:ok, 4, false} = Entity.restore(pid, Record.header(snap, 1), snap, log, 3)
      assert {:ok, %{value: 15}, 5} = Entity.read(pid, 1_000)

      Inbox.input(op, "in", "1", %{})
      [r, out] = frames(2)
      assert {5, {:delta, 1}, _} = record(r)
      assert {"op-1", "16", _} = out
    end

    test "a refused restore names its reason and leaves the Entity as it was" do
      %{op: op} = start_operative(CounterOp)
      pid = entity!(op, "counter")
      foreign = change_record(0, {:delta, 1}, 3)

      assert {:error, "record stamped schema_version 3" <> _} =
               Entity.restore(pid, <<>>, <<>>, foreign, 0)

      assert {:ok, %{value: 0}, 0} = Entity.read(pid, 1_000)
    end

    test "compaction ships the snapshot after the records, at change_seq - 1" do
      %{op: op} = start_operative(CounterOp)
      Inbox.input(op, "in", "1", %{})
      Inbox.input(op, "in", "2", %{})
      [_, _, _, _] = frames(4)
      pid = entity!(op, "counter")
      assert {:ok, 1, size} = Entity.snapshot(pid)

      [{"entity_state_snapshot", frame, _}] = frames(1)
      assert {:ok, %{seq: 1, body: body, header: header}} = Wire.decode_frame(frame)
      assert byte_size(body) == size
      assert {:ok, %{value: 3}} = Record.decode_body(body)
      assert {:ok, :decode} = Record.resolve(header, {1, 1})
    end

    test "apply_state goes through state_from_json/1 and the read sees it" do
      %{op: op} = start_operative(CounterOp)
      pid = entity!(op, "counter")
      assert :ok = Entity.apply_state(pid, %{"value" => 9})
      assert {:ok, %{value: 9}, 0} = Entity.read(pid, 1_000)
      Inbox.input(op, "in", "1", %{})
      assert [_r, {"op-1", "10", _}] = frames(2)
    end

    test "a fresh secondary reaches the Entities that already exist" do
      %{op: op} = start_operative(CounterOp)
      Inbox.input(op, "in", "1", %{})
      [_, _] = frames(2)

      ctrl_sec = rebind_secondary(op)
      Inbox.input(op, "in", "2", %{})

      assert_receive {:flz_data, id, "entity_state_change", _, _}, 2_000
      assert id == ctrl_sec.id
    end

    test "lookup names what is missing, and materialises a dynamic key only when asked" do
      %{op: op} = start_operative(DynCounterOp)
      assert {:error, :procedure_not_found} = Operative.lookup(op, "nope", nil)
      assert {:error, :entity_not_found} = Operative.lookup(op, "counter", "k1")
      assert {:ok, pid} = Operative.lookup(op, "counter", "k1", materialise: true)
      assert {:ok, ^pid} = Operative.lookup(op, "counter", "k1")
      assert [{"counter", :dynamic, [{"k1", ^pid}]}] = Operative.entities(op)

      %{op: single} = start_operative(CounterOp)
      assert {:error, :entity_not_found} = Operative.lookup(single, "counter", "k")
      assert [{"counter", :single, [{nil, _}]}] = Operative.entities(single)
    end
  end

  # Every output is matched with the change record its `flz.seq` names, and the
  # record must sit earlier in the arrival. `dispatches` is the vacuity guard:
  # without it an arrival holding no record, or no output, would satisfy the
  # comparison while proving nothing.
  defp assert_change_before_output(arrival, dispatches) do
    records = for {{:change, seq, _}, i} <- Enum.with_index(arrival), into: %{}, do: {seq, i}

    outputs =
      for {{:output, "op-1", value, %{"flz.seq" => stamp}}, i} <- Enum.with_index(arrival) do
        {stamp |> String.split(".") |> hd() |> String.to_integer(), value, i}
      end

    assert map_size(records) == dispatches
    assert length(outputs) == 2 * dispatches
    assert Enum.all?(outputs, fn {seq, _, _} -> Map.has_key?(records, seq) end)

    early =
      for {seq, value, at} <- outputs,
          Map.fetch!(records, seq) > at,
          do: {value, seq, at, Map.fetch!(records, seq)}

    assert early == [],
           "an output left before the change record that produced it " <>
             "(output, change seq, output position, record position): #{inspect(early)}"
  end
end
