defmodule Flezha.SDK.PersistenceTest do
  @moduledoc """
  The SDK's own persistence core: the record it writes, the fold it reads
  back, the surface a procedure declares, and what an Entity does with them —
  shipping, the slot, restore, compaction. No controller, no real transport:
  frames are read off a native `Local.Stream` the test owns.
  """
  use ExUnit.Case, async: true

  import FlezhaTest.Helper.SdkOperativeHarness

  alias Flezha.SDK.{Codec, Ctx, Entity, Operative, Park, Procedure, Record, Topology}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.State.Wire

  alias FlezhaTest.Helper.SdkPersistenceFixtures.{
    CounterOp,
    CounterProc,
    DynCounterOp,
    EchoOp,
    EchoProc,
    OptedOutCounterOp,
    OptedOutOp,
    ThriftyProc,
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

      assert {:ok, %{value: 7}, 2, false} = Record.fold(CounterProc, nil, <<>>, <<>>, log, 0)
    end

    test "a snapshot seeds the state and the tail folds over it" do
      snap = Record.encode_body(%{value: 10})
      log = change_record(4, {:delta, 5})

      assert {:ok, %{value: 15}, 4, false} =
               Record.fold(CounterProc, nil, Record.header(snap, 1), snap, log, 3)
    end

    test "a torn tail ends the walk with what was whole" do
      whole = change_record(0, {:delta, 1})
      torn = binary_part(change_record(1, {:delta, 2}), 0, 20)

      assert {:ok, %{value: 1}, 0, false} =
               Record.fold(CounterProc, nil, <<>>, <<>>, whole <> torn, 0)
    end

    test "a snapshot-only payload takes the controller's terminal" do
      snap = Record.encode_body(%{value: 3})

      assert {:ok, %{value: 3}, 9, false} =
               Record.fold(CounterProc, nil, Record.header(snap, 1), snap, <<>>, 9)
    end

    test "an empty payload is init_state at the hint" do
      assert {:ok, %{value: 0}, 0, false} = Record.fold(CounterProc, nil, <<>>, <<>>, <<>>, 0)
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

      assert {:error, "format_id 5 is not one this SDK writes (7): the body cannot be decoded"} =
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

    test "a crc that does not cover header and body is refused naming the seq" do
      body = Record.encode_body({:delta, 1})
      header = Record.header(body, 1)

      corrupt =
        <<3::64-big, 16 + byte_size(body)::32-big, header::binary, body::binary, 0::32-big>>

      assert {:error, "crc32 mismatch on change record (seq=3)"} =
               Record.fold(CounterProc, nil, <<>>, <<>>, corrupt, 0)
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

      assert [{"op-1", "3", %{"channel" => "out"}}, {"op-1", "7", _}] =
               for({"op-1", _, _} = o <- four, do: o)

      first_record = Enum.find_index(ids, &(&1 == "entity_state_change"))
      first_output = Enum.find_index(ids, &(&1 == "op-1"))
      assert first_record < first_output, "a dispatch's record precedes its output"
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
      first = entity!(op, "counter")
      ref = Process.monitor(first)

      Inbox.input(op, "in", "kill", %{})
      assert_receive {:DOWN, ^ref, :process, ^first, :killed}, 2_000

      Inbox.input(op, "in", "4", %{})
      [r, out] = frames(2)
      assert {2, {:delta, 4}, _} = record(r)
      assert {"op-1", "7", _} = out
      assert entity!(op, "counter") != first
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

      {sdk_sec, ctrl_sec} = Flezha.Transport.Local.Stream.open()
      :ok = Flezha.Transport.Stream.controlling_process(ctrl_sec, self())
      :ok = Flezha.Transport.Stream.set_active(ctrl_sec)
      send(op, {:secondaries, [sdk_sec]})
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
end
