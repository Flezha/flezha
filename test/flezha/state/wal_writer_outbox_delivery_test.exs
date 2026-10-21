defmodule Flezha.State.WalWriterOutboxDeliveryTest do
  @moduledoc """
  When the outbox cursor may pass an entry: a sink's hold and confirmation, the
  in-flight window that back-pressures the flush instead of dropping, the
  resettle that re-drives a chain run which never reported back, and the batch
  reports that carry the same per-entry facts at once.
  """
  use FlezhaTest.OutboxCase, async: false

  alias Flezha.Relay.Calls.Token
  alias Flezha.Relay.{Builder, Lifecycle, Overwatch}

  test "stall_status reports the outbox cursors producing-liveness compares", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    assert %{out_pending_seq: -1, outbox_flushed_seq: -1} = WalWriter.stall_status(pid)

    # An effect record carrying NO outputs must not move out_pending_seq. This is
    # the whole reason it exists rather than reusing outbox_durable_seq, which
    # advances on every fsynced record: a Cell that persists state but emits
    # nothing would otherwise read as an outbox frozen forever.
    append_effect(pid, 0, [], 100)

    assert %{out_pending_seq: -1} = WalWriter.stall_status(pid)

    # An output-bearing record does move it — and the flush cursor follows it.
    append_effect(pid, 1, [output("k", "v")], 101)

    assert {"v", md, _} = next_output(2_000)
    settle(md)

    assert %{out_pending_seq: 1, outbox_flushed_seq: 1, outbox_unconfirmed_ms: 0} =
             WalWriter.stall_status(pid)

    :ok = WalWriter.close(pid)
  end

  test "stall_status cursors survive a restart re-drive without a phantom jump", %{
    rc: rc,
    cell_id: cell_id
  } do
    ClassRegistry.put_overwatch(rc, self(), nil)

    # Two output-bearing records land durably, but the flush cursor never advances
    # (crash before flush): the re-drive owes both.
    plain = Frame.encode_record(0, effect_header(), effect_body([output("k", "a")], 1))
    second = Frame.encode_record(1, effect_header(), effect_body([output("k", "b")], 2))

    wal_path = Paths.wal_path(cell_id)
    File.mkdir_p!(Path.dirname(wal_path))
    File.write!(wal_path, plain <> second)

    pid = start_writer(cell_id)

    # Seeded from what the re-drive owes, NOT from -1 — otherwise the first tick
    # after a restart would read as an outbox that suddenly grew from nothing.
    assert %{out_pending_seq: 1} = WalWriter.stall_status(pid)

    :ok = WalWriter.close(pid)
  end

  # ---------- delivery confirmation ----------

  test "a held output holds the cursor until the sink confirms", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    append_effect(pid, 0, [output("k", "held")], 1)

    assert {"held", md, _} = next_output()

    # A delivery-guaranteed sink takes ownership before the chain settles.
    :ok = Outbox.hold(md)
    settle(md)
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: -1, awaiting: 1} = WalWriter.outbox_status(pid)

    :ok = Outbox.confirm(md)
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: 0, awaiting: 0} = WalWriter.outbox_status(pid)
    assert Lineage.progress(cell_id).outbox_flushed_seq == 0
  end

  test "an unconfirmed entry blocks the cursor behind a later confirmed one", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    stage_effect(pid, 0, [output("k", "first")], 1)
    append_effect(pid, 1, [output("k", "second")], 2)

    assert {"first", md_first, _} = next_output()
    assert {"second", md_second, _} = next_output()

    for md <- [md_first, md_second] do
      :ok = Outbox.hold(md)
      settle(md)
    end

    # The later entry confirms first: the cursor may not pass the earlier one.
    :ok = Outbox.confirm(md_second)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: -1} = WalWriter.outbox_status(pid)

    :ok = Outbox.confirm(md_first)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 1, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a permanently failed output parks and advances exactly its range", %{
    rc: rc,
    cell_id: cell_id,
    base: base
  } do
    redirect_dlq_dir(base)
    pid = armed_writer(rc, cell_id)

    stage_effect(pid, 0, [output("k", "doomed")], 1)
    append_effect(pid, 1, [output("k", "fine")], 2)

    assert {"doomed", md_doomed, _} = next_output()
    assert {"fine", md_fine, _} = next_output()

    for md <- [md_doomed, md_fine] do
      :ok = Outbox.hold(md)
      settle(md)
    end

    # Parking an undeliverable output logs at :error by design — assert that
    # user-facing line instead of letting it reach the console unasserted.
    exhausted = Flezha.Error.new("kafka.exhausted", %{reason: :broker_gone})

    log =
      capture_log(fn ->
        :ok = Outbox.park(md_doomed, "doomed", exhausted, 6)
        :ok = Outbox.confirm(md_fine)
        :ok = WalWriter.fsync(pid)
      end)

    assert log =~ "parked undeliverable output"
    assert log =~ "Kafka broker: broker_gone (kafka.exhausted)"

    assert %{outbox_flushed_seq: 1, awaiting: 0} = WalWriter.outbox_status(pid)

    assert [entry] = DlqStore.list(rc)
    assert entry["cell_key"] == "k1"
    assert entry["channel"] == "out"
    assert entry["attempts"] == 6
    assert entry["error"] == Flezha.Error.object(exhausted)
    assert {:ok, _entry, "doomed"} = DlqStore.get("#{rc}/task/#{entry["id"]}")
  end

  test "the in-flight window back-pressures the flush instead of dropping", %{
    rc: rc,
    cell_id: cell_id
  } do
    Application.put_env(:flezha, :outbox_inflight_max, 1)
    on_exit(fn -> Application.delete_env(:flezha, :outbox_inflight_max) end)

    pid = armed_writer(rc, cell_id)

    stage_effect(pid, 0, [output("k", "one")], 1)
    append_effect(pid, 1, [output("k", "two")], 2)

    assert {"one", md_one, _} = next_output()
    refute_dispatched(100)

    assert %{pending: 1} = WalWriter.outbox_status(pid)

    :ok = Outbox.hold(md_one)
    settle(md_one)
    :ok = Outbox.confirm(md_one)

    # The confirmation itself refills the window — no fsync and no retry tick
    # (1 s) in between, or a burst's tail would drain one window per tick.
    assert {"two", _, _} = next_output(500)
  end

  test "a chain run that never reports back is re-dispatched, not wedged", %{
    rc: rc,
    cell_id: cell_id
  } do
    Application.put_env(:flezha, :outbox_resettle_ms, 50)
    on_exit(fn -> Application.delete_env(:flezha, :outbox_resettle_ms) end)

    pid = armed_writer(rc, cell_id)

    append_effect(pid, 0, [output("k", "lost")], 1)

    # The Overwatch died mid-chain: the output was dispatched and nothing ever
    # settled it. The retry tick re-dispatches it with identical identity.
    assert {"lost", md, _} = next_output()
    assert {"lost", md_again, _} = next_output(3_000)
    assert md_again["flz.seq"] == md["flz.seq"]
    assert md_again["flz.epoch"] == md["flz.epoch"]

    settle(md_again)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0} = WalWriter.outbox_status(pid)
  end

  test "a resettle tick with no Overwatch for the class keeps the entry owed", %{
    rc: rc,
    cell_id: cell_id
  } do
    on_exit(TestHelpers.override_env(:outbox_resettle_ms, 0))

    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "owed")], 1)
    assert {"owed", _md, _} = next_output()

    ClassRegistry.delete(rc)
    assert {:error, _} = Flezha.Relay.Lookup.overwatch(rc)

    # The status call queues behind the tick, so it answers only once the tick ran.
    send(pid, :outbox_retry)
    assert %{awaiting: 1} = WalWriter.outbox_status(pid)
  end

  test "a new Overwatch gets what the killed one never ran before anything newer", %{
    rc: rc,
    cell_id: cell_id
  } do
    killed = spawn(fn -> Process.sleep(:infinity) end)
    ClassRegistry.put_overwatch(rc, killed, nil)
    pid = start_writer(cell_id)

    append_effect(pid, 0, [output("k1", "a")], 1)
    append_effect(pid, 1, [output("k1", "b")], 2)
    assert {:message_queue_len, 2} = Process.info(killed, :message_queue_len)

    ref = Process.monitor(killed)
    Process.exit(killed, :kill)
    assert_receive {:DOWN, ^ref, :process, ^killed, :killed}

    overwatch = %Overwatch{id: rc, builder: %Builder{id: rc, factory_id: "f", opts: []}}
    Lifecycle.bind_to_runner(overwatch, rc, "runner_b", self())
    append_effect(pid, 2, [output("k1", "c")], 3)

    assert {"a", _md, _ack} = next_output()
    assert {"b", _md, _ack} = next_output()
    assert {"c", _md, _ack} = next_output()
  end

  test "a held call is re-dispatched until its caller answers it; any other hold waits", %{
    rc: rc,
    cell_id: cell_id
  } do
    Application.put_env(:flezha, :outbox_resettle_ms, 0)
    on_exit(fn -> Application.delete_env(:flezha, :outbox_resettle_ms) end)

    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "call", [{"flz.call", "id:pricing"}])], 1)
    append_effect(pid, 1, [output("k", "sink")], 2)

    # The caller's Overwatch holds the call, a sink holds the other output.
    assert {"call", call_md, _} = next_output()
    assert {"sink", sink_md, _} = next_output()

    :ok = Outbox.hold_delivery(call_md)
    :ok = Outbox.hold(sink_md)
    Enum.each([call_md, sink_md], &settle/1)

    # A restarted Overwatch holds no row for the call, so the resettle hands
    # the call back with identical identity; the sink's hold is its own.
    send(pid, :outbox_retry)
    :ok = WalWriter.fsync(pid)
    assert {"call", again, _} = next_output()
    assert again["flz.seq"] == call_md["flz.seq"]
    refute_dispatched(0)

    :ok = Outbox.hold_delivery(again)
    settle(again)
    :ok = Outbox.confirm_delivery(again)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0} = WalWriter.outbox_status(pid)
  end

  test "a held answer is dispatched again only when its delivery reports it lost", %{
    rc: rc,
    cell_id: cell_id
  } do
    Application.put_env(:flezha, :outbox_resettle_ms, 0)
    on_exit(fn -> Application.delete_env(:flezha, :outbox_resettle_ms) end)

    pid = armed_writer(rc, cell_id)
    answer = [{"flz.reply_to", "o:gateway:c1:0:1:"}]
    append_effect(pid, 0, [output("k", "answer", answer)], 1)
    assert {"answer", md, _} = next_output()
    :ok = Outbox.hold_delivery(md)
    settle(md)

    # Its delivery worker retries it: the resettle sends no second copy.
    send(pid, :outbox_retry)
    :ok = WalWriter.fsync(pid)
    refute_dispatched(0)

    # The router lost the worker that held it: the answer goes again.
    :ok = WalWriter.redrive_answers(pid, "gateway")
    :ok = WalWriter.fsync(pid)
    assert {"answer", again, _} = next_output()
    assert again["flz.seq"] == md["flz.seq"]
  end

  test "an answer whose delivery confirmed is not re-dispatched while only a sink holds it", %{
    rc: rc,
    cell_id: cell_id
  } do
    on_exit(TestHelpers.override_env(:outbox_resettle_ms, 0))

    pid = armed_writer(rc, cell_id)
    answer = [{"flz.reply_to", "o:gateway:c1:0:1:"}]
    append_effect(pid, 0, [output("k", "answer", answer)], 1)
    assert {"answer", md, _} = output = next_output()

    # In one dispatcher run the Overwatch holds the answer for its delivery
    # and a sink in the `:on_output` chain holds the same output.
    run_batch([output], fn _payload, metadata ->
      :ok = Outbox.hold_delivery(metadata)
      :ok = Outbox.hold(metadata)
    end)

    # The delivery took the answer to its caller; the sink has not answered.
    :ok = Outbox.confirm_delivery(md)
    :ok = WalWriter.fsync(pid)
    assert %{awaiting: 1, outbox_flushed_seq: -1} = WalWriter.outbox_status(pid)

    send(pid, :outbox_retry)
    :ok = WalWriter.fsync(pid)
    refute_dispatched(0)

    # The sink's confirmation is the last one the entry waits on.
    :ok = Outbox.confirm(md)
    :ok = WalWriter.fsync(pid)
    assert %{awaiting: 0, outbox_flushed_seq: 0} = WalWriter.outbox_status(pid)
  end

  test "a second confirmation of one answer's delivery does not answer a sink's hold", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)
    answer = [{"flz.reply_to", "o:gateway:c1:0:1:"}]
    append_effect(pid, 0, [output("k", "answer", answer)], 1)
    assert {"answer", md, _} = output = next_output()

    # The delivery confirms one copy more than once, before and after the run
    # settles; the sink's hold is still open.
    :ok = Outbox.confirm_delivery(md)
    :ok = Outbox.confirm_delivery(md)

    run_batch([output], fn _payload, metadata ->
      :ok = Outbox.hold_delivery(metadata)
      :ok = Outbox.hold(metadata)
    end)

    :ok = Outbox.confirm_delivery(md)
    :ok = WalWriter.fsync(pid)
    assert %{awaiting: 1, outbox_flushed_seq: -1} = WalWriter.outbox_status(pid)

    :ok = Outbox.confirm(md)
    :ok = WalWriter.fsync(pid)
    assert %{awaiting: 0, outbox_flushed_seq: 0} = WalWriter.outbox_status(pid)
  end

  test "a superseded dispatch's delivery confirm does not answer the re-dispatch's sink hold", %{
    rc: rc,
    cell_id: cell_id
  } do
    on_exit(TestHelpers.override_env(:outbox_resettle_ms, 0))

    pid = armed_writer(rc, cell_id)
    answer = [{"flz.reply_to", "o:gateway:c1:0:1:"}]
    append_effect(pid, 0, [output("k", "answer", answer)], 1)
    assert {"answer", md, _} = output = next_output()

    # The first run: the delivery and a sink both hold the answer.
    run_batch([output], fn _payload, metadata ->
      :ok = Outbox.hold_delivery(metadata)
      :ok = Outbox.hold(metadata)
    end)

    # The router lost the delivery worker: the answer goes again.
    :ok = WalWriter.redrive_answers(pid, "gateway")
    :ok = WalWriter.fsync(pid)
    assert {"answer", again, _} = output_again = next_output()
    assert again["flz.seq"] == md["flz.seq"]

    # The superseded copy's delivery confirms while the re-dispatch is in flight.
    :ok = Outbox.confirm_delivery(md)
    :ok = WalWriter.fsync(pid)

    # Past its deadline the answer takes no delivery hold; the sink holds again.
    run_batch([output_again], fn _payload, metadata -> :ok = Outbox.hold(metadata) end)
    :ok = WalWriter.fsync(pid)

    # No sink has confirmed either copy: the cursor must not pass the entry.
    assert %{awaiting: 1, outbox_flushed_seq: -1} = WalWriter.outbox_status(pid)

    # The superseded copy's sink answers its own hold, not the re-dispatch's.
    :ok = Outbox.confirm(md)
    :ok = WalWriter.fsync(pid)
    assert %{awaiting: 1, outbox_flushed_seq: -1} = WalWriter.outbox_status(pid)

    :ok = Outbox.confirm(again)
    :ok = WalWriter.fsync(pid)
    assert %{awaiting: 0, outbox_flushed_seq: 0} = WalWriter.outbox_status(pid)
  end

  test "a later output of a relay-called input is not re-dispatched while a sink holds it", %{
    rc: rc,
    cell_id: cell_id
  } do
    on_exit(TestHelpers.override_env(:outbox_resettle_ms, 0))

    pid = armed_writer(rc, cell_id)
    token = [{"flz.reply_to", "o:gateway:c1:0:1:"}]
    append_effect(pid, 0, [output("k", "to-sink", token)], 1)
    assert {"to-sink", md, _} = next_output()

    # The call was answered by an earlier output: no delivery holds this one.
    :ok = Outbox.hold(md)
    settle(md)

    send(pid, :outbox_retry)
    :ok = WalWriter.fsync(pid)
    refute_dispatched(0)
  end

  test "asked for its held calls, a writer re-dispatches them at once and nothing else", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "call", [{"flz.call", "id:pricing"}])], 1)
    append_effect(pid, 1, [output("k", "sink")], 2)
    assert {"call", call_md, _} = next_output()
    assert {"sink", sink_md, _} = next_output()

    :ok = Outbox.hold_delivery(call_md)
    :ok = Outbox.hold(sink_md)
    Enum.each([call_md, sink_md], &settle/1)

    started = System.monotonic_time(:millisecond)
    assert WalWriter.redrive_calls(pid, started, Token.durable_id(call_md)) == 1
    assert {"call", again, _} = next_output()
    assert again["flz.seq"] == call_md["flz.seq"]
    refute_dispatched(0)
  end

  # ---------- batched reports ----------

  test "one settlement and one confirmation report complete a whole batch", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    in_one_window(fn ->
      for seq <- 0..9 do
        :ok =
          WalWriter.append(
            pid,
            seq,
            effect_header(),
            effect_body([output("k", "p#{seq}")], seq + 1)
          )
      end
    end)

    :ok = WalWriter.fsync(pid)

    outputs = assert_batch(2_000)
    assert length(outputs) == 10, "one dispatch pass must reach the chain as one batch"

    # One pass advances the watermark once, to the furthest offset it carried.
    assert InputLog.watermark(rc) == 10

    # The chain runs as the dispatcher does: every output held, the whole batch
    # settled in ONE report. Nothing confirmed yet, so no cursor may move.
    run_batch(outputs, fn _payload, metadata -> Outbox.hold(metadata) end)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: -1, awaiting: 10} = WalWriter.outbox_status(pid)

    # And ONE report from the sink completes all ten entries.
    :ok = Outbox.confirm_many(Enum.map(outputs, &metadata_of/1))
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: 9, awaiting: 0} = WalWriter.outbox_status(pid)
    assert Lineage.progress(cell_id).outbox_flushed_seq == 9
  end

  test "an out-of-order batch report cannot carry the cursor past an unconfirmed entry", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    append_effect(pid, 0, [output("k", "early")], 1)
    early = assert_batch(2_000)

    append_effect(pid, 1, [output("k", "late")], 2)
    late = assert_batch(2_000)

    for batch <- [early, late],
        do: run_batch(batch, fn _payload, metadata -> Outbox.hold(metadata) end)

    :ok = Outbox.confirm_many(Enum.map(late, &metadata_of/1))
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: -1} = WalWriter.outbox_status(pid),
           "the later batch's report must not move the cursor past the earlier entry"

    :ok = Outbox.confirm_many(Enum.map(early, &metadata_of/1))
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 1, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a crashed batch run settles none of its entries and the resettle re-drives them", %{
    rc: rc,
    cell_id: cell_id
  } do
    Application.put_env(:flezha, :outbox_resettle_ms, 50)
    on_exit(fn -> Application.delete_env(:flezha, :outbox_resettle_ms) end)

    pid = armed_writer(rc, cell_id)

    in_one_window(fn ->
      stage_effect(pid, 0, [output("k", "a")], 1)
      stage_effect(pid, 1, [output("k", "b")], 2)
    end)

    :ok = WalWriter.fsync(pid)
    outputs = assert_batch(2_000)
    assert length(outputs) == 2

    # The chain crashes partway through the batch. `run/3` reports only after
    # the whole batch has run, so neither entry is settled and the hold the
    # dead run took is never answered.
    capture_log(fn ->
      {:ok, runner} =
        Task.start(fn ->
          Outbox.run(outputs, :ok, fn payload, metadata, :ok ->
            :ok = Outbox.hold(metadata)
            if payload == "b", do: raise("chain crashed"), else: :ok
          end)
        end)

      ref = Process.monitor(runner)
      assert_receive {:DOWN, ^ref, :process, ^runner, _}, 1_000
    end)

    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: -1, awaiting: 2} = WalWriter.outbox_status(pid)

    # The resettle re-dispatches both under identical identity, in seq order.
    redriven = assert_batch(3_000)
    assert [{"a", %{"flz.seq" => "0.0"}, _}, {"b", %{"flz.seq" => "1.0"}, _}] = redriven

    run_batch(redriven)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 1, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a checkpoint record's outputs flush as one batch with sequential ordinals", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    # What an `outputs: :at_checkpoint` interval ships: ONE record carrying the
    # whole interval's outputs and the offset of the input that closed it.
    outputs = for n <- 0..299, do: output("k#{n}", "v#{n}")
    append_effect(pid, 0, outputs, 4_711)

    batch = assert_batch(2_000)

    assert length(batch) == 300,
           "the interval's outputs reach the chain as one batch, not 300 casts"

    assert Enum.map(batch, fn {_payload, md, _ack} -> md["flz.seq"] end) ==
             Enum.map(0..299, &"0.#{&1}"),
           "identities are `<record seq>.<ordinal>`, sequential across the batch"

    assert InputLog.watermark(rc) == 4_711,
           "the closing record acknowledges the interval's last input"

    # One settlement report and one confirmation complete the whole interval.
    run_batch(batch, fn _payload, metadata -> Outbox.hold(metadata) end)
    :ok = Outbox.confirm_many(Enum.map(batch, &metadata_of/1))
    :ok = WalWriter.fsync(pid)

    assert %{outbox_flushed_seq: 0, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a re-drive re-sends a checkpoint record's outputs under identical identity", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    :ok =
      WalWriter.append(
        pid,
        0,
        effect_header(),
        effect_body([output("k", "a"), output("k", "b")], 9)
      )

    :ok = WalWriter.fsync(pid)

    first = assert_batch(2_000)
    run_batch(first)
    close_writer(pid, cell_id)

    # Crash before the cursor write: the re-drive owes the whole record again.
    pid = reopen_with_stale_cursor(cell_id)

    again = assert_batch(3_000)

    assert Enum.map(again, fn {payload, md, _} -> {payload, md["flz.seq"], md["flz.epoch"]} end) ==
             Enum.map(first, fn {payload, md, _} -> {payload, md["flz.seq"], md["flz.epoch"]} end),
           "a re-driven record carries byte-identical identity, which is what sink dedup absorbs"

    :ok = WalWriter.close(pid)
  end

  test "a batch report naming entries the writer no longer owes changes nothing", %{
    rc: rc,
    cell_id: cell_id
  } do
    pid = armed_writer(rc, cell_id)

    append_effect(pid, 0, [output("k", "done")], 1)

    outputs = assert_batch(2_000)
    run_batch(outputs, fn _payload, metadata -> Outbox.hold(metadata) end)
    :ok = Outbox.confirm_many(Enum.map(outputs, &metadata_of/1))
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0, awaiting: 0} = WalWriter.outbox_status(pid)

    # A superseded run answering late: every key it names is gone from the
    # ledger, so both batch reports are dropped per key and nothing moves.
    :ok = Outbox.confirm_many(Enum.map(outputs, &metadata_of/1))

    GenServer.cast(
      pid,
      {:outbox_settled_many, Enum.map(outputs, fn {_payload, _md, %{key: key}} -> {key, 1} end)}
    )

    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0, awaiting: 0} = WalWriter.outbox_status(pid)
  end

  test "a re-drive of held calls leaves a call whose chain run is still in flight", %{
    rc: rc,
    cell_id: cell_id
  } do
    started = System.monotonic_time(:millisecond) - 1
    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "call", [{"flz.call", "id:pricing"}])], 1)
    assert {"call", call_md, _} = next_output()

    # The live Overwatch has not run this entry's chain yet: the call stays
    # owed to it, so an answer for it waits, and it does not go again.
    assert WalWriter.redrive_calls(pid, started, Token.durable_id(call_md)) == 1
    refute_dispatched(0)

    # The one run reports: the live Overwatch holds the call, so nothing waits.
    :ok = Outbox.hold_delivery(call_md)
    settle(call_md)
    assert WalWriter.redrive_calls(pid, started, Token.durable_id(call_md)) == 0
    refute_dispatched(0)

    # The caller answers it: the cursor passes the entry.
    :ok = Outbox.confirm_delivery(call_md)
    :ok = WalWriter.fsync(pid)
    assert %{outbox_flushed_seq: 0} = WalWriter.outbox_status(pid)
  end

  test "an answer for no call this writer holds is not owed by an unrelated call in flight", %{
    rc: rc,
    cell_id: cell_id
  } do
    started = System.monotonic_time(:millisecond) - 1
    pid = armed_writer(rc, cell_id)
    append_effect(pid, 0, [output("k", "call", [{"flz.call", "id:pricing"}])], 1)
    assert {"call", _call_md, _} = next_output()

    # A stray answer (its call is over) must be absorbed, not retried.
    assert WalWriter.redrive_calls(pid, started, Token.durable_id(%{})) == 0
    refute_dispatched(0)
  end

  test "a held answer goes again at most once while its caller stays away", %{
    rc: rc,
    cell_id: cell_id
  } do
    # fails when: every resettle tick copies a held answer again while its
    # delivery worker is alive, so a caller that is away finds one copy per tick.
    on_exit(TestHelpers.override_env(:outbox_resettle_ms, 0))

    pid = armed_writer(rc, cell_id)
    answer = [{"flz.reply_to", "o:gateway:c1:0:1:"}]
    append_effect(pid, 0, [output("k", "answer", answer)], 1)
    assert {"answer", md, _} = next_output()
    :ok = Outbox.hold_delivery(md)
    settle(md)

    # Each copy is taken by the same live delivery, which queues it for a caller
    # that is away; three ticks pass.
    copies =
      for _tick <- 1..3, reduce: 0 do
        count ->
          send(pid, :outbox_retry)
          :ok = WalWriter.fsync(pid)

          receive do
            {:"$gen_cast", {:io_out_batch, [{"answer", again, _}]}} ->
              :ok = Outbox.hold_delivery(again)
              settle(again)
              count + 1
          after
            100 -> count
          end
      end

    assert copies <= 1
  end
end
