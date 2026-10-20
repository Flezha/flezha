defmodule Flezha.State.RemoteFlowGatedFloorTest do
  @moduledoc """
  What a `durable: remote` flow Cell reports to its class's input-log floor
  when its writer boots, or when a sleeping class seeds its rows from disk:
  the highest consumed offset its WAL records at or below its persisted flush
  cursor carry, never one above it.
  """
  use FlezhaTest.OutboxCase, async: false

  alias Flezha.State.DurabilityTier.{Manifest, Shipper}
  alias Flezha.State.InputLog.Sweep
  alias Flezha.State.{RecordFrame, Retention, Snapshot}
  alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub

  setup %{rc: rc} do
    Stub.install()
    on_exit(&Stub.uninstall/0)
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))
    start_supervised!(Shipper)
    # The test process is the relay's Overwatch: its bind adopted it here.
    Shipper.adopt(rc)
    :ok = Shipper.sweep_now()
    :ok = ClassRegistry.put_remote(rc, true)
    :ok
  end

  test "a gated Cell's unconsumed inputs survive a sweep while its boot ship fails", ctx do
    a = {ctx.rc, "task", "kA"}
    b = {ctx.rc, "task", "kB"}
    on_exit(fn -> close_all([a, b]) end)

    for n <- 0..5, do: {:ok, ^n} = InputLog.append(ctx.rc, "in#{n}", %{})

    # Before the restart: A consumed through 1, B through 3, both shipped.
    wa = armed_writer(ctx.rc, a)
    wb = start_writer(b)
    append_effect(wa, 0, [output("kA", "a0")], 1)
    delivered("a0")
    append_effect(wb, 0, [output("kB", "b0")], 3)
    delivered("b0")
    assert wait_until(fn -> InputLog.watermark(ctx.rc) == 1 end, 5_000)

    # A consumes input 2 on this disk; its ship is refused from now on.
    supersede(a)
    append_effect(wa, 1, [output("kA", "a1")], 2)
    assert wait_until(fn -> refused?({:cell, a}) end, 5_000)
    close_writer(wa, a)
    close_writer(wb, b)

    # The restart: every watermark row of the class lived in memory.
    :ets.match_delete(InputLog.watermark_table(), {{ctx.rc, :_}, :_})

    # A boots with record 1 to re-drive; its boot ship keeps failing.
    wa = start_writer(a)
    _ = WalWriter.last_persisted_seq(wa)
    assert {a, 1} in InputLog.cell_offsets(ctx.rc)

    # B boots fully flushed, then consumes input 5 and ships it.
    wb = start_writer(b)
    append_effect(wb, 1, [output("kB", "b1")], 5)
    delivered("b1")
    assert wait_until(fn -> {b, 5} in InputLog.cell_offsets(ctx.rc) end, 5_000)
    assert refused?({:cell, a})
    assert {a, 1} in InputLog.cell_offsets(ctx.rc)
    assert InputLog.watermark(ctx.rc) == 1

    # A restore of A from its remote copy replays from input 2.
    :ok = Sweep.run(ctx.rc)
    kept = ctx.rc |> InputLog.read_range(0, 5) |> Enum.map(&elem(&1, 0))
    assert Enum.all?(2..5, &(&1 in kept)), "inputs A still needs were cut: kept #{inspect(kept)}"
  end

  test "a gated sleeper's Cell seeds only what its remote copy holds", ctx do
    a = {ctx.rc, "task", "kA"}
    on_exit(fn -> close_all([a]) end)

    wa = armed_writer(ctx.rc, a)
    append_effect(wa, 0, [output("kA", "a0")], 1)
    delivered("a0")

    supersede(a)
    append_effect(wa, 1, [output("kA", "a1")], 2)
    assert wait_until(fn -> refused?({:cell, a}) end, 5_000)
    close_writer(wa, a)

    assert WalWriter.consumed_offset_on_disk(a) == 1
  end

  # fails when: a cut with no writer folds the offsets of dropped records above
  # the persisted flush cursor, which the remote copy may not hold.
  test "a writer-less gated Cell's cut folds only through its flush cursor", ctx do
    a = {ctx.rc, "task", "kA"}
    offsets = [10, 15, 20, 25, 28, 30, 35, 41]

    wal =
      for {offset, seq} <- Enum.with_index(offsets),
          into: "",
          do: Frame.encode_record(seq, effect_header(), effect_body([], offset))

    File.mkdir_p!(Path.dirname(Paths.wal_path(a)))
    File.write!(Paths.wal_path(a), wal)
    :ok = Lineage.put_dedup(a, %{last_persisted_seq: 7, outbox_flushed_seq: 5, epoch: 0})
    {:ok, _} = Snapshot.write_snapshot(a, 7, TestHelpers.stub_header(), "snap@7")

    :ok = Retention.sweep_cell(a, 0)

    assert Lineage.progress(a).folded_offset == 30
    assert RecordFrame.file_size(Paths.wal_path(a)) == 0
  end

  defp close_all(cells) do
    for cell <- cells, do: with({:ok, w} <- Supervisor.lookup(cell), do: WalWriter.close(w))
  end

  # The chain ran the output: the Cell's flush cursor moves past its record.
  defp delivered(payload) do
    assert {^payload, md, _} = next_output(5_000)
    settle(md)
  end

  # Another controller now holds the Cell's lineage, so every later ship of it
  # is refused while other Cells' ships land (the stub's own failure switches
  # are global).
  defp supersede(cell_id) do
    peer = %{Manifest.new({:cell, cell_id}) | owner: "peer", claim: 1_000}
    key = Manifest.key({:cell, cell_id})
    :ets.insert(:flezha_durability_stub, {key, Manifest.encode(peer), 1_000, 0})
  end

  defp refused?(ref) do
    case Enum.find(Shipper.status(), &(&1.ref == ref)) do
      %{superseded_by: "peer"} -> true
      %{failures: n} -> n > 0
      nil -> false
    end
  end
end
