defmodule FlezhaTest.System.DurableFrameE2ETest do
  @moduledoc """
  The durable record frame, over a WAL a real runner wrote.

  Every other test of the frame hands it bytes a test built. This one boots a
  runner subprocess, drives real inputs through it, and folds the file that
  comes out — which is the only place the encoder, the wire, the writer and the
  restore are all the live ones. The rows are what a restore meets on disk:

    * **clean** — the two walks over the same file agree, record for record,
      and the seqs are the ones the run produced;
    * **torn tail** — a crash mid-append leaves a partial record; the log is
      repaired to the last whole one and everything before it survives;
    * **corrupt record** — a flipped bit mid-file is refused by name and
      counted, and the records after it are still on disk afterwards.

  Two rows are covered end to end elsewhere and are not repeated here: a
  restore across a schema migration is `schema_evolution_e2e_test.exs`, and a
  foreign `sdk_lang` on disk is `cross_sdk_placement_e2e_test.exs`.

  Both runtimes run this drill; which one is `FLEZHA_E2E_RUNTIME`. Tagged
  `:system` — boots a real subprocess. Run via:

      mix test --include system test/system/durable_frame_e2e_test.exs
  """

  use FlezhaTest.SystemCase, capability: :state_persistence

  import ExUnit.CaptureLog

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.RecordFrame, as: Frame
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Paths
  alias Flezha.State.RecordFrame
  alias Flezha.State.Restore
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.CompactionAssertions, as: CA
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "durable_frame_e2e"
  @factory "store_input"
  @tasklet "store_input_tasklet"

  setup do
    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(@namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  @tag timeout: 300_000
  test "the frame a runner writes is the frame every reader folds" do
    {cell_id, path} = drive_a_wal(10)

    # Row: clean. The byte walk and the device walk over the SAME file, which is
    # the substitutability the seam exists to make testable.
    bytes = File.read!(path)

    assert {:ok, from_bytes} =
             Frame.fold_bytes(bytes, {:log, path}, [], &collect/2)

    assert {:ok, from_device} =
             RecordFrame.fold_path(path, [], fn {seq, header, body}, acc ->
               {:cont, [{seq, header, body} | acc]}
             end)

    assert from_bytes != [], "the runner wrote no records"
    assert Enum.reverse(from_bytes) == Enum.reverse(from_device)

    seqs = from_bytes |> Enum.map(&elem(&1, 0)) |> Enum.reverse()
    assert seqs == Enum.sort(seqs), "the records are not in seq order on disk"
    assert seqs == Enum.uniq(seqs), "a seq was written twice"

    # The record-before-output rule: `flz.seq` is the causal link, so the
    # assertion is on the sequence, never on arrival order
    # (`pin-change-before-output-ordering`).
    assert hd(seqs) == 0, "the first durable record is not seq 0"

    # Every header on disk decodes through the one header decoder, and every one
    # of them names an allocated language.
    for {_seq, header, _body} <- from_bytes do
      assert {:ok, decoded} = Frame.decode_header(header)
      assert Frame.sdk_lang_name(decoded.sdk_lang) != "unallocated"
    end

    assert {:ok, payload} = Restore.assemble_payload(cell_id)
    assert byte_size(payload.change_log_bytes) > 0
    assert payload.terminal_seq_hint == List.last(seqs)
  end

  @tag timeout: 300_000
  test "a torn tail is repaired and a corrupt record is refused, with the file left on disk" do
    {cell_id, path} = drive_a_wal(10)

    # Read the live file first, then stop: the writer holds the handle, so the
    # bytes are taken while they are being appended and trimmed to the last
    # whole record. Everything after this point is the test's own file.
    whole = complete_prefix(File.read!(path))
    :ok = OperativeManager.stop_all_operatives(@namespace)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, whole)

    offsets = TestHelpers.record_offsets(path)
    assert length(offsets) >= 4, "the drill needs several records to corrupt a middle one"

    # Row: torn tail. Half a record appended is the append protocol's own crash;
    # the records before it are complete and must survive.
    File.write!(path, whole <> binary_part(whole, 0, 20))

    assert {:ok, repaired} = Restore.assemble_payload(cell_id)
    assert byte_size(repaired.change_log_bytes) == byte_size(whole)

    # Row: corrupt record. One flipped bit in a MIDDLE record's body: the
    # framing still locates every record after it, so the log cannot be passed
    # off as one that ended there.
    File.write!(path, whole)
    mid = Enum.at(offsets, div(length(offsets), 2))
    :ok = TestHelpers.corrupt_record_body(path, mid)
    size_before = byte_size(File.read!(path))

    parent = self()
    handler = {__MODULE__, :refused, System.unique_integer([:positive])}

    :telemetry.attach(
      handler,
      [:flezha, :state, :record_frame, :refused],
      fn _e, measurements, meta, _ -> send(parent, {:refused, measurements, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    log =
      capture_log(fn ->
        assert {:error, reason} = Restore.assemble_payload(cell_id)
        assert reason =~ "durable record refused (crc_mismatch)"
        assert reason =~ path
      end)

    assert log =~ "durable record refused (crc_mismatch)"
    assert_receive {:refused, %{count: 1}, %{reason: :crc_mismatch, path: ^path}}, 5_000

    # The bytes are still there: a refusal leaves the log alone, which is the
    # whole difference between refusing and repairing.
    assert byte_size(File.read!(path)) == size_before
  end

  defp collect({seq, header, body, _raw}, acc), do: {:cont, [{seq, header, body} | acc]}

  # The whole records in `bytes`: a file read while its writer appends can end
  # mid-record, and the drill's corruption rows need a log that is intact
  # before they damage it.
  defp complete_prefix(bytes) do
    {:ok, consumed} =
      Frame.fold_bytes(bytes, {:log, "live_wal"}, 0, fn {_s, _h, _b, raw}, n ->
        {:cont, n + byte_size(raw)}
      end)

    binary_part(bytes, 0, consumed)
  end

  # Boot a runner, register a changeful operative, push `n` inputs and wait for
  # its WAL to hold bytes. Answers the Cell and its WAL path.
  defp drive_a_wal(n) do
    {port, ce_id} = NodeTestHelper.start_runner_port(@namespace)
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "durable_frame_e2e_#{System.unique_integer([:positive])}"
    cell_id = {operative_id, @tasklet, ""}

    builder = %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{},
      # No periodic capture: a snapshot would truncate the very records the
      # drill folds.
      opts: [snapshot_interval: 0]
    }

    assert {:ok, info} = OperativeManager.start_operative(@namespace, builder)
    assert info.runner_id == ce_id

    for i <- 1..n do
      OperativeManager.send_io(operative_id, @namespace, "line-#{i}-#{operative_id}", %{
        "input_type" => "2"
      })
    end

    assert CA.wait_for_wal_bytes(cell_id, 10_000) > 0

    {cell_id, Paths.wal_path(cell_id)}
  end
end
