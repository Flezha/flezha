defmodule FlezhaTest.Helper.CompactionAssertions do
  @moduledoc """
  Shared observable-durable-state helpers for the compaction e2e tests
  (`test/system/compaction_e2e_test.exs`, `test/java_sdk/compaction_e2e_test.exs`):
  measure a Segment's on-disk WAL bytes and snapshot files, and wait for the
  compaction outcome (a snapshot file appears, the WAL shrinks). Runtime-agnostic
  — the controller-side durable state is identical whichever SDK ships the frame.

  Waits reuse `NodeTestHelper.wait_until/2` (the shared e2e poll loop);
  port teardown is `NodeTestHelper.ensure_port_dead/1`.
  """

  alias FlezhaTest.Helper.NodeTestHelper

  @doc "Total bytes across the Segment's `*.wal` files."
  def wal_bytes(segment_id) do
    Flezha.State.Registry.state_dir(segment_id)
    |> Path.join("*.wal")
    |> Path.wildcard()
    |> Enum.map(fn path ->
      case File.stat(path) do
        {:ok, %{size: size}} -> size
        _ -> 0
      end
    end)
    |> Enum.sum()
  end

  @doc "The Segment's snapshot files as `{seq, path}` (empty when none written)."
  def snapshot_files(segment_id), do: Flezha.State.Registry.snapshot_paths(segment_id)

  @doc "Wait until the Segment's WAL holds bytes, returning them; `0` on timeout."
  def wait_for_wal_bytes(segment_id, timeout_ms) do
    NodeTestHelper.wait_until(
      fn ->
        bytes = wal_bytes(segment_id)
        if bytes > 0, do: bytes, else: false
      end,
      timeout_ms
    ) || 0
  end

  @doc "Wait until a snapshot file exists; `false` on timeout."
  def wait_for_snapshot_file(segment_id, timeout_ms) do
    NodeTestHelper.wait_until(fn -> snapshot_files(segment_id) != [] end, timeout_ms) || false
  end

  @doc "Wait until the WAL is smaller than `before`, returning the size; the last measured size on timeout."
  def wait_for_wal_shrink(segment_id, before, timeout_ms) do
    NodeTestHelper.wait_until(
      fn ->
        bytes = wal_bytes(segment_id)
        if bytes < before, do: bytes, else: false
      end,
      timeout_ms
    ) || wal_bytes(segment_id)
  end
end
