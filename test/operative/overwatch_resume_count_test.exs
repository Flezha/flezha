defmodule FlezhaTest.Operative.OverwatchResumeCountTest do
  @moduledoc """
  §5b.2 crash-resume gate (controller half): `Overwatch.resume_segment_count/2`
  is the number of persisted Segments the bind-time restore path will ship a
  `RestoreInitState` for, threaded onto `AddOperative.resume_segment_count` so a
  durable bounded driver holds its first Operation until that many restores land.

  Non-zero only for a durable `:bounded` class with WAL on disk; `0` for a fresh
  spawn, an unbounded operative, or an opted-out bounded one.
  """
  use ExUnit.Case, async: false

  alias Flezha.Operative.Overwatch
  alias Flezha.State.{Registry, TestHelpers}

  setup do
    # A fresh tmp state_root isolates each case: `all_segments/0` +
    # `has_persisted_state?/1` then see only the Segments this test seeds.
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :operative_persistence)
      File.rm_rf(root)
    end)

    :ok
  end

  # Drop a minimal WAL file on disk for a Segment so `all_segments/0` enumerates
  # it and `has_persisted_state?/1` returns true (as after a real durable run).
  defp seed_segment(operative_class, operation_id, segment_key) do
    segment_id = {operative_class, operation_id, segment_key}
    path = Registry.wal_path(segment_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, TestHelpers.build_wal_record(1, <<>>))
    segment_id
  end

  test "a durable bounded operative counts its persisted Segments" do
    seed_segment("resume_class", "op", "even")
    seed_segment("resume_class", "op", "odd")

    assert Overwatch.resume_segment_count(:bounded, "resume_class") == 2
  end

  test "a fresh durable bounded operative (no WAL) waits for nothing" do
    assert Overwatch.resume_segment_count(:bounded, "unseeded_class") == 0
  end

  test "an unbounded operative never waits for a bounded-style restore gate" do
    seed_segment("cont_class", "op", "s")

    assert Overwatch.resume_segment_count(:unbounded, "cont_class") == 0
  end

  test "an opted-out bounded class counts zero even with WAL on disk" do
    Application.put_env(:flezha, :operative_persistence, %{"optout_class" => false})
    seed_segment("optout_class", "op", "s")

    assert Overwatch.resume_segment_count(:bounded, "optout_class") == 0
  end
end
