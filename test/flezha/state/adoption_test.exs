defmodule FlezhaTest.State.AdoptionTest do
  @moduledoc """
  An adoption a crash interrupted: a copy still set aside comes back whole at
  the next arrival, and one already discarded is finished removing, never
  brought back over the newer copy.
  """
  # async: false: writes Application env; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic

  alias Flezha.State.{Adoption, Paths, TestHelpers}
  alias Flezha.State.DurabilityTier.Artifact

  setup do
    class = "aside_#{System.unique_integer([:positive])}"
    cell_id = {class, "task", "k"}
    TestHelpers.write_wal(cell_id, 0..3, "old")
    old = File.read!(Paths.wal_path(cell_id))
    File.rename!(Paths.class_dir(class), Paths.set_aside_dir(class))
    %{class: class, cell_id: cell_id, old: old}
  end

  # fails when: a set-aside copy reads as an operator class, so the retention
  # sweep or the durability tier's backstop treats it as one.
  test "a copy set aside is not an operator class", ctx do
    refute Enum.any?(Paths.operator_classes(), &String.starts_with?(&1, ctx.class))
  end

  # fails when: a copy set aside is never read back, so the class binds with
  # no state, or with the part of a newer copy that landed, while its only
  # complete copy sits beside it.
  test "the next arrival brings back the copy a crash left set aside", ctx do
    TestHelpers.write_wal({ctx.class, "task", "partial"}, 0..1, "landed")

    assert :ok = Adoption.adopt_newest(ctx.class, nil)

    assert File.read!(Paths.wal_path(ctx.cell_id)) == ctx.old
    refute File.exists?(Paths.wal_path({ctx.class, "task", "partial"}))
    refute File.exists?(Paths.set_aside_dir(ctx.class))
  end

  # fails when: a crash after the newer copy landed puts the stale copy back,
  # or leaves the half-removed one beside the class.
  test "a copy discarded before a crash stays discarded", ctx do
    discarded = Paths.class_dir(ctx.class) <> ".discarded"
    File.rename!(Paths.set_aside_dir(ctx.class), discarded)
    TestHelpers.write_wal(ctx.cell_id, 0..5, "newer")
    newer = File.read!(Paths.wal_path(ctx.cell_id))
    refute Enum.any?(Paths.operator_classes(), &String.starts_with?(&1, ctx.class <> "."))

    assert :ok = Adoption.adopt_newest(ctx.class, nil)

    assert File.read!(Paths.wal_path(ctx.cell_id)) == newer
    refute File.exists?(discarded)
  end

  # fails when: with no source and no backend the local copy's WALs are read
  # anyway, on every plain restart.
  test "with nothing to compare against no local copy is read", ctx do
    reject(&Artifact.freshness_of/1)

    assert :ok = Adoption.adopt_newest(ctx.class, nil)
  end

  # fails when: a crash while the newer copy's missing files are copied back
  # puts the stale copy back over the newer one that had landed.
  test "a crash during the copy-back finishes it and keeps the newer copy", ctx do
    live = Paths.class_dir(ctx.class)
    File.rename!(Paths.set_aside_dir(ctx.class), live)
    carl = {ctx.class, "task", "carl"}
    TestHelpers.write_wal(carl, 0..1, "only_here")
    carl_bytes = File.read!(Paths.wal_path(carl))
    File.write!(Path.join(live, "kind"), "steps")
    kept = Enum.map([Paths.wal_path(carl), Path.join(live, "kind")], &Path.relative_to(&1, live))
    :ok = Paths.set_aside(ctx.class)
    TestHelpers.write_wal(ctx.cell_id, 0..5, "newer")
    newer = File.read!(Paths.wal_path(ctx.cell_id))
    :ok = Paths.mark_landed(ctx.class, kept)
    File.cp!(Path.join(Paths.set_aside_dir(ctx.class), "kind"), Path.join(live, "kind"))

    assert :ok = Adoption.adopt_newest(ctx.class, nil)

    assert File.read!(Paths.wal_path(ctx.cell_id)) == newer
    assert File.read!(Paths.wal_path(carl)) == carl_bytes
    assert File.read!(Path.join(live, "kind")) == "steps"
    refute File.exists?(Paths.set_aside_dir(ctx.class))
    assert Paths.landed_kept(ctx.class) == :none
  end
end
