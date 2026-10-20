defmodule FlezhaTest.State.AdoptionTest do
  @moduledoc """
  An adoption a crash interrupted: a copy still set aside comes back whole at
  the next arrival, and one already discarded is finished removing, never
  brought back over the newer copy.
  """
  # async: false: writes Application env; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic

  alias Flezha.State.{Adoption, Lineage, Paths, TestHelpers}
  alias Flezha.State.DurabilityTier.Artifact

  setup do
    class = "aside_#{System.unique_integer([:positive])}"
    cell_id = {class, "task", "k"}
    TestHelpers.write_wal(cell_id, 0..3, "old")
    :ok = Lineage.put(cell_id, %{Lineage.mint(Lineage.read(cell_id).record, -1) | epoch: 0})
    old = File.read!(Paths.wal_path(cell_id))
    lineage = File.read!(Paths.lineage_path(cell_id))
    File.rename!(Paths.class_dir(class), Paths.set_aside_dir(class))
    %{class: class, cell_id: cell_id, old: old, lineage: lineage}
  end

  # fails when: a set-aside copy reads as a relay class, so the retention
  # sweep or the durability tier's backstop treats it as one.
  test "a copy set aside is not a relay class", ctx do
    refute Enum.any?(Paths.relay_classes(), &String.starts_with?(&1, ctx.class))
  end

  # fails when: a copy set aside is never read back, so the class binds with
  # no state, or with the part of a newer copy that landed, while its only
  # complete copy sits beside it.
  test "the next arrival brings back the copy a crash left set aside", ctx do
    TestHelpers.write_wal({ctx.class, "task", "partial"}, 0..1, "landed")

    assert :ok = Adoption.adopt_newest(ctx.class, nil)

    assert File.read!(Paths.wal_path(ctx.cell_id)) == ctx.old
    assert File.read!(Paths.lineage_path(ctx.cell_id)) == ctx.lineage
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
    refute Enum.any?(Paths.relay_classes(), &String.starts_with?(&1, ctx.class <> "."))

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

  describe "comparing two copies of one relay" do
    @ref {:cell, {"cls", "task", "k"}}

    # fails when: a copy with no lineage, or the same one, is compared by
    # anything but its epoch and length.
    test "with no lineage, or the same one, the newer copy wins and a tie goes to it" do
      for {theirs, ours} <- [
            {%{}, %{}},
            {lineage("a"), %{@ref => nil}},
            {lineage("a"), lineage("a")}
          ] do
        assert supersedes?({0, 5}, {0, 3}, theirs, ours)
        assert supersedes?({0, 3}, {0, 3}, theirs, ours)
        refute supersedes?({0, 3}, {0, 5}, theirs, ours)
      end
    end

    # fails when: a longer copy the relay abandoned keeps its turns over the
    # copy that continued it.
    test "a copy that continues ours wins whatever its length" do
      assert supersedes?({0, 4}, {0, 7}, lineage("b", [{"a", 3}]), lineage("a"))
    end

    # fails when: a copy ours continues replaces ours though it took nothing
    # past the seq we forked from.
    test "a copy ours continues wins only once it moved past our fork" do
      ours = lineage("b", [{"a", 3}])
      refute supersedes?({0, 3}, {0, 5}, lineage("a"), ours)
      assert supersedes?({0, 4}, {0, 5}, lineage("a"), ours)
    end

    test "a copy of an unrelated lineage wins whatever its length" do
      assert supersedes?({0, 2}, {0, 9}, lineage("x"), lineage("a"))
    end

    # fails when: the backend's id two takeovers back is not found in our
    # trail, so its shorter copy replaces the one that continued it.
    test "after two takeovers whose claims never landed the local copy is kept" do
      refute supersedes?({0, 3}, {0, 6}, lineage("a"), lineage("c", [{"b", 5}, {"a", 3}]))
    end

    test "a hydration that took no record keeps the source's id, so the longer copy wins" do
      refute supersedes?({0, 3}, {0, 7}, lineage("a"), lineage("a"))
      assert supersedes?({0, 7}, {0, 3}, lineage("a"), lineage("a"))
    end

    # fails when: a peer without lineage records is not compared as before, or
    # this build's freshness stops being the 2-tuple an older build reads.
    test "a peer of an older build compares as before, both ways" do
      assert supersedes?({0, 5}, {0, 3}, %{}, lineage("a"))
      refute supersedes?({0, 3}, {0, 5}, %{}, lineage("a"))

      class = "mixed_#{System.unique_integer([:positive])}"
      TestHelpers.write_wal({class, "task", "k"}, 0..3)
      ours = Artifact.freshness_of(class)

      assert copy_supersedes_before_lineage?(
               ours,
               Map.new(ours, fn {ref, _} -> {ref, {0, 2}} end)
             )

      refute copy_supersedes_before_lineage?(
               Map.new(ours, fn {ref, _} -> {ref, {0, 2}} end),
               ours
             )
    end
  end

  defp lineage(id, trail \\ []), do: %{@ref => %{id: id, trail: trail}}

  defp supersedes?(theirs, ours, their_lineage, our_lineage),
    do: Artifact.copy_supersedes?(%{@ref => theirs}, %{@ref => ours}, their_lineage, our_lineage)

  # `Artifact.copy_supersedes?/2` as a build without lineage records has it.
  defp copy_supersedes_before_lineage?(theirs, ours) do
    shared = Map.take(ours, Map.keys(theirs))

    theirs != %{} and Enum.all?(Map.values(theirs), &match?({_, _}, &1)) and
      (ours == %{} or shared != %{}) and
      Enum.all?(shared, fn {ref, mine} -> at_least?(Map.fetch!(theirs, ref), mine) end)
  end

  defp at_least?({_, _} = theirs, {_, _} = ours), do: theirs >= ours
  defp at_least?(_theirs, _ours), do: false
end
