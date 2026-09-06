defmodule Flezha.State.DurableDeclarationTest do
  @moduledoc """
  The seam tests: a second frame walker, a hand-rolled install or a durable file
  the declaration does not know about fails here, by name.

  These read the source rather than the behaviour on purpose. The behaviour
  tests prove the seam does what it says; these prove nothing went around it,
  which is the only assertion that stays true as the tree grows.
  """
  use ExUnit.Case, async: true

  alias Flezha.State.Durable

  # The record frame lives in one module. `wire.ex` is the second file allowed
  # to carry the shape because it frames a DIFFERENT thing: the wire envelope
  # carries an entity id and crcs the body alone.
  @frame_allowed ["lib/flezha/state/record_frame.ex", "lib/flezha/state/wire.ex"]
  @frame_roots ["lib/flezha/state", "lib/flezha/sdk"]

  # An in-progress suffix is spelled in the declaration and nowhere else.
  # `lib/flezha/sdk/` is out of scope with a reason, not by omission: the Elixir
  # SDK is its own core and the packaging boundary
  # (`scripts/check-sdk-boundary.sh`) refuses it `Flezha.State.Paths`, so its
  # runner-id file keeps an install of its own.
  @suffix_allowed ["lib/flezha/state/durable.ex"]
  @suffix_roots ["lib/flezha"]
  @suffix_skipped "lib/flezha/sdk/"

  defp sources(roots) do
    roots
    |> Enum.flat_map(&Path.wildcard(Path.join(&1, "**/*.ex")))
    |> Enum.uniq()
    |> Enum.sort()
  end

  describe "the declaration and the tree" do
    test "every declared durable file is in the tree" do
      for {id, row} <- Durable.all() do
        assert File.exists?(row.file), "#{id} declares #{row.file}, which is not in the tree"
      end
    end

    test "every whole-file row is wired to the ceremony from its own file" do
      # A row nothing passes to `atomic_install/3` is a decorative declaration:
      # the suffix it names would never be worn and the fsync it declares would
      # never be owed.
      for {id, row} <- Durable.all(), row.frame == :whole do
        source = File.read!(row.file)

        assert source =~ "atomic_install(",
               "#{id} is declared a whole-file install but #{row.file} never calls the ceremony"

        assert source =~ ":#{id}",
               "#{id} is declared but #{row.file} passes no such id to the ceremony"
      end
    end

    test "every record-log row is folded by the frame seam" do
      for {id, row} <- Durable.all(), row.frame == :record do
        source = File.read!(row.file)

        assert source =~ "RecordFrame" or source =~ "RecordLog",
               "#{id} is declared a record log but #{row.file} names no record reader"
      end
    end

    test "the derived exclusion recognises every declared suffix and nothing else" do
      for {id, row} <- Durable.all() do
        assert Durable.in_progress?("entity.wal" <> row.in_progress_suffix), "#{id}"
      end

      refute Durable.in_progress?("entity.wal")
      refute Durable.in_progress?("entity.snap.7")
      refute Durable.in_progress?("object.claim")
    end
  end

  describe "a second reader cannot be added quietly" do
    test "the record framing is decomposed in one module" do
      offenders =
        for path <- sources(@frame_roots),
            path not in @frame_allowed,
            source = File.read!(path),
            source =~ "seq::64-big" and source =~ "len::32-big",
            do: path

      assert offenders == [],
             "these files decompose the durable record framing outside the seam: " <>
               Enum.join(offenders, ", ")
    end

    test "the framing crc32 is computed in one expression" do
      offenders =
        for path <- sources(@frame_roots),
            path not in @frame_allowed,
            File.read!(path) =~ ":erlang.crc32",
            do: path

      assert offenders == [],
             "these files compute a crc32 outside the frame seam: " <> Enum.join(offenders, ", ")
    end

    test "the header size is restated nowhere" do
      offenders =
        for path <- sources(@frame_roots),
            path != "lib/flezha/state/record_frame.ex",
            File.read!(path) =~ ~r/@header_size\s+\d/,
            do: path

      assert offenders == [],
             "these files restate the header's byte length: " <> Enum.join(offenders, ", ")
    end

    test "an in-progress suffix is spelled in the declaration and nowhere else" do
      offenders =
        for path <- sources(@suffix_roots),
            path not in @suffix_allowed,
            not String.starts_with?(path, @suffix_skipped),
            File.read!(path) =~ ~r/"\.(tmp|rewriting|transferring)/,
            do: path

      assert offenders == [],
             "these files build or match an in-progress suffix by hand: " <>
               Enum.join(offenders, ", ")
    end
  end
end
