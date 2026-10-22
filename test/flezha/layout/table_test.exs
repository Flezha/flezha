defmodule FlezhaTest.Layout.TableTest do
  @moduledoc """
  The layout table against the tree it declares.

  The point of a declaration is that nothing bypasses it, so these cases read
  the source tree as well as the table: a subtree with a private root, or a
  second module spelling `storage/app/<x>`, is a finding here rather than a
  directory a user discovers is not cleared.
  """
  use ExUnit.Case, async: true

  alias Flezha.Config.Keys
  alias Flezha.Layout.Table, as: Layout
  alias FlezhaTest.Helper.SourceWalk

  @fixture "test/fixtures/layout_rows.txt"

  describe "the row set" do
    test "every declared subtree resolves to a directory under its own knob" do
      for subtree <- Layout.subtrees(), subtree != :sockets do
        dir = Layout.dir(subtree)

        assert is_binary(dir) and dir != "",
               "#{subtree} resolves to #{inspect(dir)}"

        assert Layout.file(subtree, "x.dat") == Path.join(dir, "x.dat")
        assert Layout.file(subtree, ["a", "b"]) == Path.join([dir, "a", "b"])
      end
    end

    test "the knob each row names is a row of the one knob table" do
      for subtree <- Layout.subtrees() do
        knob =
          case Layout.row(subtree).knob do
            {:dirname, name} -> name
            name -> name
          end

        assert Keys.row(knob) != nil,
               "#{subtree} names #{inspect(knob)}, which is not a knob row"
      end
    end

    test "a name no row declares is refused rather than guessed at" do
      # Read at run time: a literal would let the type checker decide the
      # clause never matches, and the refusal is what is being proved.
      absent = String.to_atom("not_a_subtree")

      assert_raise FunctionClauseError, fn -> Layout.dir(absent) end
      assert_raise FunctionClauseError, fn -> Layout.reset_mode(absent) end
      assert_raise FunctionClauseError, fn -> Layout.claim(absent) end
    end
  end

  describe "the socket row" do
    test "resolves outside the data root, and this table refuses to answer for it" do
      assert Layout.row(:sockets).claim == :ladder

      for fun <- [fn -> Layout.dir(:sockets) end, fn -> Layout.claim(:sockets) end] do
        assert_raise ArgumentError, ~r/resolved by the transport's ladder/, fun
      end
    end

    test "the socket directory in use is not under the data root" do
      # The ladder answers with a runtime directory, and `--storage-root`
      # must not move it. Were it a subtree, a reset of the data root would
      # take a live controller's control socket with it.
      refute String.starts_with?(
               Flezha.Socket.Support.socket_dir(),
               Flezha.State.Paths.state_root()
             )
    end
  end

  describe "claim/1" do
    test "creates the directory, is idempotent, and gives an auth row owner-only access" do
      base = Path.join(System.tmp_dir!(), "layout_claim_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(base) end)

      plain = Path.join(base, "plain")
      assert :ok = Layout.claim(:state, plain)
      assert :ok = Layout.claim(:state, plain)
      assert File.dir?(plain)

      secret = Path.join(base, "secret")
      assert :ok = Layout.claim(:auth, secret)
      %File.Stat{mode: mode} = File.stat!(secret)
      assert Bitwise.band(mode, 0o077) == 0, "an auth directory is owner-only"
    end
  end

  describe "no bypass" do
    test "no module outside the table spells a storage subtree literal" do
      offenders =
        SourceWalk.files(["lib"], [".ex"])
        |> Enum.reject(&(&1 in ["lib/flezha/layout/table.ex", "lib/flezha/config/keys.ex"]))
        |> Enum.filter(fn file ->
          source = SourceWalk.read(file)

          String.contains?(source, "storage/app/") and
            String.match?(strip_prose(source), ~r|storage/app/[a-z_]+|)
        end)

      assert offenders == [],
             "these spell a data-root subtree rather than asking the table: " <>
               Enum.join(offenders, ", ")
    end

    test "nothing joins a resolved root straight to a class" do
      # The area's classic defect: `class_dir/1` is three segments, so
      # `Path.join(Paths.state_root(), class)` is an assertion that can never
      # fail and an `rm_rf` that reclaims nothing. The bracket form is a
      # deliberate multi-segment build and is not this shape.
      offenders =
        (SourceWalk.files(["lib"], [".ex"]) ++ SourceWalk.files(["test"], [".exs"]))
        |> Enum.reject(&(&1 == __ENV__.file |> Path.relative_to_cwd()))
        |> Enum.filter(fn file ->
          SourceWalk.read(file) =~ ~r/Path\.join\((Paths|Registry|Layout)\.(state_root|dir)\(/
        end)

      assert offenders == []
    end
  end

  describe "the shared row fixture" do
    test "declares exactly the table's rows, in the table's order, with its reset modes" do
      declared =
        @fixture
        |> File.read!()
        |> String.split("\n", trim: true)
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == "" or String.starts_with?(&1, "#")))
        |> Enum.map(fn line ->
          [name, mode] = String.split(line, "|")
          {String.to_existing_atom(name), String.to_existing_atom(mode)}
        end)

      assert declared == Enum.map(Layout.subtrees(), &{&1, Layout.reset_mode(&1)})
    end
  end

  # Comments and doc heredocs explain the layout; they are not a second
  # declaration of it. Only code is judged.
  defp strip_prose(source) do
    source
    |> String.replace(~r/^\s*@(module|type|)doc\s+"""(.|\n)*?"""/m, "")
    |> String.replace(~r/^\s*@(module|type|)doc\s+".*"$/m, "")
    |> String.replace(~r/^\s*#.*$/m, "")
  end
end
