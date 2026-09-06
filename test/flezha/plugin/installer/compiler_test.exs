defmodule Flezha.Plugin.Installer.CompilerTest do
  @moduledoc """
  Tests for `Flezha.Plugin.Installer.Compiler`. Compiles real source trees
  in per-test tmp dirs. We use distinct module names per test so the
  global code server doesn't collide across tests.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  alias Flezha.Plugin.Installer.Compiler

  setup %{tmp: tmp} do
    File.mkdir_p!(Path.join(tmp, "src"))
    :ok
  end

  test "clean compile returns produced modules", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod_atom = :"Elixir.FlzCompilerTest.Clean#{unique}"

    File.write!(Path.join(tmp, "src/clean.ex"), """
    defmodule FlzCompilerTest.Clean#{unique} do
      def hello, do: :ok
    end
    """)

    assert {:ok, beams} = Compiler.compile(Path.join(tmp, "src"))
    assert [{^mod_atom, binary}] = beams
    assert is_binary(binary)
  end

  test "syntax error aborts with :compile_failed", %{tmp: tmp} do
    File.write!(Path.join(tmp, "src/bad.ex"), """
    defmodule Bad do
      def broken do
        no end
    """)

    assert {:error, {:compile_failed, file, _reason}} =
             Compiler.compile(Path.join(tmp, "src"))

    assert String.ends_with?(file, "src/bad.ex")
  end

  test "module mismatch reported", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    mod_atom = :"Elixir.FlzCompilerTest.Mismatch#{unique}"

    File.write!(Path.join(tmp, "src/mismatch.ex"), """
    defmodule FlzCompilerTest.Mismatch#{unique} do
      def hello, do: :ok
    end
    """)

    expected = [mod_atom, SomeOther]

    assert {:error, {:manifest_invalid, {:module_mismatch, info}}} =
             Compiler.compile(Path.join(tmp, "src"), expected)

    assert Keyword.has_key?(info, :expected)
    assert Keyword.has_key?(info, :produced)
  end

  test "extra module (produced > expected) reported", %{tmp: tmp} do
    unique = System.unique_integer([:positive])

    File.write!(Path.join(tmp, "src/a.ex"), """
    defmodule FlzCompilerTest.A#{unique} do end
    defmodule FlzCompilerTest.B#{unique} do end
    """)

    expected = [:"Elixir.FlzCompilerTest.A#{unique}"]

    assert {:error, {:manifest_invalid, {:module_mismatch, _info}}} =
             Compiler.compile(Path.join(tmp, "src"), expected)
  end

  test "warnings-as-errors rejects unused variable", %{tmp: tmp} do
    unique = System.unique_integer([:positive])

    File.write!(Path.join(tmp, "src/warn.ex"), """
    defmodule FlzCompilerTest.Warn#{unique} do
      def go(unused), do: :ok
    end
    """)

    assert {:error, {:compile_failed, file, {:warning, msg}}} =
             Compiler.compile(Path.join(tmp, "src"))

    assert String.ends_with?(file, "src/warn.ex")
    assert msg =~ "unused"
  end

  test "mixed .ex and .erl package compiles", %{tmp: tmp} do
    unique = System.unique_integer([:positive])
    ex_mod = :"Elixir.FlzCompilerTest.Mixed#{unique}"
    erl_mod = :"flz_compiler_test_mixed_erl#{unique}"

    File.write!(Path.join(tmp, "src/mixed.ex"), """
    defmodule FlzCompilerTest.Mixed#{unique} do
      def hi, do: :ok
    end
    """)

    File.write!(
      Path.join(tmp, "src/flz_compiler_test_mixed_erl#{unique}.erl"),
      """
      -module(flz_compiler_test_mixed_erl#{unique}).
      -export([hi/0]).
      hi() -> ok.
      """
    )

    assert {:ok, beams} = Compiler.compile(Path.join(tmp, "src"))
    mods = Enum.map(beams, fn {m, _} -> m end) |> Enum.sort()
    assert mods == Enum.sort([ex_mod, erl_mod])
  end
end
