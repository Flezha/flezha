defmodule Flezha.Config.ParseTest do
  @moduledoc """
  One parse rule per knob type, and the same rule in every runtime.

  `test/fixtures/knob_parse_cases.txt` is the rule written down once; this is
  the controller's reading of it. Every runtime's suite feeds the same file to
  its own reader, so a Ruby runner and a Rust runner answer `"0"` the way this
  does.
  """
  use ExUnit.Case, async: true

  alias Flezha.Config
  alias Flezha.Config.Environment

  @fixture "test/fixtures/knob_parse_cases.txt"

  # A row of each type whose parse the fixture is describing.
  @row_of %{
    "positive_int" => :positive_int,
    "ms" => :ms,
    "int" => :int,
    "bool" => :bool
  }

  defp cases() do
    @fixture
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.reject(&(String.starts_with?(&1, "#") or &1 == ""))
    |> Enum.map(fn line ->
      [type, input, expected] = String.split(line, "|")
      {type, if(input == "<empty>", do: "", else: input), expected}
    end)
  end

  test "the fixture describes every type the table parses" do
    declared = cases() |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort()
    assert declared == Enum.sort(Map.keys(@row_of))
  end

  test "every case parses the way the fixture says" do
    for {type, input, expected} <- cases() do
      result = Environment.parse(%{type: Map.fetch!(@row_of, type)}, input)

      assert result == expect(expected),
             "#{type}|#{input}: expected #{expected}, got #{inspect(result)}"
    end
  end

  test "an unparsable count or flag falls back to the row's default, and every other type refuses" do
    # `"0"` already means "unset" in every runtime, so a pool size nobody meant
    # to set never shrinks a pool to nothing.
    assert Environment.runtime_config(%{"FLEZHA_CONCURRENT_CHANNEL_COUNT" => "0"}, :prod)[
             :concurrent_channel_count
           ] == System.schedulers_online()

    assert Environment.runtime_config(%{"FLEZHA_HEALTHZ_ENABLED" => "maybe"}, :prod)[
             :healthz_enabled
           ] ==
             false

    # A duration that is not one is a knob the user believes they set.
    assert_raise ArgumentError, fn ->
      Environment.runtime_config(%{"FLEZHA_OVERWATCH_PING_TIMEOUT_MS" => "soon"}, :prod)
    end

    assert_raise ArgumentError, fn ->
      Environment.runtime_config(%{"FLEZHA_TRANSPORT" => "carrier-pigeon"}, :prod)
    end
  end

  test "a variable set to nothing is a variable nobody set" do
    # An empty value cannot be a path, a port or a flag; every runtime's reader
    # reads it as unset, and the boot translation must not refuse on it.
    config =
      Environment.runtime_config(%{"FLEZHA_HEALTHZ_PORT" => "", "FLEZHA_TRANSPORT" => " "}, :prod)

    assert config[:healthz_port] == Config.default(:healthz_port)
    assert config[:transport] == Config.default(:transport)
  end

  test "a value pasted with a stray space still means what it looks like" do
    config = Environment.runtime_config(%{"FLEZHA_HEALTHZ_PORT" => " 9571 "}, :prod)
    assert config[:healthz_port] == 9571
  end

  defp expect("error"), do: :error
  defp expect("true"), do: {:ok, true}
  defp expect("false"), do: {:ok, false}
  defp expect(value), do: {:ok, String.to_integer(value)}
end
