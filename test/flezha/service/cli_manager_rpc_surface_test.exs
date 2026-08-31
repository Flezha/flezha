defmodule Flezha.Service.CliManagerRpcSurfaceTest do
  @moduledoc """
  `flz` builds `Flezha.Service.CliManager.<fun>(...)` as a *string* and sends it
  to `flzvm rpc`, so a missing or renamed delegation compiles clean on both
  sides and fails only on an operator's machine. This test reads the names out
  of `cli/src` and resolves each against the module's real export list.
  """
  use ExUnit.Case, async: true

  # `cli/src` also calls through helpers that take the function name as a
  # parameter. Each one fixes the arity by construction; the second element is
  # the index of the name argument, the third a prefix the helper adds.
  @generic %{
    "scoped_read" => {2, 1, ""},
    "one_arg_authz" => {2, 1, ""},
    "token_only" => {1, 1, ""},
    "render" => {1, 2, ""},
    "named_source" => {1, 2, ""},
    "handle_entity_verb" => {2, 2, ""},
    "handle_read" => {3, 3, ""},
    "handle_named" => {1, 0, "schedule_"}
  }

  @rpc_call ~r/Flezha\.Service\.CliManager\.([a-z_0-9]+|\{\}|schedule_\{\})\(/

  setup_all do
    files =
      "cli/src/**/*.rs"
      |> Path.wildcard()
      |> Map.new(&{&1, File.read!(&1)})

    assert map_size(files) > 0, "cli/src carries no Rust source; the gate would pass vacuously"
    %{files: files, exports: MapSet.new(Flezha.Service.CliManager.__info__(:functions))}
  end

  test "every RPC name cli/src spells resolves to an exported CliManager function", ctx do
    calls = direct_calls(ctx.files) ++ generic_calls(ctx.files)

    # A gate that finds nothing passes for the wrong reason.
    assert length(calls) >= 60,
           "only #{length(calls)} RPC call sites found in cli/src; the extraction is broken"

    for {name, arity, where} <- calls do
      assert MapSet.member?(ctx.exports, {String.to_atom(name), arity}),
             "#{where} calls Flezha.Service.CliManager.#{name}/#{arity}, which is not exported"
    end
  end

  test "no generic call site is missed: every CliManager.{} format is a known helper", ctx do
    for {path, source} <- ctx.files,
        [offset] <- generic_offsets(source) do
      helper = enclosing_fn(source, offset)

      assert Map.has_key?(@generic, helper),
             "#{path} builds a CliManager call inside #{helper}/?, which this test does not " <>
               "model; add it to @generic with the arity it fixes"
    end
  end

  defp direct_calls(files) do
    for {path, source} <- files,
        [{start, len}, {ns, nl}] <- Regex.scan(@rpc_call, source, return: :index),
        name = binary_part(source, ns, nl),
        not String.contains?(name, "{}"),
        do: {name, arity_at(source, start + len - 1), "#{path}:#{line_of(source, start)}"}
  end

  defp generic_calls(files) do
    for {path, source} <- files,
        {helper, {arity, argi, prefix}} <- @generic,
        {lit, offset} <- helper_literals(source, helper, argi),
        do: {prefix <> lit, arity, "#{path}:#{line_of(source, offset)}"}
  end

  defp generic_offsets(source) do
    for [{start, len}, {ns, nl}] <- Regex.scan(@rpc_call, source, return: :index),
        String.contains?(binary_part(source, ns, nl), "{}"),
        do: [start + len]
  end

  # The `fn <name>(` that most recently opened before `offset`.
  defp enclosing_fn(source, offset) do
    source
    |> binary_part(0, offset)
    |> then(&Regex.scan(~r/\bfn\s+([a-z_0-9]+)\s*\(/, &1))
    |> List.last()
    |> case do
      [_, name] -> name
      nil -> "(top level)"
    end
  end

  # Literal `"name"` passed at position `argi` of a call to `helper`.
  defp helper_literals(source, helper, argi) do
    for [{start, len}] <- Regex.scan(~r/(?<![\w])#{helper}\(/, source, return: :index),
        args = balanced(source, start + len - 1),
        part = Enum.at(split_args(args), argi),
        part != nil,
        [_, lit] <- [Regex.run(~r/^\s*"([a-z_0-9]+)"\s*$/, part || "")] |> Enum.reject(&is_nil/1),
        do: {lit, start}
  end

  # Arity of the call whose `(` is at `open`: top-level commas in the format string.
  defp arity_at(source, open) do
    source |> balanced(open) |> split_args() |> Enum.count(&(String.trim(&1) != ""))
  end

  defp balanced(source, open) do
    size = byte_size(source)

    Enum.reduce_while((open + 1)..(size - 1)//1, {0, []}, fn i, {depth, acc} ->
      <<c>> = binary_part(source, i, 1)

      cond do
        c == ?( -> {:cont, {depth + 1, [c | acc]}}
        c == ?) and depth == 0 -> {:halt, {depth, acc}}
        c == ?) -> {:cont, {depth - 1, [c | acc]}}
        true -> {:cont, {depth, [c | acc]}}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
    |> List.to_string()
  end

  defp split_args(args) do
    args
    |> String.to_charlist()
    |> Enum.reduce({0, [""]}, fn c, {depth, [cur | rest]} ->
      cond do
        c in ~c"([{" -> {depth + 1, [cur <> <<c>> | rest]}
        c in ~c")]}" -> {depth - 1, [cur <> <<c>> | rest]}
        c == ?, and depth == 0 -> {depth, ["", cur | rest]}
        true -> {depth, [cur <> <<c>> | rest]}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  defp line_of(source, offset),
    do: source |> binary_part(0, offset) |> String.split("\n") |> length()
end
