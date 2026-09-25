defmodule Flezha.Service.CliManagerRpcSurfaceTest do
  @moduledoc """
  `flz` builds `Flezha.Service.CliManager.<fun>(...)` as a *string* and sends it
  to `flzvm rpc`, so a missing or renamed delegation compiles clean on both
  sides and fails only on a caller's machine. This test reads the names out
  of `cli/src` and hands them to `FlezhaTest.Helper.RpcSurface`, which is the
  one resolver both name-resolved consumers share.

  A request is built by `rpc::run(name, positional, opts, token, failure)` or
  `rpc::expression(name, positional, opts, token)`: the arity the controller
  must export is the positional slice's length plus the options list every
  request ends with.
  """
  use ExUnit.Case, async: true

  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.RpcSurface

  # `cli/src` also calls through helpers that take the function name as a
  # parameter. Each one fixes the arity by construction; the second element is
  # the index of the name argument, the third a prefix the helper adds.
  @generic %{
    "scoped_read" => {3, 1, ""},
    "one_arg" => {2, 2, ""},
    "token_only" => {1, 1, ""},
    "rendered" => {2, 1, ""},
    "scoped_verb" => {3, 1, ""},
    "quota_verb" => {3, 1, ""}
  }

  # `rpc::run` forwards its name to `rpc::expression`: the seam's own plumbing,
  # not a call site.
  @plumbing ["run"]

  # `rpc::run(` anywhere, or a bare `run(` / `expression(` inside `rpc.rs`
  # itself; `dispatch::run(` and a method call are neither.
  @builder ~r/(?<!fn )(?<![\w.:])(?:rpc::)?(?:run|expression)\(/

  setup_all do
    # A file's own unit tests build requests too; the wire contract is what
    # ships, so the source is read up to its test module.
    files =
      "cli/src/**/*.rs"
      |> Path.wildcard()
      |> Map.new(&{&1, &1 |> File.read!() |> String.split("\n#[cfg(test)]") |> hd()})

    assert map_size(files) > 0, "cli/src carries no Rust source; the gate would pass vacuously"
    %{files: files}
  end

  test "every RPC name cli/src spells resolves to an exported CliManager function", ctx do
    calls = direct_calls(ctx.files) ++ generic_calls(ctx.files)

    # A gate that finds nothing passes for the wrong reason.
    assert length(calls) >= 60,
           "only #{length(calls)} RPC call sites found in cli/src; the extraction is broken"

    failures =
      calls
      |> Enum.map(&{CliManager, String.to_atom(elem(&1, 0)), elem(&1, 1), elem(&1, 2)})
      |> RpcSurface.unresolved()

    assert failures == [], Enum.map_join(failures, "\n", &RpcSurface.explain/1)
  end

  test "no generic call site is missed: every builder call without a literal name is a known helper",
       ctx do
    for {path, source} <- ctx.files,
        {offset, args} <- builder_calls(source),
        not literal?(hd(args)) do
      helper = enclosing_fn(source, offset)

      assert Map.has_key?(@generic, helper) or helper in @plumbing,
             "#{path} builds a CliManager call inside #{helper}/?, which this test does not " <>
               "model; add it to @generic with the arity it fixes"
    end
  end

  # Every `run("name", &[...], ...)` / `expression("name", &[...], ...)` with a
  # literal name: the arity is the positional slice plus the options list.
  defp direct_calls(files) do
    for {path, source} <- files,
        {offset, [name_arg, slice | _]} <- builder_calls(source),
        literal?(name_arg),
        do:
          {literal(name_arg), slice_length(path, slice) + 1, "#{path}:#{line_of(source, offset)}"}
  end

  defp generic_calls(files) do
    for {path, source} <- files,
        {helper, {arity, argi, prefix}} <- @generic,
        {lit, offset} <- helper_literals(source, helper, argi),
        do: {prefix <> lit, arity, "#{path}:#{line_of(source, offset)}"}
  end

  # `{offset, [arg, ...]}` for every builder call in `source`.
  defp builder_calls(source) do
    for [{start, len}] <- Regex.scan(@builder, source, return: :index) do
      {start, source |> balanced(start + len - 1) |> split_args() |> Enum.map(&String.trim/1)}
    end
  end

  defp literal?(arg), do: Regex.match?(~r/^"[a-z_0-9?]+"$/, arg)
  defp literal(arg), do: String.trim(arg, "\"")

  # The positional slice is a `&[ ... ]` literal, so the arity is readable here;
  # a slice built elsewhere would hide it, and is refused.
  defp slice_length(path, slice) do
    case Regex.run(~r/^&\[(.*)\]$/s, slice) do
      [_, ""] ->
        0

      [_, inner] ->
        inner |> split_args() |> Enum.count(&(String.trim(&1) != ""))

      nil ->
        flunk("#{path}: a builder's positional arguments are not a slice literal: #{slice}")
    end
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

  defp balanced(source, open) do
    size = byte_size(source)

    Enum.reduce_while((open + 1)..(size - 1)//1, {0, []}, fn i, {depth, acc} ->
      <<c>> = binary_part(source, i, 1)

      cond do
        c in [?(, ?[] -> {:cont, {depth + 1, [c | acc]}}
        c == ?) and depth == 0 -> {:halt, {depth, acc}}
        c in [?), ?]] -> {:cont, {depth - 1, [c | acc]}}
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
