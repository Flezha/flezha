defmodule FlezhaTest.Helper.RpcSurface do
  @moduledoc """
  Resolves `Module.function/arity` names a consumer spells at runtime against
  this tree's real exports.

  `flezha_ui` reaches the controller by name, calling modules over `:erpc`, and
  gets no compiler to check it. A rename compiles clean on both sides, so the
  break is reported here, in the run that causes it.

  An unresolved name is classified, because the remedy differs per class:
  `{:moved, candidates}` when some module here exports the same name and arity,
  `{:privatised, module}` when the name survives only as a `defp`, and
  `:withdrawn` when it appears nowhere. A `defp` is absent from
  `module_info/1`, so the two failure classes read the tree's source.
  """

  alias FlezhaTest.Helper.SourceWalk

  @type entry :: {module(), atom(), arity(), String.t()}
  @type verdict :: :ok | {:moved, [module()]} | {:privatised, module()} | :withdrawn

  @doc """
  The entries a manifest file records.

  One line per `Module.function/arity`; `#` opens a comment, and a trailing
  `…, <path>:<line>` names where a consumer calls it, which is where a reader has
  to go to fix the call. A line with no coordinate takes the manifest's own path,
  so a failure always says which file asked for the name.
  """
  @spec manifest(Path.t()) :: [entry()]
  def manifest(path) do
    path
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(String.trim(&1), "#"))
    |> Enum.map(&line(&1, path))
  end

  defp line(text, source) do
    {mfa, where} =
      case String.split(text, "#", parts: 2) do
        [mfa, comment] -> {mfa, coordinate(comment, source)}
        [mfa] -> {mfa, source}
      end

    [name, arity] = mfa |> String.trim() |> String.split("/")
    {module, [function]} = name |> String.split(".") |> Enum.split(-1)

    {Module.concat(module), String.to_atom(function), String.to_integer(arity),
     String.trim(where)}
  end

  defp coordinate(comment, source) do
    case String.split(comment, ", ", parts: 2) do
      [_count, coordinate] -> coordinate
      _no_coordinate -> source
    end
  end

  @doc """
  A verdict per entry, in the order given.

  The tree's source is read only when something failed to resolve, so a green
  run costs one `module_info/1` per entry.
  """
  @spec resolve([entry()]) :: [{entry(), verdict()}]
  def resolve(entries) when is_list(entries) do
    verdicts = Enum.map(entries, &{&1, if(exported?(&1), do: :ok, else: :unresolved)})

    if Enum.any?(verdicts, &match?({_, :unresolved}, &1)) do
      sources = lib_sources()

      Enum.map(verdicts, fn
        {entry, :unresolved} -> {entry, classify(entry, sources)}
        resolved -> resolved
      end)
    else
      verdicts
    end
  end

  @doc "The entries that did not resolve, with their class."
  @spec unresolved([entry()]) :: [{entry(), verdict()}]
  def unresolved(entries) do
    entries |> resolve() |> Enum.reject(&match?({_, :ok}, &1))
  end

  @doc "One line naming the entry, its class and any successor found."
  @spec explain({entry(), verdict()}) :: String.t()
  def explain({{module, fun, arity, where}, verdict}) do
    "#{where} calls #{inspect(module)}.#{fun}/#{arity}, which #{tail(verdict, fun, arity)}"
  end

  defp tail(:ok, _fun, _arity), do: "resolves."

  defp tail({:moved, candidates}, fun, arity) do
    "has MOVED: this tree exports #{fun}/#{arity} from " <>
      Enum.map_join(candidates, ", ", &inspect/1) <> ". Re-point the caller."
  end

  defp tail({:privatised, holder}, _fun, _arity) do
    "was PRIVATISED: it survives only as a `defp` in #{inspect(holder)}. " <>
      "Publish it rather than re-pointing the caller."
  end

  defp tail(:withdrawn, _fun, _arity) do
    "was WITHDRAWN: the name appears nowhere in this tree. There is nothing " <>
      "to point at, so this one needs a person."
  end

  defp exported?({module, fun, arity, _where}), do: exported?(module, fun, arity)

  defp exported?(module, fun, arity) do
    Code.ensure_loaded?(module) and function_exported?(module, fun, arity)
  end

  defp classify({module, fun, arity, _where}, sources) do
    cond do
      (moved = moved_candidates(module, fun, arity)) != [] -> {:moved, moved}
      (holder = private_holder(sources, fun, arity)) != nil -> {:privatised, holder}
      true -> :withdrawn
    end
  end

  # Every module here exporting the same name and arity, the one sharing the
  # longest namespace with the module the consumer named offered first — which
  # is what a move within a namespace looks like.
  defp moved_candidates(named, fun, arity) do
    app_modules()
    |> Enum.filter(&(&1 != named and exported?(&1, fun, arity)))
    |> Enum.sort_by(&{-shared_segments(&1, named), &1})
  end

  defp app_modules do
    Application.load(:flezha)
    Application.spec(:flezha, :modules) || []
  end

  defp shared_segments(a, b) do
    a
    |> segments()
    |> Enum.zip(segments(b))
    |> Enum.take_while(fn {x, y} -> x == y end)
    |> length()
  end

  defp segments(module) do
    case Atom.to_string(module) do
      "Elixir." <> _ -> Module.split(module)
      other -> [other]
    end
  end

  defp private_holder(sources, fun, arity) do
    needle = "defp #{fun}"

    sources
    |> Enum.filter(fn {_path, source} -> String.contains?(source, needle) end)
    |> Enum.find_value(fn {_path, source} -> private_module(source, fun, arity) end)
  end

  # The module holding a private `fun/arity`, which is the last `defmodule`
  # opened above the clause — right for a nested module too.
  defp private_module(source, fun, arity) do
    with {:ok, ast} <- Code.string_to_quoted(source),
         {modules, [line | _]} <- scan(ast, fun, arity),
         [{_line, name} | _] <- Enum.reverse(Enum.filter(modules, &(elem(&1, 0) <= line))) do
      name
    else
      _ -> nil
    end
  end

  defp scan(ast, fun, arity) do
    {_ast, {modules, lines}} =
      Macro.prewalk(ast, {[], []}, fn
        {:defmodule, meta, [alias_ast | _]} = node, {modules, lines} ->
          {node, {[{line(meta), module_name(alias_ast)} | modules], lines}}

        {:defp, meta, [head | _]} = node, {modules, lines} ->
          if head_matches?(head, fun, arity),
            do: {node, {modules, [line(meta) | lines]}},
            else: {node, {modules, lines}}

        node, acc ->
          {node, acc}
      end)

    {Enum.sort(modules), Enum.sort(lines)}
  end

  defp line(meta), do: Keyword.get(meta, :line, 0)

  defp module_name({:__aliases__, _meta, segments}), do: Module.concat(segments)
  defp module_name(_other), do: nil

  defp head_matches?({:when, _meta, [inner | _]}, fun, arity),
    do: head_matches?(inner, fun, arity)

  defp head_matches?({name, _meta, args}, fun, arity) when is_atom(name) and name == fun do
    args = if is_list(args), do: args, else: []
    defaults = Enum.count(args, &match?({:\\, _meta, _}, &1))
    arity in (length(args) - defaults)..length(args)//1
  end

  defp head_matches?(_head, _fun, _arity), do: false

  defp lib_sources do
    for path <- SourceWalk.files(["lib"], [".ex"]), do: {path, SourceWalk.read(path)}
  end
end
