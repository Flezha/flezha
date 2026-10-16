defmodule Flezha.ErrorCatalogueTest do
  @moduledoc """
  A reason cannot reach a caller without a code: every `{:error, atom}` and
  `{:error, {atom, ...}}` written under `lib/` is mapped by the catalogue or
  named on its `internal` list, no `{:error, <text>}` is written at all, and
  the catalogue itself is consistent.
  """
  use ExUnit.Case, async: true

  alias Flezha.AuthZ.Verbs

  @catalogue Flezha.Error.catalogue()

  test "every error tag under lib/ is catalogued or named internal, and no reason is text" do
    known =
      MapSet.new(
        for(
          {_code, entry} <- @catalogue["codes"],
          reason <- Map.get(entry, "reasons", []),
          do: reason["tag"] |> List.wrap() |> hd()
        ) ++ @catalogue["internal"]
      )

    findings = Enum.flat_map(Path.wildcard("lib/**/*.ex"), &scan/1)

    missing =
      for {:tag, tag, where} <- findings,
          not MapSet.member?(known, Atom.to_string(tag)),
          do: "#{where}: {:error, #{inspect(tag)}...} is neither catalogued nor internal"

    texts = for {:text, where} <- findings, do: "#{where}: a reason written as text"

    assert missing ++ texts == [], Enum.join(Enum.uniq(missing ++ texts), "\n")
  end

  test "every code is family.snake, of a known class, and names only its declared details" do
    classes = Map.keys(@catalogue["classes"])

    wrong =
      for {code, entry} <- @catalogue["codes"],
          problem <- problems(code, entry, classes),
          do: "#{code}: #{problem}"

    assert wrong == [], Enum.join(wrong, "\n")
  end

  # Fails when: an internal code the controller converts a reason into carries
  # its term under another name, which `Flezha.Error.withheld/2` would pass on.
  test "every internal code the controller converts a reason into carries it as reason" do
    misnamed =
      for {code, %{"class" => "internal", "details" => details} = entry} <- @catalogue["codes"],
          Map.has_key?(entry, "reasons"),
          details != [] and "reason" not in details,
          do: code

    assert misnamed == []
  end

  test "every site an entry is limited to is a verb action or a declared wire site" do
    sites =
      MapSet.new(Enum.map(Verbs.all(), &Atom.to_string(&1.action)) ++ @catalogue["sites"])

    unknown =
      for {code, entry} <- @catalogue["codes"],
          reason <- Map.get(entry, "reasons", []),
          site <- Map.get(reason, "at") || [],
          not MapSet.member?(sites, site),
          do: "#{code}: #{site}"

    assert unknown == [],
           "sites that are neither a verb action nor a wire site:\n" <> Enum.join(unknown, "\n")
  end

  test "no reason at one site maps to two codes" do
    claims =
      for {code, entry} <- @catalogue["codes"],
          reason <- Map.get(entry, "reasons", []),
          site <- Map.get(reason, "at") || [:anywhere],
          do: {{List.wrap(reason["tag"]), length(Map.get(reason, "args", [])), site}, code}

    twice =
      claims
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.filter(fn {_key, codes} -> length(Enum.uniq(codes)) > 1 end)

    assert twice == [], inspect(twice, pretty: true)
  end

  test "an unknown hook is refused in the one catalogued shape wherever it is named" do
    catalog = Flezha.Relay.Overwatch.__extensions_catalog__()
    table = Flezha.Plugin.Extension.Table.new(catalog)

    {:error, at_dispatch} = Flezha.Plugin.Extension.run(table, :on_nothing, %{})

    {:error, at_subscribe} =
      Flezha.Plugin.Extension.Table.canonicalise(catalog, %Flezha.Plugin{
        id: "p",
        provider: nil,
        hooks: [{"on_nothing", []}]
      })

    for reason <- [at_dispatch, at_subscribe] do
      assert Flezha.Error.catalogued?(reason, nil), "#{inspect(reason)} has no code"
      assert Flezha.Error.from_reason(reason, nil).code == "plugin.unknown_hook"
    end
  end

  test "docs/errors.md is the fresh generation of the catalogue" do
    assert File.read!("docs/errors.md") == Mix.Tasks.Flezha.ErrorsDoc.render(),
           "docs/errors.md is stale: run `mix flezha.errors_doc`"
  end

  defp problems(code, entry, classes) do
    declared = Map.get(entry, "details", [])

    placeholders =
      [entry["message"], entry["hint"]]
      |> Enum.reject(&is_nil/1)
      |> Enum.flat_map(&Regex.scan(~r/\{([a-z0-9_]+)\}/, &1, capture: :all_but_first))
      |> List.flatten()

    ([
       unless(code =~ ~r/^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$/, do: "is not spelled family.snake"),
       unless(entry["class"] in classes,
         do: "names the unknown class #{inspect(entry["class"])}"
       ),
       unless(is_binary(entry["message"]), do: "has no message")
     ] ++
       for(
         key <- Enum.uniq(placeholders) -- declared,
         do: "names {#{key}} but declares no such detail"
       ))
    |> Enum.reject(&is_nil/1)
  end

  # ---------- the source scan ----------

  # Literals are wrapped so a keyword pair (`error: x`, `format: :keyword`) is
  # told apart from a tuple written as `{:error, x}`.
  defp scan(file) do
    {:ok, ast} =
      file
      |> File.read!()
      |> Code.string_to_quoted(
        file: file,
        columns: true,
        literal_encoder: &{:ok, {:__block__, &2, [&1]}}
      )

    {_ast, findings} =
      Macro.prewalk(ast, [], fn
        {{:__block__, meta, [:error]}, reason} = node, acc ->
          if meta[:format] == :keyword,
            do: {node, acc},
            else: {node, finding(reason, "#{file}:#{meta[:line]}") ++ acc}

        node, acc ->
          {node, acc}
      end)

    findings
  end

  defp finding({:__block__, _, [tag]}, where) when is_atom(tag) and tag not in [nil, true, false],
    do: [{:tag, tag, where}]

  defp finding({:__block__, _, [text]}, where) when is_binary(text), do: [{:text, where}]
  defp finding({:<<>>, _, _parts}, where), do: [{:text, where}]
  defp finding({:<>, _, _parts}, where), do: [{:text, where}]

  defp finding({:__block__, _, [{{:__block__, _, [tag]}, _second}]}, where) when is_atom(tag),
    do: [{:tag, tag, where}]

  defp finding({:{}, _, [{:__block__, _, [tag]} | _]}, where) when is_atom(tag),
    do: [{:tag, tag, where}]

  defp finding(_reason, _where), do: []
end
