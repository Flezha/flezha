defmodule Flezha.Observability.TelemetryEventSeamTest do
  @moduledoc """
  The export seam: an event name the tree writes reaches an exporter, or the
  bridge says why it does not.

  Three clauses walk `lib/flezha` and fail naming what they found:

    1. **Coverage** - every `[:flezha, …]` event NAME the tree writes is a key
       of the bridge's merged mapping or a member of its unexported list, whose
       length is pinned as a ceiling and whose every entry carries a reason.
       Both directions: a mapped event nothing writes any more is named too.
    2. **Readability** - a `:telemetry.execute` whose event argument this walk
       cannot read is one of three named relays, and a fourth fails.
    3. **Derivation** - every event a detector entry declares is bridged by
       that declaration, with no line in the hand-kept half.

  The name is judged where it is WRITTEN, not only at the `:telemetry.execute`
  call. Three names are assembled into a tuple in one module and executed in
  another, and a provider's path is assembled from its own identifier, so a walk
  that reads only the execute call sees neither. It saw neither for a long time,
  and eight live event names reached no exporter while this gate was green:
  `docs/dev_notes/telemetry-bridge.md`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Convergence.Probe.Table
  alias Flezha.Observability.TelemetryBridge
  alias FlezhaTest.Helper.SourceWalk

  # Two files write `[:flezha, …]` names without emitting them: the detector
  # table declares what the derived half bridges, and the bridge holds the
  # mapping and the unexported list. Reading either as a writer would make every
  # mapped name its own justification.
  @declaring %{
    "lib/flezha/convergence/probe/table.ex" => "the detector declarations",
    "lib/flezha/observability/telemetry_bridge.ex" => "the mapping and the unexported list"
  }

  # `:telemetry.execute` sites whose event argument this walk cannot read. Each
  # names where its literal is written instead. A FOURTH such site fails the
  # clause below, because a relay whose writer this walk never reads would be
  # invisible exactly the way these three were.
  @relays %{
    "lib/flezha/plugin/provider_behaviour.ex" =>
      "the provider family's one constructor; `provider_events/1` reads its call sites",
    "lib/flezha/runner/control_channel/commits.ex" =>
      "`commit.telemetry`, whose names are written literally in the same module",
    "lib/flezha/state/wal_writer.ex" =>
      "the outbox flush tuple, whose name is written literally in `state/proc_journal.ex`"
  }

  describe "clause 1: every event name reaches an exporter" do
    test "every event name the tree writes is mapped or unexported with a reason" do
      mapped = MapSet.new(Map.keys(TelemetryBridge.mapping()))
      unexported = MapSet.new(TelemetryBridge.unexported())

      offenders =
        for {event, where} <- written_events(),
            not MapSet.member?(mapped, event),
            not MapSet.member?(unexported, event),
            uniq: true,
            do: "#{where} #{inspect(event)}"

      assert offenders == [],
             "event names reaching no exporter: " <> Enum.join(Enum.sort(offenders), "; ")
    end

    test "the unexported list is a ratchet, is really written, and every entry has a reason" do
      assert length(TelemetryBridge.unexported()) <= 21,
             "the unexported list may only shrink"

      written = MapSet.new(written_events(), fn {event, _where} -> event end)

      stale = Enum.reject(TelemetryBridge.unexported(), &MapSet.member?(written, &1))
      assert stale == [], "unexported events nothing writes: #{inspect(stale)}"

      for event <- TelemetryBridge.unexported() do
        assert TelemetryBridge.unexported_reason(event) in TelemetryBridge.reasons(),
               "no reason for #{inspect(event)}"
      end
    end

    test "a mapped event nothing writes is named, and only those are" do
      written = MapSet.new(written_events(), fn {event, _where} -> event end)
      mapped = MapSet.new(Map.keys(TelemetryBridge.mapping()))

      assert MapSet.equal?(
               MapSet.difference(mapped, written),
               MapSet.new(Map.keys(TelemetryBridge.unwritten()))
             ),
             "the mapping holds keys nothing writes, or names keys something does: " <>
               inspect(MapSet.to_list(MapSet.difference(mapped, written)))
    end
  end

  describe "clause 2: every execute site is readable, or a named relay" do
    test "an execute site whose event name this walk cannot read is a named relay" do
      offenders =
        for file <- source_files(),
            not Map.has_key?(@relays, file),
            ast = SourceWalk.ast(file),
            {line, raw} <- executes(ast),
            not readable?(raw, ast),
            do: "#{file}:#{line} #{Macro.to_string(raw)}"

      assert offenders == [],
             "a `:telemetry.execute` whose event name this walk cannot read: " <>
               Enum.join(offenders, "; ")

      assert map_size(@relays) <= 3,
             "the relay list may only shrink: a name this walk cannot read is " <>
               "invisible to clause 1 unless something else writes it literally"
    end
  end

  describe "clause 3: a detector's declaration is the bridge line" do
    test "every event a detector declares is bridged by derivation" do
      derived = TelemetryBridge.derived()

      for event <- Table.events() do
        assert Map.has_key?(derived, event), "undeclared in the derived half: #{inspect(event)}"
      end
    end
  end

  # Every `[:flezha, …]` event NAME the tree writes, as `{event, where}`.
  #
  # The name is judged where it is WRITTEN, not only at the `:telemetry.execute`
  # call. Three names are assembled into a tuple in one module and executed in
  # another, and a provider's path is assembled from its own identifier, so a
  # walk that reads only the execute call sees neither, and a gate that reports
  # completeness while missing a name is worse than the hand list it replaces.
  # A name found at an execute call keeps its line, so the offender message
  # still reads `file:line event` for the shape the spec describes; a name
  # written anywhere else is located by its file.
  defp written_events() do
    (literal_events() ++ assembled_events())
    |> Enum.reduce(%{}, fn {event, where}, acc ->
      Map.update(acc, event, where, &if(String.contains?(&1, ":"), do: &1, else: where))
    end)
    |> Map.to_list()
  end

  defp literal_events() do
    for file <- source_files(),
        not Map.has_key?(@declaring, file),
        event <- literals(SourceWalk.ast(file)),
        do: {event, file}
  end

  defp literals(ast) do
    {_, found} =
      Macro.prewalk(ast, [], fn
        list, acc when is_list(list) ->
          if match?([:flezha, _ | _], list) and Enum.all?(list, &is_atom/1),
            do: {list, [list | acc]},
            else: {list, acc}

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(found)
  end

  # A name assembled at run time, in the two shapes the tree uses: an emit
  # helper that takes the last segment (`[:flezha, :boot, event]`), and the
  # provider family, whose prefix comes from the provider's own identifier.
  defp assembled_events() do
    Enum.flat_map(source_files(), fn file ->
      ast = SourceWalk.ast(file)

      located =
        for {line, raw} <- executes(ast),
            event <- assembled(raw, ast),
            match?([:flezha | _], event),
            do: {event, "#{file}:#{line}"}

      located ++ for(event <- provider_events(ast), do: {event, file})
    end)
  end

  defp executes(ast) do
    {_, acc} =
      Macro.prewalk(ast, [], fn
        {{:., _, [:telemetry, :execute]}, meta, [event | _]} = node, acc ->
          {node, [{Keyword.get(meta, :line), event} | acc]}

        node, acc ->
          {node, acc}
      end)

    acc
  end

  # Every name a `:telemetry.execute` first argument can take.
  #
  # EVERY segment is resolved, not only the last. A variable in the middle,
  # `[:flezha, hub_id, :control]`, is exactly as unreadable as one at the end,
  # and a walk that reads only the tail passes it in silence. `++` composition
  # is resolved on both sides for the same reason. An unresolvable segment
  # becomes `:__dynamic__`, which is nobody's mapping key and so fails by name.
  defp assembled({:++, _, [left, right]}, ast) do
    for l <- assembled(left, ast), r <- assembled(right, ast), do: l ++ r
  end

  defp assembled({:@, _, [{name, _, nil}]}, ast),
    do: assembled(Map.get(attributes(ast), name), ast)

  defp assembled(list, ast) when is_list(list) do
    list
    |> Enum.map(&segment(&1, ast))
    |> Enum.reduce([[]], fn choices, acc ->
      for prefix <- acc, choice <- choices, do: prefix ++ [choice]
    end)
  end

  defp assembled(_other, _ast), do: []

  defp segment(atom, _ast) when is_atom(atom), do: [atom]

  defp segment({:@, _, [{name, _, nil}]}, ast), do: [Map.get(attributes(ast), name)]

  defp segment({var, _, nil}, ast) when is_atom(var), do: domain(var, ast)

  defp segment(_other, _ast), do: [:__dynamic__]

  # A site this walk can read is one that resolves to at least one name.
  # Anything else is a relay, and every relay is named.
  defp readable?(event, ast), do: assembled(event, ast) != []

  # `ProviderBehaviour.emit(provider, verb, …)`. The prefix is derived from the
  # provider's own identifier and the verb from one declared vocabulary, so the
  # family is read from the declaration rather than from a list kept here.
  defp provider_events(ast) do
    attrs = attributes(ast)

    for {provider, verb} <- provider_emits(ast),
        segment <- verb_domain(verb, ast),
        do: [:flezha, :plugin, provider_id(provider, attrs), segment]
  end

  defp provider_emits(ast) do
    {_, acc} =
      Macro.prewalk(ast, [], fn
        {{:., _, [{:__aliases__, _, mod}, :emit]}, _, [provider, verb | _]} = node, acc ->
          if List.last(mod) == :ProviderBehaviour,
            do: {node, [{provider, verb} | acc]},
            else: {node, acc}

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(acc)
  end

  # `ProviderBehaviour.event/2` takes a binary or an atom. Anything else is a
  # provider whose prefix this walk cannot read, and it fails by name rather
  # than dropping out of the family: `ProviderBehaviour` advertises that a new
  # provider is one declaration, so this is the path that has to be loud.
  defp provider_id(name, _attrs) when is_binary(name), do: String.to_atom(name)

  defp provider_id({:@, _, [{attr, _, nil}]}, attrs),
    do: provider_id(Map.get(attrs, attr), attrs)

  defp provider_id(atom, _attrs) when is_atom(atom) and not is_nil(atom), do: atom

  defp provider_id(_other, _attrs), do: :__dynamic__

  defp verb_domain(verb, _ast) when is_atom(verb), do: [verb]
  defp verb_domain({var, _, nil}, ast) when is_atom(var), do: domain(var, ast)
  defp verb_domain(_other, _ast), do: [:__dynamic__]

  # Every literal atom a call site passes for `var`, read across ALL arities of
  # the function that binds it: a default argument makes one definition answer
  # to two arities, and a walk keyed by the definition's own arity misses the
  # shorter calls. A call site that passes anything else, or a binding with no
  # call sites at all, yields `:__dynamic__` rather than being dropped: the
  # domain read as a subset is the failure this clause exists to prevent.
  defp domain(var, ast) do
    calls = call_sites(ast)

    atoms =
      for {name, index} <- bindings(var, ast),
          args <- Map.get(calls, name, []),
          length(args) > index,
          uniq: true,
          do: literal_atom(Enum.at(args, index))

    if atoms == [], do: [:__dynamic__], else: atoms
  end

  defp literal_atom(atom) when is_atom(atom), do: atom
  defp literal_atom(_other), do: :__dynamic__

  defp bindings(var, ast) do
    {_, acc} =
      Macro.prewalk(ast, [], fn
        {kind, _, [{name, _, params} | _]} = node, acc
        when kind in [:def, :defp] and is_atom(name) and is_list(params) ->
          case parameter_index(params, var) do
            nil -> {node, acc}
            index -> {node, [{name, index} | acc]}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(acc)
  end

  defp parameter_index(params, var) do
    Enum.find_index(params, fn
      {^var, _, nil} -> true
      {:\\, _, [{^var, _, nil}, _]} -> true
      _ -> false
    end)
  end

  # Every call by name, with the definition heads skipped: a head's own
  # parameter list is not a call site, and reading it as one turns every
  # resolved domain into `:__dynamic__`.
  defp call_sites(ast) do
    {_, acc} =
      Macro.prewalk(ast, %{}, fn
        {kind, _, [_head | body]}, acc when kind in [:def, :defp] ->
          {{:__block__, [], body}, acc}

        {name, _, args} = node, acc when is_atom(name) and is_list(args) ->
          {node, Map.update(acc, name, [args], &[args | &1])}

        node, acc ->
          {node, acc}
      end)

    acc
  end

  defp attributes(ast) do
    {_, acc} =
      Macro.prewalk(ast, %{}, fn
        {:@, _, [{name, _, [value]}]} = node, acc when is_atom(name) ->
          {node, Map.put(acc, name, value)}

        node, acc ->
          {node, acc}
      end)

    acc
  end

  defp source_files(), do: SourceWalk.files(["lib/flezha"], [".ex"])
end
