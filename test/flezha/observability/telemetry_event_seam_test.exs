defmodule Flezha.Observability.TelemetryEventSeamTest do
  @moduledoc """
  The export seam: an event name the tree writes reaches an exporter, or the
  bridge says why it does not.

  Three clauses walk `lib/flezha` and the Elixir SDK's `sdk/elixir/lib` and fail
  naming what they found:

    1. **Coverage** - every `[:flezha, …]` event NAME the tree writes is a key
       of the bridge's merged mapping or a member of its unexported list, whose
       length is pinned as a ceiling and whose every entry carries a reason.
       Both directions: a mapped event nothing writes any more is named too.
    2. **Readability** - a `:telemetry.execute` whose event argument this walk
       cannot read is one of three named forwarders, and a fourth fails.
    3. **Derivation** - every event a detector entry declares is bridged by
       that declaration, with no line in the hand-kept half.
    4. **Sentences** - a bridged line's sentence template names only keys the
       line's emit sites write, wherever this walk can read a site's keys.
    5. **Levels** - a line that is also its event's log line names one of
       `Logger`'s levels.

  The name is judged where it is WRITTEN, not only at the `:telemetry.execute`
  call. Three names are assembled into a tuple in one module and executed in
  another, and a provider's path is assembled from its own identifier, so a walk
  that reads only the execute call sees neither. It saw neither for a long time,
  and eight live event names reached no exporter while this gate was green:
  `docs/dev_notes/telemetry-bridge.md`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Convergence.Probe.Table
  alias Flezha.Observability.{SignalWords, TelemetryBridge}
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
  # clause below, because a forwarder whose writer this walk never reads would be
  # invisible exactly the way these three were.
  @forwarders %{
    "lib/flezha/plugin/provider_behaviour.ex" =>
      "the provider family's one constructor; `provider_events/1` reads its call sites",
    "lib/flezha/runner/control_channel/commits.ex" =>
      "`commit.telemetry`, whose names are written literally in the same module",
    "lib/flezha/state/wal_writer.ex" =>
      "the outbox flush tuple, whose name is written literally in `state/task_journal.ex`"
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

  describe "clause 2: every execute site is readable, or a named forwarder" do
    test "an execute site whose event name this walk cannot read is a named forwarder" do
      offenders =
        for file <- source_files(),
            not Map.has_key?(@forwarders, file),
            ast = SourceWalk.ast(file),
            {line, raw} <- executes(ast),
            not readable?(raw, ast),
            do: "#{file}:#{line} #{Macro.to_string(raw)}"

      assert offenders == [],
             "a `:telemetry.execute` whose event name this walk cannot read: " <>
               Enum.join(offenders, "; ")

      assert map_size(@forwarders) <= 3,
             "the forwarder list may only shrink: a name this walk cannot read is " <>
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

  describe "clause 4: a sentence names only keys its emit sites write" do
    test "every key a bridged line's template names is written where its event is emitted" do
      sites = emit_sites()

      for {event, {source, name, _kind, words}} <- TelemetryBridge.lines(),
          is_list(words),
          sentence = Keyword.get(words, :sentence) do
        {:ok, placeholders} = SignalWords.placeholders(sentence)
        needed = MapSet.new(placeholders, fn {key, _noun} -> String.to_atom(key) end)
        at_event = for {^event, keys, where} <- sites, do: {keys, where}

        missing =
          for {{known, complete?}, where} <- at_event,
              complete?,
              not MapSet.subset?(needed, known),
              do: "#{where} lacks #{inspect(MapSet.to_list(MapSet.difference(needed, known)))}"

        assert missing == [],
               "#{source}.#{name}'s sentence #{inspect(sentence)}: " <> Enum.join(missing, "; ")

        assert Enum.any?(at_event, fn {{known, _}, _} -> MapSet.subset?(needed, known) end),
               "#{source}.#{name}'s sentence #{inspect(sentence)} names keys no readable " <>
                 "emit site of #{inspect(event)} writes; write it only where the keys are read"
      end
    end
  end

  describe "clause 5: a leveled line names a Logger level" do
    test "every line's level is one of Logger's" do
      offenders =
        for {event, {_source, _name, _kind, words}} <- TelemetryBridge.lines(),
            is_list(words),
            Keyword.has_key?(words, :level),
            not SignalWords.valid_word?(:level, words[:level]),
            do: "#{inspect(event)} #{inspect(words[:level])}"

      assert offenders == [], "levels Logger does not have: " <> Enum.join(offenders, "; ")
    end
  end

  # Every emit site with the keys it writes, as `{event, {known, complete?}, where}`:

  # each `:telemetry.execute` and each provider-family `ProviderBehaviour.emit`.
  # A site inside a function whose parameters carry the event or the keys is read
  # once per call of that function, with the call's arguments in place of the
  # parameters, so a helper's verb is paired with the labels that same call passes.
  defp emit_sites() do
    for file <- source_files(),
        not Map.has_key?(@declaring, file),
        ast = SourceWalk.ast(file),
        defs = definitions(ast),
        calls = call_sites(ast),
        {name, params, call, line} <- emitting_calls(defs),
        binding <- bindings_for(name, params, call, calls),
        {events, measurements, metadata} = emitted(substitute(call, binding), ast),
        event <- events,
        do:
          {event, union(keys(measurements, defs, 3), keys(metadata, defs, 3)), "#{file}:#{line}"}
  end

  # Every clause of every function, as `{name, params, blocks}` (its `do`
  # block and any `rescue`, `catch` or `after`), and the module's attributes.
  defp definitions(ast) do
    {_, clauses} =
      Macro.prewalk(ast, [], fn
        {kind, _, [head, [{:do, _} | _] = blocks]} = node, acc when kind in [:def, :defp] ->
          {name, params} = head_of(head)
          {node, [{name, params, blocks} | acc]}

        node, acc ->
          {node, acc}
      end)

    %{clauses: clauses, attributes: attributes(ast)}
  end

  defp emitting_calls(%{clauses: clauses}) do
    for {name, params, body} <- clauses,
        {call, line} <- emits_in(body),
        do: {name, params, call, line}
  end

  defp emits_in(body) do
    {_, found} =
      Macro.prewalk(body, [], fn
        {{:., meta, [:telemetry, :execute]}, _, [_, _, _]} = call, acc ->
          {call, [{call, Keyword.get(meta, :line)} | acc]}

        {{:., meta, [{:__aliases__, _, mod}, :emit]}, _, [_, _, _, _]} = call, acc ->
          if List.last(mod) == :ProviderBehaviour,
            do: {call, [{call, Keyword.get(meta, :line)} | acc]},
            else: {call, acc}

        node, acc ->
          {node, acc}
      end)

    found
  end

  defp head_of({:when, _, [head | _]}), do: head_of(head)
  defp head_of({name, _, params}) when is_list(params), do: {name, params}
  defp head_of({name, _, _}), do: {name, []}

  # One binding per call of the enclosing function when the site reads one of its
  # parameters, else one empty binding.
  defp bindings_for(name, params, call, calls) do
    vars = for {var, _, ctx} <- params, is_atom(var) and is_atom(ctx), do: var
    used = vars_in(call)

    bound =
      if Enum.any?(vars, &(&1 in used)) do
        for args <- Map.get(calls, name, []),
            length(args) == length(params),
            do:
              for(
                {{var, _, ctx}, arg} <- Enum.zip(params, args),
                is_atom(var) and is_atom(ctx),
                into: %{},
                do: {var, arg}
              )
      else
        []
      end

    if bound == [], do: [%{}], else: Enum.uniq(bound)
  end

  defp vars_in(ast) do
    {_, acc} =
      Macro.prewalk(ast, [], fn
        {var, _, ctx} = node, acc when is_atom(var) and is_atom(ctx) -> {node, [var | acc]}
        node, acc -> {node, acc}
      end)

    acc
  end

  defp substitute(ast, binding) when binding == %{}, do: ast

  # Post-order, so an argument that names the parameter it replaces is not
  # substituted again.
  defp substitute(ast, binding) do
    Macro.postwalk(ast, fn
      {var, _, ctx} = node when is_atom(var) and is_atom(ctx) -> Map.get(binding, var, node)
      node -> node
    end)
  end

  defp emitted({{:., _, [:telemetry, :execute]}, _, [event, measurements, metadata]}, ast),
    do: {assembled(event, ast), measurements, metadata}

  defp emitted({{:., _, [_, :emit]}, _, [provider, verb, measurements, metadata]}, ast) do
    events =
      for segment <- verb_domain(verb, ast),
          do: [:flezha, :plugin, provider_id(provider, attributes(ast)), segment]

    {events, measurements, metadata}
  end

  # The keys a map expression writes: `{known, complete?}`. `complete?` is false
  # where part of the map is built from something this walk cannot read. A local
  # function is followed `depth` calls deep, so a recursive one cannot loop.
  defp keys(_expr, _defs, 0), do: {MapSet.new(), false}

  defp keys({:%{}, _, [{:|, _, [base, pairs]}]}, defs, depth),
    do: union(keys(base, defs, depth), keys({:%{}, [], pairs}, defs, depth))

  defp keys({:%{}, _, pairs}, _defs, _depth) do
    atoms = for {key, _} <- pairs, is_atom(key), do: key
    {MapSet.new(atoms), length(atoms) == length(pairs)}
  end

  defp keys({{:., _, [{:__aliases__, _, [:Map]}, :put]}, _, [base, key, _]}, defs, depth)
       when is_atom(key) do
    {known, complete?} = keys(base, defs, depth)
    {MapSet.put(known, key), complete?}
  end

  defp keys({{:., _, [{:__aliases__, _, [:Map]}, :merge]}, _, [left, right]}, defs, depth),
    do: union(keys(left, defs, depth), keys(right, defs, depth))

  defp keys({:@, _, [{name, _, nil}]}, defs, depth),
    do: keys(Map.get(defs.attributes, name), defs, depth)

  defp keys({name, _, args}, defs, depth) when is_atom(name) and is_list(args) do
    bodies =
      for {^name, params, blocks} <- defs.clauses, length(params) == length(args), do: blocks

    case bodies do
      [[do: body]] -> keys(body, defs, depth - 1)
      _ -> {MapSet.new(), false}
    end
  end

  defp keys(_other, _defs, _depth), do: {MapSet.new(), false}

  defp union({a, a_complete?}, {b, b_complete?}),
    do: {MapSet.union(a, b), a_complete? and b_complete?}

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
  # Anything else is a forwarder, and every forwarder is named.
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

  # Every call by name, with the definition heads and module attributes
  # skipped: a head's own parameter list and a `@spec`'s argument types are not
  # call sites, and reading either as one turns every resolved domain into
  # `:__dynamic__`.
  defp call_sites(ast) do
    {_, acc} =
      Macro.prewalk(ast, %{}, fn
        {kind, _, [_head | body]}, acc when kind in [:def, :defp] ->
          {{:__block__, [], body}, acc}

        {:@, _, _}, acc ->
          {:ok, acc}

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

  # The Elixir SDK's own events are emitted in this VM too, by the in-node SDK
  # and by the pure modules the controller shares with it.
  defp source_files(), do: SourceWalk.files(["lib/flezha", "sdk/elixir/lib"], [".ex"])
end
