defmodule Flezha.Observability.TelemetryBridge do
  @moduledoc """
  Single dispatching `:telemetry` handler that lifts existing
  `[:flezha, ...]` telemetry events onto the observability bus.

  No producer call-site is rewritten. The mapping has three halves: the
  hand-kept half, kept under each source in `Lines.sources/0`; a half DERIVED
  from the convergence loop's detector table, so every event a detector entry
  declares is bridged by that declaration and a new detector needs no edit here;
  and a half derived at start from the provider table (`provider_mapping/0`), so
  a provider's events are exported under its name from its declaration, with no
  edit here.
  Where halves name one event the hand-kept line wins, because its source, name
  and kind are a deliberate choice a derivation cannot know.

  A line whose words carry a `level:` is also the event's log line: its handler
  writes `Flezha.Observability.Signal.line/1` at that level in the emitting
  process, marked `flezha_event: true` so `LoggerHandler` does not put the fact
  on the bus twice. With observability disabled the bridge attaches only these.

  Every `[:flezha, …]` event NAME the tree writes must be a key of the merged
  mapping or a member of `unexported/0`, which carries the reason it reaches no
  exporter. The suite judges the name where it is written rather than at the
  `:telemetry.execute` call, because three names are assembled into a tuple in one
  module and executed in another, and a provider's path is assembled from its
  own identifier, so an emitter cannot be added without an answer. It pins
  `unexported/0`'s length so the list can only shrink, and `unwritten/0` names
  the mapped events nothing writes any more.

  A broken bridge must never crash a producer: handler bodies are
  wrapped in `try`. On error the `:telemetry_bridge_failures` counter
  increments and the error is logged once per unique error class.
  """

  use GenServer, restart: :transient

  require Logger

  alias Flezha.Convergence.Probe.Table
  alias Flezha.Observability.{Bus, Counters, Registry, Signal, SignalWords, Source}
  alias Flezha.Observability.TelemetryBridge.Lines
  alias Flezha.Plugin.{ProviderBehaviour, Providers}

  @handler_id :flezha_observability_telemetry_bridge

  # Every source the bridge registers: area, title, description and its
  # hand-kept lines.
  @sources Lines.sources()

  # The hand-kept half: event => {source, name, kind, words}. The words never
  # reach the per-event path, which reads `@all_mapping`, the same lines
  # without them.
  @lines for {source, {_area, _title, _description, lines}} <- @sources,
             {event, name, kind, words} <- lines,
             into: %{},
             do: {event, {source, name, kind, words}}

  # Every `[:flezha, …]` event name the tree writes that reaches no exporter,
  # grouped under the reason it has none. A ratchet: the suite pins the total,
  # so an event may leave a group and none may join.
  @unexported %{
    # One signal per durable steps relay commit, at the data plane's rate, with
    # `task_idx` and `iter` unique to each. The commit's outcome rides the ack
    # the SDK observes and the durability counters, not a per-commit event.
    commit_path: [
      [:flezha, :state, :iter_commit],
      [:flezha, :state, :task_commit]
    ],
    # The convergence Loop attaches to this itself: it is a control input, and
    # the Loop must not read what an exporter publishes.
    control_input: [
      [:flezha, :convergence, :tick]
    ],
    # An action the controller took, or a step it completed, and never a fault
    # it detected, which is the line the mapping draws. Each is already
    # answered by an operations surface that reports the resulting state: the
    # convergence snapshot, `/healthz`, `flz boot await`, the schedule listing.
    # Where one of these has a failing sibling, the sibling IS mapped
    # (`wal_pull_failed`, `spawn_failed`, `compaction_failed`).
    lifecycle: [
      [:flezha, :boot, :reconcile_round],
      [:flezha, :cluster, :ready],
      [:flezha, :correction, :completed],
      [:flezha, :correction, :retention_derived],
      [:flezha, :hub, :pid_removed],
      [:flezha, :relay, :requested_spawn],
      [:flezha, :schedule, :dormant],
      [:flezha, :schedule, :fired],
      [:flezha, :state, :compaction_completed],
      [:flezha, :state, :schema_migration],
      [:flezha, :state, :wal_moved],
      [:flezha, :state, :wal_pulled],
      [:flezha, :steps, :completed],
      [:flezha, :steps, :rehydrating],
      [:flezha, :steps, :rewind]
    ]
  }

  # Mapped events nothing in `lib/flezha` writes any more, each with the reason
  # the line stays. A dead line costs nothing at run time and removing one is a
  # published-surface decision, so the suite pins the set rather than the count.
  @unwritten %{}

  # Derived at COMPILE time from the detector table, so adding a detector
  # recompiles this module and its event is bridged with no edit here.
  @derived_lines Map.new(Table.event_descriptions(), fn {event, words} ->
                   {event, {:flezha_health, List.last(event), :event, words}}
                 end)

  @derived Map.new(@derived_lines, fn {event, {source, name, kind, _words}} ->
             {event, {source, name, kind}}
           end)

  @all_lines Map.merge(@derived_lines, @lines)
  @all_mapping Map.new(@all_lines, fn {event, {source, name, kind, _words}} ->
                 {event, {source, name, kind}}
               end)

  # Each source's area, title, description and signals, its derived lines
  # under its hand-kept ones.
  @groups Map.new(@sources, fn {source, {area, title, description, _lines}} ->
            signals =
              for {_event, {^source, name, kind, words}} <- @all_lines, do: {name, {kind, words}}

            {source, {area, title, description, Enum.uniq_by(signals, &elem(&1, 0))}}
          end)

  # The controller hooks' lines: plugin notices, not telemetry, which only the
  # sinks naming their source hear (`Bus.emit_named/1`).
  @hook_mapping for {_event, {:flezha_hooks, _name, _kind}} = row <- @all_mapping,
                    into: %{},
                    do: row

  # The shared provider verbs: a failure is read one at a time, the rest are counts.
  @shared_verbs for {verb, words} <- Lines.shared_verbs(),
                    do: {verb, if(verb == :failed, do: :event, else: :metric), words}

  if Keyword.keys(Lines.shared_verbs()) != ProviderBehaviour.verbs() do
    raise CompileError,
      description: "the shared verbs' words do not match ProviderBehaviour.verbs/0"
  end

  # ---------- Public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  What the bridge registers for each source its lines name: kinds, area,
  title, description and every signal with its words. `providers` are the
  provider-derived lines, as `provider_lines/0` answers them. Registered on
  bridge start so `Bus` enforces cardinality budgets against them and a
  reader can tell what each one says.
  """
  @spec registrations(map()) :: [map()]
  def registrations(providers \\ provider_lines()) do
    providers
    |> Map.values()
    |> Enum.group_by(&elem(&1, 0), fn {_source, name, kind, words} -> {name, {kind, words}} end)
    |> Enum.reduce(@groups, fn {source, signals}, groups ->
      Map.update(groups, source, provider_source(source, signals), fn {area, title, text, own} ->
        {area, title, text, own ++ signals}
      end)
    end)
    |> Enum.map(fn {source, {area, title, description, signals}} ->
      Source.registration(
        name: source,
        area: area,
        title: title,
        description: description,
        signals: Enum.uniq_by(signals, &elem(&1, 0))
      )
    end)
  end

  # A provider source with no hand-kept group says what its declaration says,
  # else what a package that declares nothing gets.
  defp provider_source(source, signals) do
    name = source |> Atom.to_string() |> String.replace_prefix("flezha_", "")

    decl =
      Enum.find_value(Providers.all(), %{}, fn {_id, module} ->
        decl = module.__provider__()
        if decl.name == name, do: decl
      end)

    {:plugins, Map.get(decl, :title) || "#{SignalWords.humanize(name)} plugin",
     Map.get(decl, :description) || "The #{name} plugin, installed from a package.", signals}
  end

  @doc """
  The half derived from the provider table as it stands: for each provider its
  own declared verbs and the shared ones (a first-party provider only those its
  `emits:` names, a package every one) under `[:flezha, :plugin, <name>, verb]`,
  `name` being the one its declaration answers and `ProviderBehaviour.emit/4`
  writes, sourced `flezha_<name>`, less every event `mapping/0` already names.
  Read once at bridge start, which a package's provider row precedes.
  """
  @spec provider_mapping() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def provider_mapping(), do: strip(provider_lines())

  @doc "`provider_mapping/0` with each line's words: a meaning, or the long form."
  @spec provider_lines() ::
          %{[atom()] => {atom(), atom(), Signal.kind(), String.t() | keyword()}}
  def provider_lines() do
    first_party = Providers.first_party()

    for {id, module} <- Providers.all(),
        Code.ensure_loaded?(module),
        decl = module.__provider__(),
        {verb, kind, words} <-
          shared_verbs(decl, Map.has_key?(first_party, id)) ++ own_verbs(decl),
        # A name from a declaration the release or a verified package ships, never input.
        event = ProviderBehaviour.event(String.to_atom(decl.name), verb),
        not Map.has_key?(@all_mapping, event),
        into: %{},
        do: {event, {:"flezha_#{decl.name}", verb, kind, words}}
  end

  # A package built before `emits:` existed carries no such key and writes every
  # shared verb; a level adds the line's log level.
  defp shared_verbs(decl, first_party?) do
    emits = Map.get(decl, :emits, [])

    for {verb, kind, words} <- @shared_verbs,
        not first_party? or Keyword.has_key?(emits, verb),
        do: {verb, kind, leveled(words, emits[verb])}
  end

  defp leveled(words, nil), do: words
  defp leveled(words, level), do: [{:level, level} | words]

  # A package built before `telemetry:` existed declares no own verbs, and one
  # built before a verb could carry a meaning declares a bare kind.
  defp own_verbs(decl) do
    for {verb, declared} <- Map.get(decl, :telemetry, []) do
      case declared do
        {kind, words} -> {verb, kind, words}
        kind -> {verb, kind, "No description from the plugin."}
      end
    end
  end

  defp strip(lines),
    do:
      Map.new(lines, fn {event, {source, name, kind, _words}} ->
        {event, {source, name, kind}}
      end)

  @doc false
  @spec handler_id() :: atom()
  def handler_id(), do: @handler_id

  @doc """
  The bridged events: the derived half under the hand-kept one. A detector's
  declared event maps to `{:flezha_health, <last segment>, :event}` unless the
  hand-kept half already names it.
  """
  @spec mapping() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def mapping(), do: @all_mapping

  @doc "`mapping/0` with each line's words, as `{source, name, kind, words}`."
  @spec lines() :: %{[atom()] => {atom(), atom(), Signal.kind(), String.t() | keyword()}}
  def lines(), do: @all_lines

  @doc """
  The Markdown table a telemetry page carries for `sources`: every signal each
  registers, its kind, the series an exporter names a metric by, and what it
  says.
  """
  @spec render_table([atom()]) :: String.t()
  def render_table(sources) do
    registered = Map.new(registrations(), &{&1.name, &1.signals})

    rows =
      for source <- sources,
          signal <- Enum.sort_by(Map.fetch!(registered, source), & &1.name),
          do:
            "| `#{source}` | `#{signal.name}` | #{signal.kind} | #{series(source, signal)} " <>
              "| #{signal.description} |\n"

    "| Source | Signal | Kind | Series | Meaning |\n| --- | --- | --- | --- | --- |\n" <>
      Enum.join(rows)
  end

  defp series(source, %{kind: :metric, name: name}),
    do: "`#{Signal.exposition_name(source, name, "_")}`"

  defp series(_source, _signal), do: ""

  @doc "The half derived from the convergence loop's detector declarations."
  @spec derived() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def derived(), do: @derived

  @doc """
  Event names the tree writes that reach no exporter. A ratchet: the suite pins
  the total, so an event can leave this list and none may join.
  """
  @spec unexported() :: [[atom()]]
  def unexported(), do: Enum.concat(Map.values(@unexported))

  @doc "Why `event` reaches no exporter, or `nil` when it does."
  @spec unexported_reason([atom()]) :: atom() | nil
  def unexported_reason(event) do
    Enum.find_value(@unexported, fn {reason, events} -> if event in events, do: reason end)
  end

  @doc "The reasons an event may carry no bridge line."
  @spec reasons() :: [atom()]
  def reasons(), do: Map.keys(@unexported)

  @doc "Mapped events nothing writes any more, each with the reason its line stays."
  @spec unwritten() :: %{[atom()] => binary()}
  def unwritten(), do: @unwritten

  # ---------- :telemetry handler entry point ----------

  @doc false
  def handle_event(event, measurements, metadata, _config) do
    try do
      do_handle_event(event, measurements, metadata)
    rescue
      e -> handle_error(event, e, __STACKTRACE__)
    catch
      kind, reason -> handle_error(event, {kind, reason}, __STACKTRACE__)
    end

    :ok
  end

  defp do_handle_event(event, measurements, metadata) do
    case Map.fetch(@all_mapping, event) do
      {:ok, {source, name, kind}} ->
        signal =
          Signal.new(%{
            kind: kind,
            source: source,
            name: name,
            measurements: measurements,
            metadata: metadata
          })

        Bus.emit(signal)

      :error ->
        :ok
    end
  end

  # A provider-derived event, whose handler carries its own line.
  @doc false
  def handle_line(event, measurements, metadata, line),
    do: emit_line(&Bus.emit/1, event, measurements, metadata, line)

  # A controller hook's event, which reaches only the sinks naming its source.
  @doc false
  def handle_hook(event, measurements, metadata, line),
    do: emit_line(&Bus.emit_named/1, event, measurements, metadata, line)

  # A leveled line's event, written as its log line and then emitted.
  @doc false
  def handle_logged(event, measurements, metadata, {source, name, kind, level}),
    do: emit_line(&log_and_emit(level, &1), event, measurements, metadata, {source, name, kind})

  defp log_and_emit(level, signal) do
    Logger.log(level, fn -> Signal.line(signal) end, flezha_event: true)
    Bus.emit(signal)
  end

  defp emit_line(emit, event, measurements, metadata, {source, name, kind}) do
    emit.(
      Signal.new(%{
        kind: kind,
        source: source,
        name: name,
        measurements: measurements,
        metadata: metadata
      })
    )
  rescue
    e -> handle_error(event, e, __STACKTRACE__)
  catch
    kind, reason -> handle_error(event, {kind, reason}, __STACKTRACE__)
  end

  defp handle_error(event, error, stacktrace) do
    Counters.bump(:telemetry_bridge_failures)
    class = Counters.error_class(error)

    if Counters.first_seen?({:telemetry_bridge, event, class}) do
      Logger.error(
        "[observability.telemetry_bridge] error event=#{inspect(event)} " <>
          "class=#{inspect(class)} error=#{inspect(error)} " <>
          "stack=#{Exception.format_stacktrace(stacktrace)}"
      )
    end
  end

  # ---------- GenServer ----------

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    provider_lines = provider_lines()
    # A leveled line has a handler of its own carrying it, so the merged
    # handler's events cost what they did.
    logged = logged(Map.merge(@all_lines, provider_lines))
    plain = Map.drop(provider_lines, Map.keys(logged))

    handlers =
      if Keyword.get(opts, :lines_only, false),
        do: [handle_logged: logged],
        else: attach_bus(provider_lines, plain, logged)

    # A handler's config is copied out of telemetry's table on every execute of
    # its events, so each of these carries only its own line.
    for {handler, lines} <- handlers,
        {event, line} <- lines,
        do:
          :ok =
            :telemetry.attach(
              {@handler_id, event},
              event,
              Function.capture(__MODULE__, handler, 4),
              line
            )

    {:ok, %{line_events: Enum.flat_map(handlers, fn {_handler, lines} -> Map.keys(lines) end)}}
  end

  defp attach_bus(provider_lines, plain, logged) do
    # A refused registration leaves its signals unbudgeted and unlisted, so it
    # is said rather than dropped.
    for entry <- registrations(provider_lines),
        {:error, reason} <- [Registry.register_source(entry)] do
      Logger.error(
        "[observability.telemetry_bridge] source #{inspect(entry.name)} not registered: " <>
          inspect(reason)
      )
    end

    compiled =
      @all_mapping
      |> Map.drop(Map.keys(@hook_mapping) ++ Map.keys(logged))
      |> Map.keys()

    :ok = :telemetry.attach_many(@handler_id, compiled, &__MODULE__.handle_event/4, nil)
    [handle_line: strip(plain), handle_hook: @hook_mapping, handle_logged: logged]
  end

  defp logged(lines) do
    for {event, {source, name, kind, words}} <- lines,
        level = is_list(words) && words[:level],
        level,
        into: %{},
        do: {event, {source, name, kind, level}}
  end

  @impl true
  def terminate(_reason, state) do
    _ = :telemetry.detach(@handler_id)
    Enum.each(state.line_events, &:telemetry.detach({@handler_id, &1}))
    :ok
  end
end
