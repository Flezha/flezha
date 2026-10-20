defmodule Flezha.Observability.TelemetryBridge do
  @moduledoc """
  Single dispatching `:telemetry` handler that lifts existing
  `[:flezha, ...]` telemetry events onto the observability bus.

  No producer call-site is rewritten. The mapping has three halves: the
  hand-kept lifecycle half in `@lines`; a half DERIVED from the convergence
  loop's detector table, so every event a detector entry declares is bridged by
  that declaration and a new detector needs no edit here; and a half derived at
  start from the providers packages installed (`provider_mapping/0`), so a
  package's events are exported under its provider's name with no edit here.
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

  # The hand-kept half: event => {source, name, kind, words}. The words never
  # reach the per-event path, which reads `@all_mapping`, the same lines
  # without them.
  @lines Lines.hand_kept()

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

  # Every source a line names: where it sits, its plain title and what it is.
  @sources %{
    flezha_boot:
      {:controller, "Controller start-up",
       "Bringing relays back and starting each namespace's hub when the controller starts."},
    flezha_shutdown:
      {:controller, "Controller shutdown",
       "A graceful stop of the controller, step by step, moving relays off first."},
    flezha_error:
      {:controller, "Undocumented errors",
       "Errors a caller got without a documented code; each one is a defect to report."},
    flezha_state:
      {:storage, "Saved relay state",
       "Each relay's state saved on this node: writes, clean-ups and repairs."},
    flezha_durability:
      {:storage, "Remote backups",
       "Copies of relay state sent to remote storage and restored from it."},
    flezha_relay:
      {:relays, "Relay lifecycle", "Relays starting, stopping, and halting after a failure."},
    flezha_runner:
      {:relays, "Runner connections",
       "Runners opening and closing their connection to the controller, and the ones it turned away."},
    flezha_call:
      {:relays, "Relay calls", "Calls made to relays: answers, timeouts and refusals."},
    flezha_steps:
      {:relays, "Steps sessions", "Turns of steps relays that failed and were set aside."},
    flezha_dlq:
      {:relays, "Dead letters",
       "Messages that failed and were set aside in dead-letter queues, and those that expired."},
    flezha_stream:
      {:relays, "Runner streams",
       "The streams that carry messages between runners and their relays."},
    flezha_schedule: {:relays, "Schedules", "Schedules that start relays on a timetable."},
    flezha_health:
      {:relays, "Health findings",
       "What the health checks found about relays, runners and plugins."},
    flezha_convergence:
      {:relays, "Self-repair",
       "The loop that keeps running relays matching what was asked, and its retries."},
    flezha_correction:
      {:relays, "Refused corrections",
       "Corrections to a relay that the controller would not apply."},
    flezha_plugin:
      {:plugins, "Plugin lifecycle", "Plugins moving between the phases of their life."},
    flezha_plugin_packages:
      {:plugins, "Plugin packages",
       "Plugin packages the controller loaded or refused when it started."},
    flezha_hooks:
      {:plugins, "Notices to controller plugins",
       "Notices handed to controller plugins, such as a message becoming a dead letter."},
    flezha_ws:
      {:plugins, "Websocket plugin",
       "The websocket plugin: its connections, subscriptions and sends."},
    flezha_relay_forward:
      {:plugins, "Relay forward plugin",
       "The plugin that hands a relay's messages to the next relay."},
    flezha_http_poll:
      {:plugins, "HTTP poll plugin",
       "The plugin that reads a URL on a timer and hands what changed to a relay."},
    flezha_http_push:
      {:plugins, "HTTP push plugin", "The plugin that sends a relay's output to a URL."},
    flezha_hook_webhook:
      {:plugins, "Webhook plugin", "The plugin that sends controller notices to a URL."},
    flezha_file_source:
      {:plugins, "File source plugin", "The plugin that reads lines from a file into a relay."},
    flezha_file_sink:
      {:plugins, "File sink plugin", "The plugin that writes a relay's output to a file."},
    flezha_postgres_poll:
      {:plugins, "Postgres poll plugin",
       "The plugin that reads new rows from a table into a relay."}
  }

  for {_event, {source, _name, _kind}} <- @all_mapping, not Map.has_key?(@sources, source) do
    raise CompileError,
      description: "bridged source #{inspect(source)} has no area and description in @sources"
  end

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
    (Map.values(@all_lines) ++ Map.values(providers))
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.map(fn {source, lines} ->
      {area, title, description} =
        Map.get_lazy(@sources, source, fn -> package_source(source) end)

      %{
        name: source,
        kinds: lines |> Enum.map(&elem(&1, 2)) |> Enum.uniq(),
        flavour: :declarative,
        area: area,
        title: title,
        description: description,
        signals:
          Source.signals(
            for {_source, name, kind, words} <- Enum.uniq_by(lines, &elem(&1, 1)),
                do: {name, {kind, words}}
          )
      }
    end)
  end

  defp package_source(source) do
    name = source |> Atom.to_string() |> String.replace_prefix("flezha_", "")

    {:plugins, "#{SignalWords.humanize(name)} plugin",
     "The #{name} plugin, installed from a package."}
  end

  @doc """
  The half derived from the provider table as it stands, for the providers a
  package installed: each one's shared verbs and its own declared ones under
  `[:flezha, :plugin, <name>, verb]`, `name` being the one its declaration
  answers and `ProviderBehaviour.emit/4` writes, sourced `flezha_<name>`, less
  every event `mapping/0` already names. A first-party provider's events are
  hand-kept lines, which the suite judges against what the tree writes. Read
  once at bridge start, which a package's provider row precedes.
  """
  @spec provider_mapping() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def provider_mapping(), do: strip(provider_lines())

  @doc "`provider_mapping/0` with each line's words: a meaning, or the long form."
  @spec provider_lines() ::
          %{[atom()] => {atom(), atom(), Signal.kind(), String.t() | keyword()}}
  def provider_lines() do
    for {_id, module} <- Providers.installed(),
        Code.ensure_loaded?(module),
        decl = module.__provider__(),
        {verb, kind, words} <- @shared_verbs ++ own_verbs(decl),
        # A name from code a verified package ships, never input.
        event = ProviderBehaviour.event(String.to_atom(decl.name), verb),
        not Map.has_key?(@all_mapping, event),
        into: %{},
        do: {event, {:"flezha_#{decl.name}", verb, kind, words}}
  end

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
