defmodule Flezha.Service.Relay do
  @moduledoc """
  Published service-API surface for namespaces, relays and their
  partitions: every public function takes a `Flezha.Service.CallerContext` as
  its last argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.

  `Flezha.Service.RelayManager` is the internal manager and stays internal;
  this is the half a consumer is allowed to see. They are two modules rather
  than one because the manager is within a hundred lines of the repository's
  800-line file ceiling, which the charter says wins over collocation.

  One question is asked one way here. `list_namespaces/1` is the only
  spelling a consumer has for "which namespaces are there, and which are
  running": the reads below it take no caller and answer the whole cluster,
  which is what the controller's own machinery needs and what a tenant-bound
  caller must not be handed.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ
  alias Flezha.AuthZ.Namespace
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.Scope
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Verdict
  alias Flezha.Relay.Builder
  alias Flezha.Relay.Calls.Token
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Lookup
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Relay.Replay
  alias Flezha.Partition
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Service.CallerContext
  alias Flezha.Service.HubManager
  alias Flezha.Service.Listener
  alias Flezha.Service.Relay.Diagnose
  alias Flezha.Service.Relay.Info
  alias Flezha.Service.RelayApi
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Steps
  alias Flezha.State.TimeTravel

  @type caller_context :: CallerContext.t()

  @namespace [type: :string, doc: "The namespace."]
  @relay_id [type: :string, doc: "The relay's id."]
  @activity_figures [:tasklet, :cells, :queued, :queued_max, :admitted_total, :blocked_total] ++
                      [:dropped_newest_total, :dropped_oldest_total, :dropped_capped_total] ++
                      [:cells_created_total, :cells_evicted_total]
  @cell_figures [:key, :queued, :admitted_total, :blocked_total, :dropped_newest_total] ++
                  [:dropped_oldest_total, :last_admitted_ms]
  @message [type: :string, doc: "The input's payload."]
  @channel [type: :string, default: "", doc: "The input channel; none when empty."]

  # The fields a start reads besides the spec's own.
  @start_fields [:partitions, :plugins, :text, :channel]

  # ---------- Reads ----------

  @doc """
  The namespaces in the durable catalog the caller may be shown. That is its
  own, or all of them for an admin in `_system`, each with whether this
  controller runs a hub for it.

  Reading the running hub set alone left a namespace created a moment earlier
  invisible until something connected to it, and reported one that had been
  torn down as gone from the catalog.
  """
  @spec list_namespaces(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list_namespaces(caller_context) do
    running = MapSet.new(HubManager.get_running_namespaces())

    # `unbound: :all`: the row is `:caller`-scoped, so a context with no
    # namespace is an ungated deployment's user rather than a tenant, and
    # narrowing it to `_system` would hide the namespaces it just created.
    {:ok,
     NamespaceStore.list()
     |> Enum.map(& &1.name)
     |> Namespace.visible(caller_context, :all)
     |> Enum.sort()
     |> Enum.map(&%{namespace: &1, running: MapSet.member?(running, &1)})}
  end

  @doc "The relay ids running in `namespace`."
  @spec list_relays(String.t(), caller_context()) :: {:ok, [String.t()]} | {:error, term()}
  verb list_relays(namespace, caller_context), args: [namespace: @namespace] do
    RelayManager.get_relay_ids(namespace)
  end

  @doc """
  The relays in a namespace, one row each. Each row holds its health, runner,
  node and, for a partition group, its width and members. A group is one row,
  exactly as healthy as its sickest member: an aggregate that rounded up would
  hide the one partition that stopped serving its keys.
  """
  @spec list(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list(namespace, caller_context), verb: :list_relays, args: [namespace: @namespace] do
    rows = namespace |> Namespace.or_default() |> HubManager.relay_rows()
    plain = Enum.reject(rows, &(&1[:partition] != nil))
    {:ok, Enum.sort_by(plain ++ Enum.map(Partition.groups(rows), &group_row/1), &inspect(&1.id))}
  end

  @doc """
  Every relay in a namespace as its Overwatch reports it. It also answers every
  finished one still inside its retention window, and every partition group with
  its key-group coverage.
  """
  @spec info(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb info(namespace, caller_context), verb: :relay_info, args: [namespace: @namespace] do
    {:ok, namespace |> Namespace.or_default() |> Info.report()}
  end

  @doc """
  What is wrong with a relay and what to do about it. Answers a `verdict` of
  `ok`, `warning` or `problem`, and the findings that explain it, each a
  catalogued `diagnose.*` code with its message, details and hint. Read from the
  facts the controller already keeps; it changes nothing. Dead letters' error
  texts are shown only to a caller that may also list dead letters (`dlq.list`).
  """
  @spec diagnose(String.t(), String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb diagnose(namespace, relay_id, caller_context),
    verb: :relay_info,
    args: [namespace: @namespace, relay_id: @relay_id] do
    namespace = Namespace.or_default(namespace)
    errors? = AuthZ.allowed?(caller_context, :"dlq.list", Scope.build(namespace))
    Diagnose.report(namespace, relay_id, errors?)
  end

  @doc "One row per relay in `namespace`: node, health and partition."
  @spec relay_rows(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb relay_rows(namespace, caller_context),
    verb: :relay_info,
    args: [namespace: @namespace] do
    {:ok, HubManager.relay_rows(namespace)}
  end

  @doc "The plugin bindings each relay in `namespace` declares."
  @spec relay_plugins(String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb relay_plugins(namespace, caller_context),
    verb: :relay_info,
    args: [namespace: @namespace] do
    {:ok, HubManager.relay_bindings(namespace)}
  end

  @doc "The namespace's hub registry, as the hub reports it."
  @spec registry(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb registry(namespace, caller_context), verb: :relay_info, args: [namespace: @namespace] do
    HubManager.get_registry(namespace)
  end

  @doc "Partition coverage over `{id, range}` pairs: gaps, overlaps and total."
  @spec partition_coverage([{String.t(), term()}], caller_context()) ::
          {:ok, term()} | {:error, term()}
  verb partition_coverage(members, caller_context) when is_list(members),
    verb: :partition_report,
    args: [members: [type: :list, doc: "The `{id, range}` pairs."]] do
    {:ok, Partition.coverage(members)}
  end

  @doc """
  The partition groups `rows` describe. Each answers its group, declared width,
  members and key-group coverage.

  The report, not the row: `list/2` collapses the same rows into one row per
  group.
  """
  @spec partition_groups([map()], caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb partition_groups(rows, caller_context) when is_list(rows),
    verb: :partition_report,
    args: [rows: [type: :list, doc: "The relay rows."]] do
    {:ok, Partition.groups(rows)}
  end

  @doc "Whether `run` is a way a relay runs that this controller knows."
  @spec run_known?(term(), caller_context()) :: {:ok, boolean()} | {:error, term()}
  verb run_known?(run, caller_context),
    verb: :run_read,
    args: [run: [type: :atom, doc: "The way a relay runs."]] do
    {:ok, ClassRegistry.known_run?(run)}
  end

  @doc """
  A relay's live state, as JSON, and the change sequence it was read at.

  `cell` narrows the read to `<tasklet>` or `<tasklet>/<cell_key>`; absent, it
  covers every Cell. The sequence is part of the answer because a caller that
  renders state next to a position needs both, and reading them separately
  cannot pair them.
  """
  @spec cell_state(String.t(), String.t(), String.t() | nil, caller_context()) ::
          {:ok, binary(), term()} | {:error, term()}
  verb cell_state(namespace, relay_id, cell, caller_context),
    args: [
      namespace: @namespace,
      relay_id: @relay_id,
      cell: [
        type: :string,
        default: nil,
        doc: "`<tasklet>[/<cell_key>]`; every Cell when absent."
      ]
    ] do
    RelayManager.get_cell_state(Namespace.or_default(namespace), relay_id, selector(cell))
  end

  @doc """
  What each tasklet of a relay is doing, as its runner counts it. The runner
  hosting the relay counts, per tasklet, its live Cells, the items queued in
  total and in the deepest Cell, and the totals of items admitted, blocked waits
  and items dropped. A figure the runner does not keep is `nil`, never zero.
  Totals rise for the life of one instance; a different `instance_seq` or
  `runner` means they started again. `queued` is approximate while items arrive.
  A partition group is read one member at a time.

  `opts` carries `cells_of`, a tasklet whose Cells are answered as well under
  `cells`, and `limit`, at most how many (default 50, at most 500): the most
  recently admitting first, each with its key, queue, totals and
  `last_admitted_ms` on the clock of `read_at_ms`, beside the tasklet's
  `cells_total`. `cells` is `nil` when none were asked for, or when the runner
  keeps no per-Cell figures.
  """
  @spec activity(String.t(), String.t(), keyword(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb activity(namespace, relay_id, opts \\ [], caller_context),
    verb: :relay_info,
    args: [
      namespace: @namespace,
      relay_id: @relay_id,
      opts: [
        type: :keyword,
        default: [],
        doc: "`cells_of`, a tasklet whose Cells to answer as well, and `limit` (default 50)."
      ]
    ] do
    with {:ok, read} <- cells_read(opts),
         {:ok, raw, runner} <-
           RelayManager.get_tasklet_activity(Namespace.or_default(namespace), relay_id, read) do
      answer = RunnerCommands.TaskletActivityResponse.decode!(raw)

      {:ok,
       %{
         relay: relay_id,
         runner: runner,
         read_at_ms: answer.read_at_ms,
         instance_seq: answer.instance_seq,
         tasklets: Enum.map(answer.rows, &Map.take(&1, @activity_figures)),
         cells: cells(answer.cells)
       }}
    end
  end

  # The Cell read the runner is asked for: `cells_of` as text, `limit` as a
  # count its wire field can carry.
  defp cells_read(opts) do
    with {:ok, read} <- put_text(%{}, :cells_of, Keyword.get(opts, :cells_of), "cells_of") do
      case Keyword.get(opts, :limit) do
        nil -> {:ok, read}
        n when n in 0..0xFFFF_FFFF -> {:ok, Map.put(read, :limit, n)}
        written -> Steps.invalid_number("limit", written)
      end
    end
  end

  defp cells(nil), do: nil

  defp cells(%{tasklet: tasklet, cells_total: total, rows: rows}),
    do: %{
      tasklet: tasklet,
      cells_total: total,
      rows: Enum.map(rows, &Map.take(&1, @cell_figures))
    }

  @doc "One relay's Overwatch state."
  @spec overwatch_state(String.t(), String.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  verb overwatch_state(namespace, relay_id, caller_context),
    verb: :relay_info,
    args: [namespace: @namespace, relay_id: @relay_id] do
    RelayManager.get_overwatch_state(namespace, relay_id)
  end

  # ---------- Mutations ----------

  @doc """
  Start a relay in a namespace, answering the id it runs under. The answer also
  says whether this call started it (`:started`) or found a live instance
  already holding the id it named (`:already_running`).

  `relay` is a `Flezha.Relay.Builder` or the named fields to build one
  from, so a consumer that reached the controller over a wire never has to put
  a struct on it. The same affordance `Flezha.Service.PluginManager.enable/3`
  gives for a plugin. A spec without an id is given a generated one. The
  fields may also carry `plugins` (the plugin specs, as a list or its JSON
  text, decoded under the id the start runs under), `text` and `channel` (a
  first message, delivered as the instance's first input on the mechanism a
  tasklet's spawn uses), and `partitions`: two or more start a partition group,
  whose answer also names its `members` and those `already_running`.
  """
  @spec start(String.t(), Builder.t() | keyword(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb start(namespace, relay, caller_context),
    verb: :relay_start,
    target: &RelayManager.start_target/2,
    args: [
      namespace: @namespace,
      relay: [
        type: :keyword,
        doc:
          "`factory_id`, `id`, `init_values`, `opts`, `plugins`, `text`, `channel` and `partitions`."
      ]
    ] do
    namespace = Namespace.or_default(namespace)
    {extra, fields} = start_fields(relay)

    with {:ok, spec} <- Builder.build(fields),
         {:ok, seed} <- Builder.seed_input(extra[:text], extra[:channel]),
         {:ok, partitions} <- partitions(extra[:partitions]) do
      spec = if seed, do: %{spec | opts: Keyword.put(spec.opts, :seed_input, seed)}, else: spec
      plugins = plugins_json(extra[:plugins])

      if partitions >= 2,
        do: start_group(namespace, spec, partitions, plugins),
        else: start_one(namespace, spec, plugins)
    end
  end

  @doc """
  Stop a relay in a namespace, or a whole partition group. When `relay_id` names
  a partition group, every member stops. A steps relay is stopped cooperatively:
  it finishes its in-flight work, then completes as cancelled and is removed.
  """
  @spec stop(String.t(), String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb stop(namespace, relay_id, caller_context),
    verb: :relay_stop,
    args: [namespace: @namespace, relay_id: @relay_id] do
    namespace = Namespace.or_default(namespace)

    case PartitionGroup.members(namespace, relay_id) do
      [] -> stop_one(namespace, relay_id)
      members -> stop_group(namespace, relay_id, members)
    end
  end

  @doc """
  Inject a message into a relay's input stream. It blocks for the confirming
  verdict, so a halted or absent target is reported rather than silently
  dropped. `channel` names the input channel; `""` is none. `opts[:key]` names
  the member of a partition group that takes it, and `opts[:msg_id]` is the
  message's id, stamped as `flz.msg_id` so a relay whose input log still holds
  that id absorbs the repeat. The answer names the relay that took it and the
  relay's own verdict: `:delivered`, `:absorbed` for a duplicate, or
  `:suspended` for one journaled until the instance wakes.
  """
  @spec send(String.t(), String.t(), binary(), String.t(), keyword(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb send(namespace, relay_id, message, channel, opts \\ [], caller_context),
    verb: :relay_send,
    args: [
      namespace: @namespace,
      relay_id: @relay_id,
      message: @message,
      channel: @channel,
      opts: [
        type: :keyword,
        default: [],
        doc: "`key`, for a partition group, and `msg_id`, the message's id."
      ]
    ] do
    send_input(Namespace.or_default(namespace), relay_id, message, channel, opts)
  end

  defp send_input(namespace, relay_id, message, channel, opts) do
    with {:ok, metadata} <-
           put_text(RelayManager.channel_metadata(channel), "flz.msg_id", opts[:msg_id], "msg_id"),
         {:ok, member} <- RelayManager.resolve_member(namespace, relay_id, opts[:key]) do
      member
      |> RelayManager.send_io_confirming(namespace, message, metadata)
      |> delivered(member)
    end
  end

  @doc """
  Call `relay_id` with `message` and wait for its answer. The answer is the
  first output it caused on a sink, as
  `{:ok, payload, %{"channel" => channel}}`, or a coded error saying why none
  came.

  `opts` carries `deadline_ms` (required, a positive integer),
  `answer_channel`, the channel whose output answers, and `key`, the key whose
  member of a partition group takes the input. The call is judged as a send:
  its act is an input, and its answer is the sender's own.
  """
  @spec call(String.t(), String.t(), binary(), String.t(), keyword(), caller_context()) ::
          {:ok, binary(), map()} | {:error, term()}
  verb call(namespace, relay_id, message, channel, opts, caller_context),
    verb: :relay_call,
    args: [
      namespace: @namespace,
      relay_id: @relay_id,
      message: @message,
      channel: @channel,
      opts: [
        type: :keyword,
        doc:
          "`deadline_ms`, `answer_channel` (the channel whose output answers), and `key` for a partition group."
      ]
    ] do
    call_input(Namespace.or_default(namespace), relay_id, message, channel, opts, caller_context)
  end

  # The answer channel rides the call's metadata to the row
  # `Flezha.Relay.Calls.Inbound.admit/5` keeps, where `""` names none; the caller's
  # source names it to the relay's calls listeners.
  defp call_input(namespace, relay_id, message, channel, opts, caller_context) do
    with {:ok, deadline_ms} <- deadline(Keyword.get(opts, :deadline_ms)),
         {:ok, metadata} <-
           put_text(
             RelayManager.channel_metadata(channel),
             Token.answer_channel_key(),
             opts[:answer_channel],
             "answer_channel"
           ) do
      RelayManager.call_relay(
        namespace,
        relay_id,
        message,
        metadata,
        deadline_ms,
        Keyword.get(opts, :key),
        Atom.to_string(caller_context.source)
      )
    end
  end

  @doc """
  The factories announced in a namespace, with their APIs. Each comes with its
  description and its API: one entry per entry channel, with its format, whether
  it answers and in which format, its description, its schemas and its MCP tool
  name. With `relay_id`, only that relay's factory, as the runner hosting it
  announced it.
  """
  @spec api(String.t(), String.t(), caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb api(namespace, relay_id \\ "", caller_context),
    verb: :relay_info,
    args: [
      namespace: @namespace,
      relay_id: [
        type: :string,
        default: "",
        doc: "One relay's factory only; every one when empty."
      ]
    ] do
    namespace = Namespace.or_default(namespace)

    case relay_id do
      "" ->
        {:ok, RelayApi.list(namespace)}

      relay_id ->
        with {:ok, factory} <- RelayApi.factory_of(namespace, relay_id),
             do: {:ok, [Map.put(factory, :relay, relay_id)]}
    end
  end

  @doc """
  The graph of a relay's factory, as its runner announces it. The runner hosting
  the relay announces it; while the relay has no runner, the latest announcement
  answers. The graph holds every channel with its kind, producer, consumers,
  plugin end, call target and overflow, and every tasklet with its cell mode,
  cell keys and mailbox capacity. A runner that announced none answers
  `graph: nil` with the reason `not_announced`; `disagree` is true when the
  factory's runners announce different graphs.
  """
  @spec graph(String.t(), String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb graph(namespace, relay_id, caller_context),
    verb: :relay_info,
    args: [namespace: @namespace, relay_id: @relay_id] do
    # A node that could not be asked answers its error, never "not announced".
    with {:ok, %{graph: graph} = read} <-
           RelayApi.graph_of(Namespace.or_default(namespace), relay_id) do
      reason = if graph, do: nil, else: "not_announced"
      {:ok, Map.merge(read, %{relay: relay_id, reason: reason})}
    end
  end

  @doc """
  Call an entry channel of a relay with a value. `value` is encoded by the
  relay's API: any JSON value for a `json` channel, a string for `text`, base64
  for `bytes`, `msgpack` and `custom`. A channel that answers is called and its
  answer decoded by the answer's format, as `%{answer, answer_codec}`; one that
  does not is sent to, answering `%{accepted: true}`.

  `opts` carries `deadline_ms` (default 30000), `answer_channel`, `key`, and
  `payload`: `text` or `base64` when `value` is the payload itself, sent
  unchanged. A factory with no announced API is `api.not_advertised`, except
  for a `text` payload (`flz relay call --text`), which calls the relay as
  `call/6` does, so a runner older than the API still answers it.
  """
  @spec call_channel(String.t(), String.t(), String.t(), term(), keyword(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb call_channel(namespace, relay_id, channel, value, opts \\ [], caller_context),
    verb: :relay_call,
    args: [
      namespace: @namespace,
      relay_id: @relay_id,
      channel: [type: :string, default: "", doc: "The entry channel; the first when empty."],
      value: [type: :any, doc: "The value, as the channel's format reads it."],
      opts: [
        type: :keyword,
        default: [],
        doc:
          "`deadline_ms`, `answer_channel`, `key` for a partition group, and `payload` (`text` or `base64`)."
      ]
    ] do
    namespace = Namespace.or_default(namespace)

    opts =
      Keyword.put_new_lazy(opts, :deadline_ms, fn -> Durations.get(:call_channel_deadline_ms) end)

    payload = opts[:payload]

    with {:ok, factory} <- RelayApi.factory_of(namespace, relay_id) do
      if factory.channels == [] and payload == "text",
        do: unannounced_call(namespace, relay_id, channel, value, opts, caller_context),
        else: channel_call(namespace, relay_id, factory, channel, value, opts, caller_context)
    end
  end

  defp channel_call(namespace, relay_id, factory, channel, value, opts, caller_context) do
    with {:ok, entry} <- RelayApi.entry(factory, namespace, relay_id, channel),
         {:ok, message} <- RelayApi.encode(entry, value, opts[:payload]) do
      if entry.answer_codec,
        do:
          answered(
            call_input(namespace, relay_id, message, entry.channel, opts, caller_context),
            entry
          ),
        else: accepted(send_input(namespace, relay_id, message, entry.channel, opts))
    end
  end

  # `flz relay call --text` to a relay whose runners announced no API is
  # called as `call/6` calls it.
  defp unannounced_call(namespace, relay_id, channel, value, opts, caller_context) do
    entry = %{channel: channel, codec: "bytes", answer_codec: nil}

    with {:ok, message} <- RelayApi.encode(entry, value, opts[:payload]),
         do:
           answered(
             call_input(namespace, relay_id, message, channel, opts, caller_context),
             entry
           )
  end

  defp answered({:ok, answer, _meta}, entry),
    do: {:ok, RelayApi.decode(entry.answer_codec, answer)}

  defp answered({:error, _reason} = refused, _entry), do: refused

  defp accepted({:ok, _delivered}), do: {:ok, %{accepted: true}}
  defp accepted({:error, _reason} = refused), do: refused

  defp deadline(deadline_ms) when is_integer(deadline_ms) and deadline_ms > 0,
    do: {:ok, deadline_ms}

  defp deadline(written), do: Steps.invalid_number("deadline_ms", written)

  @doc """
  Stream every frame a relay emits, or those of one direction.

  Each frame is handed to the context's `:emit` with its parts and dead-letter
  notices; when `direction` is `input`, the frames it ingests are handed
  instead. They are handed until the window closes, the caller goes away or the
  relay ends. With `direction` `calls` it hands each call the relay takes,
  `{:call, request, metadata}`, and then how that call ended:
  `{:answered, answer, metadata}`, `{:timed_out, error, metadata}` or
  `{:refused, error, metadata}`, each carrying the call's `flz.call_id`. With
  `direction` `tally` it hands, about once per `tally_ms`,
  `{:tally, totals, %{read_at_ms:, epochs:}}`: how many frames have left on each
  output channel, counted on the relay's node with nothing sent per frame.

  `:emit` is called with `:attached` once the listener is in place, then with
  each `t:Flezha.Service.Listener.event/0`; answering `:done` ends the listen.
  A pid as `:emit` receives each as `{:flezha_emit, event}` and its exit ends
  the listen (`Flezha.Service.CallerContext.emitter/1`). `opts` carries `window_ms`.
  """
  @spec listen(String.t(), String.t(), atom(), keyword(), caller_context()) ::
          {:ok, atom()} | {:error, term()}
  verb listen(namespace, relay_id, direction, opts, caller_context),
    verb: :relay_listen,
    stream: true,
    args: [
      namespace: @namespace,
      relay_id: @relay_id,
      direction: [
        type: :enum,
        values: [:output, :input, :calls, :tally],
        default: :output,
        doc: "What to hear."
      ],
      opts: [
        type: :keyword,
        default: [],
        doc: "`window_ms`; for a tally, `tally_ms` and `channels`."
      ]
    ] do
    {emit, watch} = CallerContext.emitter(caller_context)

    Listener.listen(
      Namespace.or_default(namespace),
      relay_id,
      Keyword.take(opts, [:tally_ms, :channels]) ++
        [
          direction: direction,
          window_ms: Keyword.get(opts, :window_ms, :infinity),
          watch: watch,
          before: fn -> emit.(:attached) end
        ],
      &CallerContext.hand_on([&1], emit)
    )
  end

  @doc """
  Re-run a Cell's retained inputs from a point in the past. They run through the
  current code: rewind the Cell to `from` (a WAL seq, or an ISO 8601 time), bump
  the epoch, and replay the input log forward, so a conforming keyed output
  plugin overwrites the stale rows by natural key. A Cell of a steps relay is
  refused before anything is rewound.
  """
  @spec replay(String.t(), String.t(), String.t(), caller_context()) ::
          {:ok, map()} | {:error, term()}
  verb replay(namespace, path, from, caller_context),
    verb: :relay_replay,
    row: &replay_row/1,
    as: located,
    args: [
      namespace: @namespace,
      path: [type: :string, doc: "The Cell, as `<relay>/<tasklet>/<cell_key>`."],
      from: [type: :string, doc: "A WAL seq, or an ISO 8601 time."]
    ] do
    {relay_class, _tasklet, _key} = cell_id = located.cell_id

    with {:ok, at} <- Steps.target(from, "from"),
         :ok <- replayable(relay_class, path),
         {:ok, overwatch} <- overwatch(relay_class),
         {:ok, %{terminal_seq: seq}} <- TimeTravel.query(cell_id, at) do
      Replay.replay_from(overwatch, cell_id, seq)
      {:ok, %{cell: path, from_seq: seq}}
    end
  end

  # ---------- Helpers ----------

  defp replay_row([_namespace, path | _]), do: Steps.persisted_cell(path)

  defp replayable(relay_class, path) do
    if ClassRegistry.journals?(relay_class), do: {:error, {:steps_cell, path}}, else: :ok
  end

  defp overwatch(relay_class) do
    case Lookup.overwatch(relay_class) do
      {:ok, pid} -> {:ok, pid}
      {:error, _reason} -> {:error, {:relay_not_running, relay_class}}
    end
  end

  defp selector(nil), do: %{}

  defp selector(cell) do
    case String.split(cell, "/", parts: 2) do
      [tasklet, cell_key] -> %{tasklet: tasklet, cell_key: cell_key}
      [tasklet] -> %{tasklet: tasklet}
    end
  end

  defp start_fields(%Builder{} = spec), do: {[], spec}
  defp start_fields(fields), do: fields |> Enum.to_list() |> Keyword.split(@start_fields)

  defp partitions(nil), do: {:ok, 1}
  defp partitions(n) when is_integer(n) and n >= 1, do: {:ok, n}

  defp partitions(n),
    do: {:error, Flezha.Error.new("usage.not_positive", option: "partitions", value: inspect(n))}

  defp plugins_json(nil), do: "[]"
  defp plugins_json(text) when is_binary(text), do: text
  defp plugins_json(specs), do: Jason.encode!(specs)

  defp start_group(namespace, spec, partitions, plugins) do
    with {:ok, %{group: group, members: members} = started} <-
           PartitionGroup.start(namespace, spec, partitions, plugins) do
      {:ok,
       %{
         id: group,
         outcome: if(members == [], do: :already_running, else: :started),
         members: members,
         already_running: Map.get(started, :already_running, [])
       }}
    end
  end

  defp start_one(namespace, %{id: nil} = spec, plugins) do
    start = &start_with_plugins(namespace, %{spec | id: &1}, plugins, generated: true)

    spec.factory_id
    |> RelayManager.with_generated_id(start)
    |> RelayManager.start_answer(nil)
  end

  defp start_one(namespace, spec, plugins) do
    namespace
    |> start_with_plugins(spec, plugins, [])
    |> RelayManager.start_answer(spec.id)
  end

  # A plugin spec names the relay it binds, so it is decoded under the id
  # the start runs under, which for a generated id is each one drawn.
  defp start_with_plugins(namespace, spec, plugins, opts) do
    with {:ok, decoded} <- SpecDecoder.decode(plugins, namespace, spec.id),
         {:ok, bindings} <- PluginManager.start_bindings(namespace, spec.factory_id, decoded) do
      spec_opts =
        if bindings == [], do: spec.opts, else: Keyword.put(spec.opts, :bindings, bindings)

      RelayManager.start_relay(namespace, %{spec | opts: spec_opts}, opts)
    end
  end

  defp stop_one(namespace, relay_id) do
    case RelayManager.request_stop(namespace, relay_id) do
      :ok -> {:ok, %{id: relay_id, stopped: :stopped}}
      {:ok, :cooperative} -> {:ok, %{id: relay_id, stopped: :cooperative}}
      {:error, _reason} = refused -> refused
    end
  end

  defp stop_group(namespace, group_id, members) do
    results = Enum.map(members, &{&1.id, RelayManager.request_stop(namespace, &1.id)})

    case Enum.reject(results, fn {_id, result} -> result == :ok or match?({:ok, _}, result) end) do
      [] -> {:ok, %{id: group_id, stopped: :group, partitions: length(results)}}
      failed -> {:error, {:group_stop_partial, group_id, Enum.map(failed, &member_failure/1)}}
    end
  end

  # A member that did not stop, named with its own refusal.
  defp member_failure({id, {:error, reason}}),
    do: "#{id} (#{Flezha.Error.from_reason(reason, :"relay.stop").message})"

  # An optional text option, put on the input's metadata under `key`.
  defp put_text(metadata, _key, nil, _option), do: {:ok, metadata}

  defp put_text(metadata, key, text, _option) when is_binary(text),
    do: {:ok, Map.put(metadata, key, text)}

  defp put_text(_metadata, _key, written, option),
    do: {:error, Flezha.Error.new("usage.not_text", option: option, value: inspect(written))}

  defp delivered(:ok, member), do: {:ok, %{to: member, verdict: :delivered}}
  defp delivered({:ok, verdict}, member), do: {:ok, %{to: member, verdict: verdict}}
  defp delivered({:error, _reason} = refused, _member), do: refused

  defp group_row(report) do
    members = report.members

    %{
      id: report.group,
      health: members |> Enum.map(& &1.health) |> Enum.max_by(&Verdict.rank/1),
      runner: Enum.map_join(members, ", ", &"p#{&1.partition.index}→#{&1.runner || "-"}"),
      factory: members |> hd() |> Map.get(:factory),
      partitions: "#{length(members)}/#{report.declared}",
      node: members |> hd() |> Map.get(:node),
      members: members
    }
  end
end
