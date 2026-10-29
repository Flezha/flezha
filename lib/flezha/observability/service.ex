defmodule Flezha.Service.Observability do
  @moduledoc """
  Service-API surface for the observability foundation. Every published function
  takes a `Flezha.Service.CallerContext` as its last argument and binds a row
  of `Flezha.AuthZ.Verbs`.

  With the master flag `:observability` → `enabled: false`, every public
  function returns `{:error, :observability_disabled}`.

  `tail/2` hands each signal to the context's `:emit` (see its doc); without
  one it subscribes the calling process.

  The reads cover every controller: a query merges every controller's ring
  buffer by wall-clock time, the listings answer every controller's view, a
  switch applies on every controller, and a tail hands on every controller's
  signals. Each signal answered carries its `node` and `at_us`.

  The tail, its undo and the two mutations are audited on the
  `:observability_service` source before the effect, under the function's
  name — the seam the audit log consumes. The effects are public so
  `Flezha.Service.Cli.Observe` renders the same answers under the same rows.
  """

  use Flezha.AuthZ.Verb

  use Flezha.Observability.Source,
    name: :observability_service,
    emit: false,
    cardinality_budget: 1_000,
    area: :controller,
    title: "Observability calls",
    description: "Calls that tail a source, or switch one on or off.",
    signals: Flezha.AuthZ.Verbs.audit_signals(:observability_service)

  # Taken as text so the service, not the door, refuses an unknown source:
  # it names the registered ones and mints no atom.
  @source [type: :string, doc: "A registered observability source."]

  alias Flezha.AuthZ.Namespace
  alias Flezha.Cluster.Reads
  alias Flezha.Config
  alias Flezha.Observability
  alias Flezha.Observability.{Registry, RingBufferSink, Signal}
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @type query_params :: %{
          required(:source) => atom(),
          optional(:since_us) => integer(),
          optional(:limit) => non_neg_integer()
        }

  # ---------- Subscriptions ----------

  @doc """
  Undo a `tail/2`.

  A subscription established on this surface is undone on it, so a consumer
  never has to reach past the service API to release what it took through it.
  Authorized as `tail/2` is: the two are one capability.
  """
  @spec unsubscribe(atom() | String.t(), caller_context()) :: :ok | {:error, term()}
  verb unsubscribe(source, caller_context) when is_atom(source) or is_binary(source),
    verb: :observability_unsubscribe,
    args: [source: @source] do
    with :ok <- check_enabled(),
         {:ok, source} <- registered(source),
         do: Observability.unsubscribe(source)
  end

  @doc """
  Stream the live signals of one source that the caller may see.

  With an `:emit` in the context (a function, or a pid that receives
  `{:flezha_emit, signal}`), each signal is handed to it until the watched
  process exits, `:emit` answers `:done` or the context's `window_ms` runs
  out, and the subscription is released on the way out; the answer is
  `{:ok, :caller_down | :done | :window_closed}`. Without one, the calling
  process is subscribed and receives `{:flezha_signal, signal}` itself, and
  undoes it with `unsubscribe/2`.

  Answers `{:error, :pubsub_sink_disabled}` when the PubSub sink is not
  enabled: there is no broadcast to stream.
  """
  @spec tail(atom() | String.t(), caller_context()) :: :ok | {:ok, atom()} | {:error, term()}
  verb tail(source, caller_context) when is_atom(source) or is_binary(source),
    verb: :observability_tail,
    stream: true,
    args: [source: @source] do
    with :ok <- check_enabled(),
         {:ok, source} <- registered(source),
         :ok <- check_pubsub_sink() do
      if Map.has_key?(caller_context, :emit),
        do: forward(source, caller_context),
        else: Observability.subscribe(source)
    end
  end

  # ---------- Reads ----------

  @doc """
  The signals every controller's ring buffer holds for `params.source`,
  recorded at or after `params.since_us` (wall clock, microseconds), merged by
  `at_us` and the newest `params.limit` of them kept. Only the signals of the
  caller's namespace are answered, each with its `node` and `at_us`.
  """
  @spec query(query_params() | keyword(), caller_context()) ::
          {:ok, Reads.rows()} | {:error, term()}
  verb query(params, caller_context),
    verb: :observability_query,
    args: [params: [type: :keyword, doc: "`source`, `since_us` and `limit`."]] do
    params = Map.new(params)

    with :ok <- check_enabled(),
         {:ok, written} <- Map.fetch(params, :source),
         {:ok, source} <- registered(written) do
      limit = Map.get(params, :limit)
      args = [source, Map.get(params, :since_us, 0), visibility(caller_context), limit]

      {:ok,
       Reads.controllers()
       |> Reads.rows(__MODULE__, :local_query, args)
       |> Reads.newest(& &1.at_us, limit)}
    else
      :error -> {:error, :missing_source}
      other -> other
    end
  end

  @doc """
  Every observability source registered on any controller and what it said
  recently, one row per source. `statuses` holds its status on each
  controller, and `recent` holds the ring buffer's `window_ms` and, per signal
  name the caller may read, how many the buffers hold across controllers and
  the age of the newest in milliseconds. `recent` is `nil` when no ring buffer
  is running.
  """
  @spec list_sources(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb list_sources(caller_context), verb: :observability_sources do
    with :ok <- check_enabled() do
      {:ok,
       Reads.controllers()
       |> Reads.rows(__MODULE__, :local_sources, [visibility(caller_context)])
       |> Reads.grouped(& &1.name, &merged/1)}
    end
  end

  @doc "Every controller's registered observability sinks, each with its `node`."
  @spec list_sinks(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb list_sinks(caller_context), verb: :observability_sinks do
    with :ok <- check_enabled(),
         do: {:ok, Reads.rows(Reads.controllers(), Registry, :list_sinks, [])}
  end

  # ---------- Mutations ----------

  @doc """
  Enable an observability source on every controller. Answers the controllers
  that `applied` it and those that did not answer (`unreachable`).
  """
  @spec enable_source(atom() | String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb enable_source(name, caller_context) when is_atom(name) or is_binary(name),
    verb: :observability_enable,
    args: [name: @source] do
    with :ok <- check_enabled(), {:ok, name} <- registered(name), do: switch(name, :enable_source)
  end

  @doc """
  Disable an observability source on every controller. Answers the controllers
  that `applied` it and those that did not answer (`unreachable`).
  """
  @spec disable_source(atom() | String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb disable_source(name, caller_context) when is_atom(name) or is_binary(name),
    verb: :observability_disable,
    args: [name: @source] do
    with :ok <- check_enabled(),
         {:ok, name} <- registered(name),
         do: switch(name, :disable_source)
  end

  # ---------- The local halves ----------

  @doc false
  # The local half of `query/2`. The caller's namespace is applied before the
  # limit, so a bound caller gets its own newest signals rather than what is
  # left of everyone's.
  @spec local_query(atom(), integer(), :all | String.t(), non_neg_integer() | nil) :: [Signal.t()]
  def local_query(source, since_us, visibility, limit) do
    visible =
      source
      |> RingBufferSink.query(Signal.monotonic_us(since_us))
      |> Enum.filter(visible_to(visibility))

    newest = if limit, do: Enum.take(visible, -limit), else: visible
    Enum.map(newest, &Signal.located/1)
  end

  @doc false
  # The local half of `list_sources/1`: this controller's sources, aged on its
  # own clock.
  @spec local_sources(:all | String.t()) :: [map()]
  def local_sources(visibility), do: with_recent(Registry.list_sources(), visibility)

  # ---------- Internal helpers ----------

  # The tail of a caller that named an `:emit`: this process subscribes, and
  # hands on what the caller may read, so data a caller may not see never
  # leaves the controller.
  defp forward(source, caller_context) do
    {emit, watch} = CallerContext.emitter(caller_context)
    visible? = caller_context |> visibility() |> visible_to()

    with :ok <- Observability.subscribe(source) do
      try do
        outcome =
          CallerContext.receive_for(
            Map.get(caller_context, :window_ms, :infinity),
            fn
              {:flezha_signal, signal} ->
                CallerContext.hand_on(Enum.filter([signal], visible?), emit)

              _other ->
                :continue
            end,
            watch
          )

        {:ok, outcome}
      after
        Observability.unsubscribe(source)
      end
    end
  end

  # The registered source `written` names, as an atom or as its text; one the
  # registry does not hold is refused naming the ones it does.
  defp registered(written) do
    sources = Enum.map(Registry.list_sources(), & &1.name)

    case Enum.find(sources, &(&1 == written or Atom.to_string(&1) == written)) do
      nil ->
        {:error,
         Flezha.Error.new("usage.unknown_word", noun: "source", value: written, allowed: sources)}

      source ->
        {:ok, source}
    end
  end

  defp check_pubsub_sink() do
    if Config.get(:observability_pubsub_sink_enabled),
      do: :ok,
      else: {:error, :pubsub_sink_disabled}
  end

  defp switch(name, fun) do
    %{answers: answers, unreachable: unreachable} =
      Reads.ask(Reads.controllers(), Registry, fun, [name])

    {:ok, %{applied: for({node, :ok} <- answers, do: node), unreachable: unreachable}}
  end

  # One source's rows from every controller as one: its status on each, and
  # its recent counts summed with the youngest age kept.
  defp merged([first | _] = per_node) do
    first
    |> Map.drop([:status, :node])
    |> Map.merge(%{
      statuses: Map.new(per_node, &{&1.node, &1.status}),
      recent: per_node |> Enum.map(& &1.recent) |> Enum.reject(&is_nil/1) |> merged_recent()
    })
  end

  defp merged_recent([]), do: nil

  defp merged_recent([%{window_ms: window_ms} | _] = recents) do
    signals =
      for %{signals: signals} <- recents, {name, row} <- signals, reduce: %{} do
        acc ->
          Map.update(acc, name, row, fn seen ->
            %{count: seen.count + row.count, last_age_ms: min(seen.last_age_ms, row.last_age_ms)}
          end)
      end

    %{window_ms: window_ms, signals: signals}
  end

  defp with_recent(sources, visibility) do
    if Process.whereis(RingBufferSink) do
      window_ms = Config.get(:observability_ring_buffer_retention_ms)
      now = System.monotonic_time(:microsecond)
      from_ts = now - window_ms * 1_000
      visible? = visible_to(visibility)

      Enum.map(sources, fn source ->
        signals = RingBufferSink.summary(source.name, from_ts, visible?)
        Map.put(source, :recent, %{window_ms: window_ms, signals: aged(signals, now)})
      end)
    else
      Enum.map(sources, &Map.put(&1, :recent, nil))
    end
  end

  defp aged(summary, now) do
    for {name, %{count: count, newest_ts: ts}} <- summary,
        into: %{},
        do: {name, %{count: count, last_age_ms: div(max(now - ts, 0), 1_000)}}
  end

  # Which signals the caller's namespace may see, for a query, a forwarded tail
  # and the listing's counts alike. It is data, so a local half on another
  # controller applies it; a signal whose metadata carries no namespace (a
  # pre-change emit) is seen by every caller.
  defp visibility(caller_context), do: Namespace.read_scope(caller_context)

  defp visible_to(:all), do: fn _signal -> true end
  defp visible_to(namespace), do: &(Map.get(&1.metadata, :namespace) in [nil, namespace])

  defp check_enabled() do
    if Observability.enabled?(), do: :ok, else: {:error, :observability_disabled}
  end
end
