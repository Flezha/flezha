defmodule Flezha.Observability.Registry do
  @moduledoc """
  ETS-backed registry of live observability sources and sinks.

  Owns two `:protected` `:set` tables — `:flezha_observability_sources`
  and `:flezha_observability_sinks` — keyed by source / sink name.
  Read paths (`list_sources/0`, `list_sinks/0`, `lookup_*`) read
  directly from ETS without going through the GenServer.

  Per-source cardinality enforcement (`check_cardinality/2`) and per-source /
  per-sink counter access live here too. The first time a source
  exceeds its cardinality budget, `check_cardinality/2` answers `:first`
  and the calling process owes the single `:cardinality_budget_exceeded`
  event; the registry itself never dispatches to a sink.
  """

  use GenServer

  alias Flezha.Config

  # The two counter tables this registry keeps per entry: one per source, one
  # per sink. Declared rather than numbered by hand in function clauses.
  use Flezha.Observability.CounterTable,
    table: :source_counter,
    slots: [
      [name: :budget_exceeded, kind: :count],
      [name: :violations, kind: :count]
    ]

  use Flezha.Observability.CounterTable,
    table: :sink_counter,
    slots: [
      [name: :dropped, kind: :count],
      [name: :crashes, kind: :count]
    ]

  alias Flezha.Observability.Signal
  alias Flezha.Process.Cspec

  @sources :flezha_observability_sources
  @sinks :flezha_observability_sinks
  @registry_topic "flezha.observability.registry"

  @system_source :observability_registry

  @type source_flavour :: :declarative | :process_backed

  @type source_entry :: %{
          required(:name) => atom(),
          required(:kinds) => [Signal.kind()],
          required(:flavour) => source_flavour(),
          required(:cardinality_budget) => pos_integer(),
          required(:status) => :active | :disabled,
          required(:registered_at) => integer(),
          required(:metadata_shapes) => MapSet.t(),
          required(:budget_exceeded_emitted?) => boolean(),
          required(:counters) => :counters.counters_ref()
        }

  @type sink_entry :: %{
          required(:name) => atom(),
          required(:module) => module(),
          required(:config) => map(),
          required(:filter) => Flezha.Observability.Sink.filter(),
          required(:overflow) => :drop | :block,
          required(:mailbox_high_watermark) => pos_integer(),
          required(:block_timeout_ms) => pos_integer(),
          required(:pid) => pid() | nil,
          required(:counters) => :counters.counters_ref()
        }

  # ---------- Public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Register a source: `name`, `kinds`, `flavour` and an optional
  `cardinality_budget`. Contract: `Flezha.Observability.Source`.
  """
  @spec register_source(map()) :: :ok | {:error, term()}
  def register_source(%{name: _, kinds: _, flavour: _} = entry) do
    GenServer.call(__MODULE__, {:register_source, entry})
  end

  @spec unregister_source(atom()) :: :ok
  def unregister_source(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:unregister_source, name})
  end

  @spec enable_source(atom()) :: :ok | {:error, :not_found}
  def enable_source(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:set_source_status, name, :active})
  end

  @spec disable_source(atom()) :: :ok | {:error, :not_found}
  def disable_source(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:set_source_status, name, :disabled})
  end

  @spec list_sources() :: [source_entry()]
  def list_sources() do
    case :ets.whereis(@sources) do
      :undefined -> []
      _ -> :ets.tab2list(@sources) |> Enum.map(fn {_name, entry} -> public_source(entry) end)
    end
  end

  @spec lookup_source(atom()) :: {:ok, source_entry()} | :error
  def lookup_source(name) when is_atom(name) do
    case :ets.whereis(@sources) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(@sources, name) do
          [{^name, entry}] -> {:ok, entry}
          [] -> :error
        end
    end
  end

  @spec source_counter(atom(), :budget_exceeded | :violations) :: non_neg_integer()
  def source_counter(name, kind) do
    with {:ok, entry} <- lookup_source(name) do
      source_counter_get(entry.counters, kind)
    else
      _ -> 0
    end
  end

  @doc """
  Register a sink. The sink module's `config_schema/0` is consulted to
  validate `config` BEFORE the sink process is started. Returns
  `{:ok, pid}` on success.
  """
  @spec register_sink(map()) :: {:ok, pid()} | {:error, term()}
  def register_sink(%{name: _, module: _} = entry) do
    GenServer.call(__MODULE__, {:register_sink, entry})
  end

  @doc """
  Register a sink whose GenServer is owned by a different supervisor
  (e.g. the export subtree). The caller passes its own `pid`; the
  registry inserts the ETS row and the Bus then routes signals to
  that pid. Used by exporters that need to live under
  `Flezha.Observability.Export.Supervisor` rather than the foundation's
  generic `SinkSupervisor`.
  """
  @spec register_external_sink(map()) :: :ok
  def register_external_sink(%{name: name, module: _module, pid: pid} = entry)
      when is_atom(name) and is_pid(pid) do
    GenServer.call(__MODULE__, {:register_external_sink, entry})
  end

  @spec unregister_sink(atom()) :: :ok
  def unregister_sink(name) when is_atom(name) do
    GenServer.call(__MODULE__, {:unregister_sink, name})
  end

  @spec list_sinks() :: [sink_entry()]
  def list_sinks() do
    case :ets.whereis(@sinks) do
      :undefined -> []
      _ -> :ets.tab2list(@sinks) |> Enum.map(fn {_name, entry} -> public_sink(entry) end)
    end
  end

  @spec lookup_sink(atom()) :: {:ok, sink_entry()} | :error
  def lookup_sink(name) when is_atom(name) do
    case :ets.whereis(@sinks) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(@sinks, name) do
          [{^name, entry}] -> {:ok, entry}
          [] -> :error
        end
    end
  end

  @spec sink_counter(atom(), :dropped | :crashes) :: non_neg_integer()
  def sink_counter(name, kind) do
    with {:ok, entry} <- lookup_sink(name) do
      sink_counter_get(entry.counters, kind)
    else
      _ -> 0
    end
  end

  @doc """
  Increment a sink's counter — called by `Bus` and `Sink` processes.
  """
  @spec bump_sink_counter(atom(), :dropped | :crashes) :: :ok
  def bump_sink_counter(name, kind) do
    case lookup_sink(name) do
      {:ok, entry} -> sink_counter_bump(entry.counters, kind)
      :error -> :ok
    end
  end

  @doc """
  Cardinality check for `:metric` signals. Returns `:ok` when the
  signal's metadata-key-shape is already known, when it is new and
  fits within the per-source budget, or when the source is not
  registered (no enforcement possible). Returns
  `{:dropped, :cardinality_budget_exceeded, :first | :repeat}` when the
  shape is new and the source is at or over its budget.

  `:first` marks the crossing of the budget and is the caller's cue to
  emit the single `:cardinality_budget_exceeded` event. The event is
  owed by the calling process and never by this one: a `:block` sink
  would otherwise hold the registry, and every registration behind it.
  """
  @spec check_cardinality(atom(), map()) ::
          :ok | {:dropped, :cardinality_budget_exceeded, :first | :repeat}
  def check_cardinality(source, metadata) when is_atom(source) and is_map(metadata) do
    shape = shape_hash(metadata)

    case lookup_source(source) do
      :error ->
        :ok

      {:ok, %{metadata_shapes: shapes}} ->
        if MapSet.member?(shapes, shape) do
          :ok
        else
          GenServer.call(__MODULE__, {:add_shape, source, shape})
        end
    end
  end

  @doc """
  Subscribe the calling process to registry-mutation events
  (`{:source_registered, name}`, `{:source_unregistered, name}`,
  `{:source_status, name, status}`).

  Returns `:ok` even if the underlying PubSub server is not running;
  callers tolerate the no-op so the subsystem is usable when PubSub
  is intentionally disabled.
  """
  @spec subscribe_registry_events() :: :ok | {:error, term()}
  def subscribe_registry_events() do
    if Process.whereis(Flezha.Observability.PubSub) do
      Phoenix.PubSub.subscribe(Flezha.Observability.PubSub, @registry_topic)
    else
      :ok
    end
  end

  @doc false
  @spec registry_topic() :: String.t()
  def registry_topic(), do: @registry_topic

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    :ets.new(@sources, [:set, :protected, :named_table, read_concurrency: true])
    :ets.new(@sinks, [:set, :protected, :named_table, read_concurrency: true])

    # Pre-register internal sources: `:observability_registry` for
    # foundation-emitted system events (cardinality breach, kind
    # violation), and `:observability_service` for service-API audit
    # events (subscribe/enable/disable invocations).
    do_register_source(%{
      name: @system_source,
      kinds: [:event],
      flavour: :declarative,
      cardinality_budget: 1_000
    })

    do_register_source(%{
      name: :observability_service,
      kinds: [:event],
      flavour: :declarative,
      cardinality_budget: 1_000
    })

    {:ok, %{}}
  end

  @impl true
  def handle_call({:register_source, entry}, _from, state) do
    {:reply, do_register_source(entry), state}
  end

  def handle_call({:unregister_source, name}, _from, state) do
    :ets.delete(@sources, name)
    broadcast({:source_unregistered, name})
    {:reply, :ok, state}
  end

  def handle_call({:set_source_status, name, status}, _from, state) do
    reply =
      case :ets.lookup(@sources, name) do
        [{^name, entry}] ->
          :ets.insert(@sources, {name, %{entry | status: status}})
          broadcast({:source_status, name, status})
          :ok

        [] ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:add_shape, source, shape}, _from, state) do
    reply =
      case :ets.lookup(@sources, source) do
        [] ->
          :ok

        [{^source, entry}] ->
          if MapSet.size(entry.metadata_shapes) >= entry.cardinality_budget do
            source_counter_bump(entry.counters, :budget_exceeded)
            breach = if entry.budget_exceeded_emitted?, do: :repeat, else: :first
            :ets.insert(@sources, {source, %{entry | budget_exceeded_emitted?: true}})
            {:dropped, :cardinality_budget_exceeded, breach}
          else
            entry = %{entry | metadata_shapes: MapSet.put(entry.metadata_shapes, shape)}
            :ets.insert(@sources, {source, entry})
            :ok
          end
      end

    {:reply, reply, state}
  end

  def handle_call({:register_sink, entry}, _from, state) do
    {:reply, do_register_sink(entry), state}
  end

  def handle_call({:register_external_sink, entry}, _from, state) do
    counters_ref = sink_counter_new()

    row = %{
      name: entry.name,
      module: entry.module,
      config: Map.get(entry, :config, %{}),
      filter: Map.get(entry, :filter, %{kinds: :any, sources: :any}),
      overflow: Map.get(entry, :overflow, :drop),
      mailbox_high_watermark: Map.get(entry, :mailbox_high_watermark, 10_000),
      block_timeout_ms: Map.get(entry, :block_timeout_ms, 5_000),
      pid: entry.pid,
      counters: counters_ref
    }

    :ets.insert(@sinks, {entry.name, row})
    {:reply, :ok, state}
  end

  def handle_call({:unregister_sink, name}, _from, state) do
    case :ets.lookup(@sinks, name) do
      [{^name, %{pid: pid}}] when is_pid(pid) ->
        _ = DynamicSupervisor.terminate_child(Flezha.Observability.SinkSupervisor, pid)
        :ets.delete(@sinks, name)

      _ ->
        :ets.delete(@sinks, name)
    end

    {:reply, :ok, state}
  end

  # ---------- Internal helpers ----------

  defp do_register_source(entry) do
    case validate_source(entry) do
      :ok ->
        budget = Map.get(entry, :cardinality_budget, default_budget())
        counters_ref = source_counter_new()

        row = %{
          name: entry.name,
          kinds: entry.kinds,
          flavour: entry.flavour,
          cardinality_budget: budget,
          status: Map.get(entry, :status, :active),
          registered_at: System.monotonic_time(:microsecond),
          metadata_shapes: MapSet.new(),
          budget_exceeded_emitted?: false,
          counters: counters_ref
        }

        :ets.insert(@sources, {entry.name, row})
        broadcast({:source_registered, entry.name})
        :ok

      {:error, _} = err ->
        err
    end
  end

  defp do_register_sink(entry) do
    module = entry.module
    config = Map.get(entry, :config, %{})

    case sink_module_ready(module, config) do
      :ok -> start_and_record_sink(entry, module, config)
      {:error, _} = err -> err
    end
  end

  # Everything checkable before a process exists: the module is loaded, it
  # exports the two functions the bus reads, and its config satisfies the
  # schema it declares. An invalid sink is refused at registration, never at
  # start.
  defp sink_module_ready(module, config) do
    cond do
      not Code.ensure_loaded?(module) ->
        {:error, {:invalid_sink_module, module}}

      not function_exported?(module, :config_schema, 0) ->
        {:error, {:missing_config_schema, module}}

      not function_exported?(module, :filter, 0) ->
        {:error, {:missing_filter, module}}

      true ->
        validate_config(module.config_schema(), config)
    end
  end

  defp start_and_record_sink(entry, module, config) do
    spec =
      Cspec.build(
        entry.name,
        {module, :start_link, [%{name: entry.name, config: config}]},
        :worker_transient
      )

    case DynamicSupervisor.start_child(Flezha.Observability.SinkSupervisor, spec) do
      {:ok, pid} ->
        row = %{
          name: entry.name,
          module: module,
          config: config,
          filter: module.filter(),
          overflow: Map.get(entry, :overflow, :drop),
          mailbox_high_watermark: Map.get(entry, :mailbox_high_watermark, 10_000),
          block_timeout_ms: Map.get(entry, :block_timeout_ms, 5_000),
          pid: pid,
          counters: sink_counter_new()
        }

        :ets.insert(@sinks, {entry.name, row})
        {:ok, pid}

      {:error, reason} ->
        {:error, {:sink_start_failed, reason}}
    end
  end

  defp validate_source(%{name: name, kinds: kinds, flavour: flavour})
       when is_atom(name) and is_list(kinds) and flavour in [:declarative, :process_backed] do
    cond do
      not Enum.all?(kinds, &(&1 in Signal.kinds())) ->
        {:error, {:invalid_kinds, kinds}}

      # A source name is the head of every metric name it emits, and a metric
      # name outside the exposition grammar fails the whole scrape. Refused
      # here, where the developer wrote it, rather than rewritten at scrape
      # time — a source name is chosen, not forwarded from a third party.
      not Signal.legal_exposition_name?(Atom.to_string(name)) ->
        {:error, {:invalid_source_name, name}}

      true ->
        :ok
    end
  end

  defp validate_source(_), do: {:error, :invalid_source_entry}

  defp validate_config(schema, config) when is_map(schema) and is_map(config) do
    missing = for key <- Map.keys(schema), not Map.has_key?(config, key), do: key

    if missing == [] do
      :ok
    else
      {:error, {:missing_config_keys, missing}}
    end
  end

  defp validate_config(_schema, _config), do: {:error, :invalid_config}

  defp broadcast(message) do
    if Process.whereis(Flezha.Observability.PubSub) do
      Phoenix.PubSub.broadcast(
        Flezha.Observability.PubSub,
        @registry_topic,
        {:flezha_observability_registry, message}
      )
    else
      :ok
    end
  end

  defp shape_hash(metadata) do
    metadata
    |> Map.keys()
    |> Enum.sort()
    |> :erlang.phash2()
  end

  defp public_source(entry), do: Map.drop(entry, [:metadata_shapes, :counters])
  defp public_sink(entry), do: Map.drop(entry, [:counters])

  defp default_budget() do
    Config.get(:observability_cardinality_budget_per_source)
  end
end
