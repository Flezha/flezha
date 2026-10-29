defmodule Flezha.Logs.Sink do
  @moduledoc """
  The audit kind's feed: a sink on the observability bus.

  Registered as `:audit_log` with overflow `:block`. Its bus filter names the
  sources its `(source, name | :any)` allow-list names, so the bus never calls
  it for any other source; the name part is applied here.

  An accepted signal is encoded and handed to the writer with the producer's
  call, and the writer answers the producer once the record is written (and,
  under `:per_record`, synced). The producer's wait is the back-pressure, and
  the writer serves every producer queued at one moment with one sync, while
  this process never waits on it.

  The default allow-list is the system pairs plus every source
  `Flezha.AuthZ.Verbs` declares; `:extra_subscriptions` extends it.

  It also declares the `:audit_log` source, which the observability registry
  registers either way: verb audit records reach it from `Flezha.AuthZ.Verb`
  whether or not the audit log runs.
  """

  use GenServer

  use Flezha.Observability.Source,
    name: :audit_log,
    cardinality_budget: 200,
    area: :controller,
    title: "Audit log",
    description: "The audit log: who read it, and its files starting and expiring.",
    signals:
      [
        audit_rotated: {:event, label: "New file", meaning: "The audit log started a new file."},
        audit_retention_sweep:
          {:event,
           label: "Old files deleted",
           meaning: "Audit files older than the retention period were deleted."},
        log_overloaded:
          {:event,
           label: "Records refused",
           meaning: "The audit log refused records because its queue was full."}
      ] ++ Flezha.AuthZ.Verbs.audit_signals(:audit_log)

  use Flezha.Observability.Sink,
    name: :audit_log,
    overflow: :block,
    block_timeout_ms: Flezha.Config.Durations.get(:audit_log_block_timeout_ms)

  alias Flezha.AuthZ.Verbs
  alias Flezha.Config
  alias Flezha.Logs.{Codec, Kinds, Writer}
  alias Flezha.Observability.Signal

  # The system pairs. Every source an administration verb's row declares joins them
  # at boot, so a declared audit is durable by construction.
  @system_allow_list [
    {:flezha_shutdown, :any},
    {:flezha_plugin_packages, :any},
    # Auth-state mutations and authorization denials emit on
    # `:platform_authz`; the audit log subscribes by default so every
    # token issue/revoke, bootstrap completion, and forbidden decision is
    # durably recorded.
    {:platform_authz, :any}
  ]

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(),
    do: %{kinds: :any, sources: allow_list() |> Enum.map(&elem(&1, 0)) |> Enum.uniq()}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  The audit record a signal becomes: its wall-clock time and the fields after
  `ts_iso`. `caller` is denormalised from `metadata.caller_source`,
  `metadata.principal` and `metadata.request_id`, so a reader does not need to
  know the metadata convention to identify the actor.
  """
  @spec record(Signal.t()) :: {integer(), [{String.t(), term()}]}
  def record(%Signal{} = signal) do
    {signal.at_us || Signal.wall_us(signal.ts),
     [
       {"source", Atom.to_string(signal.source)},
       {"name", Atom.to_string(signal.name)},
       {"measurements", Signal.metadata_json(signal.measurements)},
       {"metadata", Signal.metadata_json(signal.metadata)},
       {"caller", caller(signal.metadata)},
       {"namespace", Map.get(signal.metadata, :namespace) || "_system"}
     ]}
  end

  @impl GenServer
  def init(_opts) do
    :ok = register_self()
    {:ok, %{allow_index: index(allow_list()), kind: Kinds.fetch!(:audit)}}
  end

  @impl GenServer
  def handle_call({:signal, signal}, from, state) do
    if allowed?(state.allow_index, signal) do
      Writer.forward(state.kind, from, Codec.encode(state.kind, signal))
      {:noreply, state}
    else
      {:reply, :ok, state}
    end
  end

  # A signal this process emitted itself arrives as a message: nobody waits.
  @impl GenServer
  def handle_info({:signal, signal}, state) do
    if allowed?(state.allow_index, signal),
      do: Writer.forward(state.kind, nil, Codec.encode(state.kind, signal))

    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp allow_list() do
    extra = Enum.filter(Config.get(:audit_log_extra_subscriptions), &valid_entry?/1)
    @system_allow_list ++ Enum.map(Verbs.audit_sources(), &{&1, :any}) ++ extra
  end

  defp valid_entry?({source, name}) when is_atom(source) and (is_atom(name) or name == :any),
    do: true

  defp valid_entry?(_), do: false

  defp index(allow_list) do
    Enum.reduce(allow_list, %{}, fn {source, name}, acc ->
      Map.update(acc, source, MapSet.new([name]), &MapSet.put(&1, name))
    end)
  end

  defp allowed?(index, signal) do
    case Map.fetch(index, signal.source) do
      :error -> false
      {:ok, names} -> MapSet.member?(names, :any) or MapSet.member?(names, signal.name)
    end
  end

  # With authorization off the principal column carries the caller's surface,
  # so a record stays attributable; on, only the verified principal does.
  defp caller(metadata) when is_map(metadata) do
    principal =
      if Flezha.AuthZ.enabled?(),
        do: Map.get(metadata, :principal),
        else: Map.get(metadata, :principal) || Map.get(metadata, :caller_source)

    %{
      "source" => text(Map.get(metadata, :caller_source)),
      "principal" => text(principal),
      "request_id" => Map.get(metadata, :request_id)
    }
  end

  defp text(nil), do: nil
  defp text(a) when is_atom(a), do: Atom.to_string(a)
  defp text(other), do: other
end
