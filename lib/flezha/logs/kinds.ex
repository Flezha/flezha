defmodule Flezha.Logs.Kinds do
  @moduledoc """
  The durable logs the controller keeps, each a `Flezha.Logs.Kind` of the one
  log core: `audit`, the security trail the observability bus feeds, and
  `relay_history`, each relay run's start and end, appended directly.

  A kind that is off starts nothing and answers every read with the refusal
  `check/1` names. A bus-fed kind also needs the observability bus.
  """

  alias Flezha.Logs.{Kind, RelayHistory, Sink}
  alias Flezha.Observability

  @text "text"
  @enum "enum"
  @time "time"

  @doc "Every kind, in the order a reader lists them."
  @spec all() :: [Kind.t()]
  def all(), do: [audit(), relay_history()]

  @doc "The kind named `id` (an atom or its text), or `:error`."
  @spec fetch(atom() | String.t()) :: {:ok, Kind.t()} | :error
  def fetch(id) do
    case Enum.find(all(), &(&1.id == id or Atom.to_string(&1.id) == id)) do
      nil -> :error
      kind -> {:ok, kind}
    end
  end

  @doc "`fetch/1`, raising for a kind no row declares."
  @spec fetch!(atom()) :: Kind.t()
  def fetch!(id) do
    {:ok, kind} = fetch(id)
    kind
  end

  @doc "`:ok` when `kind` runs, else the refusal a read of it answers."
  @spec check(Kind.t()) :: :ok | {:error, term()}
  def check(%Kind{feed: {:bus, _sink}} = kind) do
    cond do
      not Observability.enabled?() -> {:error, :observability_disabled}
      not Kind.get(kind, :enabled) -> {:error, :audit_log_disabled}
      true -> :ok
    end
  end

  def check(%Kind{keys: %{enabled: key}} = kind) do
    if Kind.get(kind, :enabled),
      do: :ok,
      else: {:error, {:logs_kind_disabled, Atom.to_string(kind.id), Atom.to_string(key)}}
  end

  @doc "Whether `kind` runs on this controller."
  @spec enabled?(Kind.t()) :: boolean()
  def enabled?(kind), do: check(kind) == :ok

  defp audit() do
    %Kind{
      id: :audit,
      title: "Audit log",
      description: "Who did what: token, role and namespace changes, refusals and audited acts.",
      layout: :audit,
      prefix: "audit",
      keys: %{
        enabled: :audit_log_enabled,
        max_file_bytes: :audit_log_max_file_bytes,
        max_file_age_seconds: :audit_log_max_file_age_seconds,
        retention_seconds: :audit_log_retention_seconds,
        max_total_bytes: :audit_log_max_total_bytes,
        fsync_mode: :audit_log_fsync_mode,
        fsync_interval_ms: :audit_log_fsync_interval_ms,
        append_ms: {:duration, :audit_log_write_ms},
        rotation_check_interval_ms: :audit_log_rotation_check_interval_ms,
        retention_check_interval_ms: :audit_log_retention_check_interval_ms
      },
      feed: {:bus, Sink},
      encode: &Sink.record/1,
      events: %{
        source: Sink,
        rotated: :audit_rotated,
        swept: :audit_retention_sweep,
        overloaded: :log_overloaded
      },
      filters: [
        filter("source", "Source", @text),
        filter("name", "Name", @text),
        filter("namespace", "Namespace", @text)
      ],
      columns: [
        column("ts_iso", "Time", @time),
        column("source", "Source", @text),
        column("name", "Name", @text),
        column("caller", "Caller", @text),
        column("namespace", "Namespace", @text),
        column("node", "Controller", @text)
      ]
    }
  end

  defp relay_history() do
    %Kind{
      id: :relay_history,
      title: "Relay history",
      description:
        "Each relay run's start and end: who started it, how it ended, how long it ran.",
      layout: :relay_history,
      prefix: "relay_history",
      keys: %{
        enabled: :relay_history_enabled,
        max_file_bytes: :relay_history_max_file_bytes,
        max_file_age_seconds: :relay_history_max_file_age_seconds,
        retention_seconds: :relay_history_retention_seconds,
        max_total_bytes: :relay_history_max_total_bytes,
        fsync_mode: {:value, :group_commit},
        max_pending: :relay_history_max_pending,
        append_ms: :relay_history_append_ms,
        rotation_check_interval_ms: {:value, 60_000},
        retention_check_interval_ms: {:value, 3_600_000}
      },
      feed: :direct,
      encode: &RelayHistory.fields/1,
      events: %{
        source: RelayHistory,
        rotated: :relay_history_rotated,
        swept: :relay_history_retention_sweep,
        overloaded: :log_overloaded
      },
      children: [Flezha.Logs.Seen],
      filters: history_filters(),
      columns: history_columns()
    }
  end

  defp history_filters() do
    [
      filter("namespace", "Namespace", @text),
      filter("factory", "Factory", @text),
      filter("relay", "Relay", @text),
      filter("event", "Event", @enum, RelayHistory.events()),
      filter("outcome", "Outcome", @enum, RelayHistory.outcomes())
    ]
  end

  defp history_columns() do
    [
      column("ts_iso", "Time", @time),
      column("event", "Event", @text),
      column("namespace", "Namespace", @text),
      column("relay", "Relay", @text),
      column("factory", "Factory", @text),
      column("run", "Run", @text),
      column("origin", "Origin", @text),
      column("outcome", "Outcome", @text),
      column("duration_ms", "Duration (ms)", @text),
      column("halted", "Halted", @text),
      column("error", "Error", @text),
      column("node", "Controller", @text)
    ]
  end

  defp filter(field, label, type, values \\ nil),
    do: %{field: field, label: label, type: type, values: values}

  defp column(field, label, type), do: %{field: field, label: label, type: type}
end
