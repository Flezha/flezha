defmodule Flezha.Plugin.Extension.Table.Entry do
  @moduledoc """
  One plugin's lifecycle record, and the only functions that change it.

  One record rather than a map per fact, so a plugin cannot be half-removed:
  every read of "what is this plugin doing" reaches the same struct. What is
  here is what the record IS — its phase, the workers the table started for it,
  when it began running — and the one transition that changes a phase, which is
  therefore the one place the lifecycle event is emitted.

  The table owns the map of these; this module owns what one of them means.
  """

  @phase_event [:flezha, :plugin, :phase]

  @typedoc """
  A plugin's lifecycle phase. `nil` until it enters the table, so entering is a
  transition like any other; `:prepared` is validated and holding nothing;
  `:running` is activated with its declared worker up; `{:down, reason}` is a
  subscriber whose worker died, which the convergence loop restarts;
  `{:not_started, reason}` is a binding that could not come up here at all.
  """
  @type phase() :: nil | :prepared | :running | {:down, term()} | {:not_started, term()}

  @type t() :: %__MODULE__{
          provider: module() | nil,
          phase: phase(),
          started_at: integer() | nil,
          workers: %{term() => {pid(), reference()}}
        }

  defstruct [:provider, :started_at, :phase, workers: %{}]

  @doc "The event every phase transition emits, for handlers to attach to."
  @spec phase_event() :: [atom()]
  def phase_event(), do: @phase_event

  @doc """
  Move `entry` to `phase`, emitting the one lifecycle event when it actually
  changes. `started_at` is stamped on the way INTO `:running` and cleared on the
  way out, which is what makes "how long has this been up" a fact about the
  current run rather than about the record's age.
  """
  @spec transition(t(), binary(), module() | nil, phase(), term()) :: t()
  def transition(%__MODULE__{} = entry, plugin_id, provider, phase, reason) do
    provider = entry.provider || provider
    emit(plugin_id, provider, entry.phase, phase, reason)

    started_at =
      case {entry.phase, phase} do
        {:running, :running} -> entry.started_at
        {_, :running} -> :erlang.monotonic_time(:millisecond)
        _ -> nil
      end

    %__MODULE__{entry | phase: phase, provider: provider, started_at: started_at}
  end

  @doc "Report that `plugin_id` left the table entirely."
  @spec departed(t(), binary()) :: :ok
  def departed(%__MODULE__{} = entry, plugin_id) do
    emit(plugin_id, entry.provider, entry.phase, :unknown, :unsubscribed)
    :ok
  end

  @doc "`plugin_id`'s record on `table`, an empty one when it holds none."
  @spec of(map(), binary()) :: t()
  def of(%{entries: entries}, plugin_id) do
    case Map.get(entries, plugin_id) do
      %__MODULE__{} = entry -> entry
      _none -> %__MODULE__{}
    end
  end

  @doc "Record the worker started under `key`, replacing any earlier one."
  @spec put_worker(t(), term(), pid(), reference()) :: t()
  def put_worker(%__MODULE__{} = entry, key, pid, ref),
    do: %__MODULE__{entry | workers: Map.put(entry.workers, key, {pid, ref})}

  @doc "Forget the worker under `key`."
  @spec drop_worker(t(), term()) :: t()
  def drop_worker(%__MODULE__{} = entry, key),
    do: %__MODULE__{entry | workers: Map.delete(entry.workers, key)}

  @doc "The worker under `key`, as `{pid, monitor_ref}`, or `nil`."
  @spec worker(t(), term()) :: {pid(), reference()} | nil
  def worker(%__MODULE__{workers: workers}, key), do: Map.get(workers, key)

  @doc """
  The one process the host tracks as this plugin's own: the first of its
  workers by key, so a provider that runs several still has one the table can
  name, monitor and hand to `health/1`.
  """
  @spec owned_pid(t()) :: pid() | nil
  def owned_pid(%__MODULE__{workers: workers}) do
    case Enum.min_by(workers, &elem(&1, 0), fn -> nil end) do
      {_key, {pid, _ref}} -> pid
      nil -> nil
    end
  end

  @doc "Every worker's pid, by the key it is registered under."
  @spec pids(t()) :: %{term() => pid()}
  def pids(%__MODULE__{workers: workers}),
    do: Map.new(workers, fn {key, {pid, _ref}} -> {key, pid} end)

  @doc "Every key this plugin holds a worker under."
  @spec keys(t()) :: [term()]
  def keys(%__MODULE__{workers: workers}), do: Map.keys(workers)

  defp emit(_plugin_id, _provider, from, to, _reason) when from == to, do: :ok

  defp emit(plugin_id, provider, from, to, reason) do
    :telemetry.execute(@phase_event, %{count: 1}, %{
      plugin_id: plugin_id,
      provider: provider,
      from: from || :unknown,
      to: to,
      reason: reason
    })
  end
end
