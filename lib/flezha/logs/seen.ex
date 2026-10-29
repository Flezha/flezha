defmodule Flezha.Logs.Seen do
  @moduledoc """
  The relay history's in-memory index of the runs this controller recorded
  within the dedup window (`relay_history_dedup_window_hours`): by relay id and
  by spawn identity, the latest event and when. An `ended` key is never
  overwritten by a late `started` one.

  Its owner fills it from the kind's own files at boot, before the writer
  starts, so a restarted controller finds its keys before it takes its first
  append. A key is noted when its append is queued, so a lookup sees a record
  not yet synced. Keys older than the window go on each retention tick; past
  `relay_history_dedup_max_entries` the oldest go and are counted.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:tick]

  alias Flezha.Process.Cspec
  alias Flezha.Config
  alias Flezha.Logs.{Index, Kind, Reader}

  @seen :relay_history_seen
  @by_ts :relay_history_seen_by_ts
  @evicted {__MODULE__, :evicted}

  @type key :: {:relay, String.t()} | {:spawn_identity, String.t()}
  @type answer :: {:started | :ended, String.t(), integer()}

  def child_spec(%Kind{} = kind),
    do: Cspec.build({__MODULE__, kind.id}, {__MODULE__, :start_link, [kind]}, :worker_permanent)

  def start_link(%Kind{} = kind),
    do: GenServer.start_link(__MODULE__, kind, name: Kind.name(kind, :seen))

  @doc "Note a relay history record's keys."
  @spec note(map()) :: :ok
  def note(%{"event" => event, "namespace" => ns, "relay" => relay, "ts_us" => ts} = record)
      when event in ["started", "ended"] do
    if :ets.whereis(@seen) != :undefined do
      value = {String.to_existing_atom(event), relay, ts}
      put({ns, :relay, relay}, value)

      case record["spawn_identity"] do
        identity when is_binary(identity) and identity != "" ->
          put({ns, :identity, identity}, value)

        _none ->
          :ok
      end

      cap()
    end

    :ok
  end

  def note(_record), do: :ok

  @doc "The latest event this controller recorded for `key` in `namespace`, or `nil`."
  @spec lookup(String.t(), key()) :: answer() | nil
  def lookup(namespace, {:relay, id}), do: get({namespace, :relay, id})
  def lookup(namespace, {:spawn_identity, identity}), do: get({namespace, :identity, identity})

  @doc "How many keys the cap has evicted since boot."
  @spec evicted() :: non_neg_integer()
  def evicted() do
    case :persistent_term.get(@evicted, nil) do
      nil -> 0
      counter -> :counters.get(counter, 1)
    end
  end

  @impl true
  def init(kind) do
    :ets.new(@seen, [:named_table, :public, :set, write_concurrency: true, read_concurrency: true])

    :ets.new(@by_ts, [:named_table, :public, :ordered_set, write_concurrency: true])
    :persistent_term.put(@evicted, :counters.new(1, [:write_concurrency]))
    rebuild(kind)
    {:ok, arm_tick(%{kind: kind})}
  end

  @impl true
  def handle_info(:tick, state) do
    evict_before(System.os_time(:microsecond) - window_us())
    {:noreply, arm_tick(state)}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp get(key) do
    case :ets.whereis(@seen) != :undefined and :ets.lookup(@seen, key) do
      [{^key, answer}] -> answer
      _ -> nil
    end
  end

  defp put(key, {_event, _relay, ts} = value) do
    case :ets.lookup(@seen, key) do
      [{^key, {:ended, _, _}}] when elem(value, 0) == :started ->
        :ok

      _ ->
        :ets.insert(@seen, {key, value})
        :ets.insert(@by_ts, {{ts, key}})
    end
  end

  # Oldest first, past the cap.
  defp cap() do
    over = :ets.info(@seen, :size) - Config.get(:relay_history_dedup_max_entries)

    if over > 0 do
      evicted = evict_oldest(over, 0)
      :counters.add(:persistent_term.get(@evicted), 1, evicted)
    end
  end

  defp evict_oldest(0, evicted), do: evicted

  defp evict_oldest(n, evicted) do
    case :ets.first(@by_ts) do
      :"$end_of_table" -> evicted
      entry -> evict_oldest(n - 1, evicted + drop(entry))
    end
  end

  defp evict_before(cutoff) do
    case :ets.first(@by_ts) do
      {ts, _key} = entry when ts < cutoff ->
        drop(entry)
        evict_before(cutoff)

      _ ->
        :ok
    end
  end

  # A key overwritten later has a newer entry of its own: only its own goes.
  defp drop({ts, key} = entry) do
    :ets.delete(@by_ts, entry)

    case :ets.lookup(@seen, key) do
      [{^key, {_event, _relay, ^ts}}] -> :ets.delete(@seen, key) && 1
      _ -> 0
    end
  end

  defp rebuild(kind) do
    dir = Kind.dir(kind)
    cutoff = System.os_time(:microsecond) - window_us()

    rotated =
      for %{filename: name} = entry <- catalog(dir),
          not is_integer(entry[:last_ts_us]) or entry.last_ts_us >= cutoff,
          do: name

    for file <- rotated ++ [Kind.active(kind)],
        record <- Reader.stream(Path.join(dir, file), match: &recent?(&1, cutoff)),
        do: note(record)
  end

  defp catalog(dir) do
    case Index.load(dir) do
      {:ok, entries} -> Enum.sort_by(entries, &(&1.first_ts_us || 0))
      {:error, _} -> []
    end
  end

  defp recent?(%{"ts_us" => ts}, cutoff) when is_integer(ts), do: ts >= cutoff
  defp recent?(_record, _cutoff), do: false

  defp window_us(), do: Config.get(:relay_history_dedup_window_hours) * 3_600_000_000

  defp arm_tick(state),
    do: arm_timer(state, :tick, Kind.get(state.kind, :retention_check_interval_ms), :tick)
end
