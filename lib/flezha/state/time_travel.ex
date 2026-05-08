defmodule Flezha.State.TimeTravel do
  @moduledoc """
  Point-in-time state queries against persisted Partition data.

  `query/2` resolves a `(partition_id, target)` tuple where target is
  `{:seq, u64}` or `{:timestamp, datetime}`. It locates the latest
  snapshot at-or-before the target, then folds the WAL records up to
  the target on the SDK side via a transient `RestoreInitState`-style
  replay (forwarded by the controller; the SDK runs the fold and ships
  back JSON-encoded terminal state).

  v1 `query/2` returns the assembled bytes payload + the resolved
  terminal seq. The replay-on-SDK round-trip is wired by
  `Overwatch.dispatch_restore_init_state/2` and is invoked by the CLI
  command `flz state at <relay/shard/partition> <seq | timestamp>`.

  Out-of-retention errors are returned as `%TimeTravelError{}`.
  """

  alias Flezha.State.{Registry, Snapshot, WalWriter}

  defmodule TimeTravelError do
    @moduledoc "Reasons a `TimeTravel.query/2` cannot be served."

    defexception [:kind, :message, :earliest_available]

    @type t :: %__MODULE__{
            kind: :outside_retention | :no_data | :decode,
            message: binary(),
            earliest_available: term()
          }

    @impl true
    def exception(opts) do
      kind = Keyword.fetch!(opts, :kind)
      msg = Keyword.get(opts, :message, Atom.to_string(kind))

      %__MODULE__{
        kind: kind,
        message: msg,
        earliest_available: Keyword.get(opts, :earliest_available)
      }
    end
  end

  @type target :: {:seq, non_neg_integer()} | {:timestamp, integer()}

  @type result :: %{
          snapshot_bytes: binary(),
          change_log_bytes: binary(),
          terminal_seq: non_neg_integer()
        }

  @spec query(Registry.partition_id(), target()) ::
          {:ok, result()} | {:error, TimeTravelError.t()}
  def query(partition_id, target) do
    snaps = Registry.snapshot_paths(partition_id)
    target_seq = resolve_target_seq(partition_id, target)

    case earliest_available_seq(snaps, partition_id) do
      :no_data ->
        {:error,
         TimeTravelError.exception(kind: :no_data, message: "no persisted state for partition")}

      earliest when target_seq < earliest ->
        {:error,
         TimeTravelError.exception(
           kind: :outside_retention,
           message: "target seq #{target_seq} predates retention window (earliest=#{earliest})",
           earliest_available: earliest
         )}

      _earliest ->
        snapshot = pick_snapshot(snaps, target_seq)

        snapshot_bytes =
          case snapshot do
            nil ->
              <<>>

            {_seq, path} ->
              case Snapshot.read_snapshot(path) do
                {:ok, _header, bytes} -> bytes
                {:error, _} -> <<>>
              end
          end

        snapshot_seq = elem(snapshot || {0, nil}, 0)

        {change_log_bytes, terminal_seq} =
          read_wal_in_range(partition_id, snapshot_seq, target_seq)

        {:ok,
         %{
           snapshot_bytes: snapshot_bytes,
           change_log_bytes: change_log_bytes,
           terminal_seq: max(snapshot_seq, terminal_seq)
         }}
    end
  end

  defp resolve_target_seq(_partition_id, {:seq, seq}) when is_integer(seq) and seq >= 0, do: seq

  defp resolve_target_seq(partition_id, {:timestamp, ts}) when is_integer(ts) do
    # Walk WAL records and find the highest seq whose record-time is ≤ ts.
    # The on-disk header carries no per-record timestamp in v1; we fall
    # back to the WAL file's mtime as the upper bound. Refining this to
    # per-record `ts` is a follow-up once headers carry it.
    case File.stat(Registry.wal_path(partition_id), time: :posix) do
      {:ok, %{mtime: file_mtime}} when ts >= file_mtime ->
        # Target is now or in the future — return the latest seq.
        case Registry.snapshot_paths(partition_id) do
          [] -> 0
          paths -> elem(List.last(paths), 0)
        end

      _ ->
        # Target is before the WAL's last write — use the latest snapshot
        # seq before/at the target as the cap. Without per-record
        # timestamps, this is a coarse approximation.
        case pick_snapshot_by_mtime(Registry.snapshot_paths(partition_id), ts) do
          nil -> 0
          {seq, _} -> seq
        end
    end
  end

  defp earliest_available_seq([], partition_id) do
    if File.exists?(Registry.wal_path(partition_id)), do: 0, else: :no_data
  end

  defp earliest_available_seq([{seq, _} | _], _), do: seq

  defp pick_snapshot([], _target_seq), do: nil

  defp pick_snapshot(snaps, target_seq) do
    snaps
    |> Enum.filter(fn {seq, _} -> seq <= target_seq end)
    |> List.last()
  end

  defp pick_snapshot_by_mtime([], _ts), do: nil

  defp pick_snapshot_by_mtime(snaps, ts) do
    snaps
    |> Enum.filter(fn {_seq, path} ->
      case :file.read_file_info(path, time: :posix) do
        {:ok, info} -> elem(info, 5) <= ts
        _ -> false
      end
    end)
    |> List.last()
  end

  defp read_wal_in_range(partition_id, snapshot_seq, target_seq) do
    path = Registry.wal_path(partition_id)

    if File.exists?(path) do
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])

      try do
        gather(dev, 0, snapshot_seq, target_seq, [], snapshot_seq)
      after
        :file.close(dev)
      end
    else
      {<<>>, snapshot_seq}
    end
  end

  defp gather(dev, position, snapshot_seq, target_seq, acc, terminal) do
    case WalWriter.decode_record(dev, position) do
      {:ok, seq, _header, _bytes, _consumed} when seq > target_seq ->
        {acc |> Enum.reverse() |> IO.iodata_to_binary(), terminal}

      {:ok, seq, header, bytes, consumed} when seq > snapshot_seq ->
        gather(
          dev,
          position + consumed,
          snapshot_seq,
          target_seq,
          [WalWriter.encode_record(seq, header, bytes) | acc],
          max(terminal, seq)
        )

      {:ok, _seq, _header, _bytes, consumed} ->
        gather(dev, position + consumed, snapshot_seq, target_seq, acc, terminal)

      {:error, _} ->
        {acc |> Enum.reverse() |> IO.iodata_to_binary(), terminal}
    end
  end
end
