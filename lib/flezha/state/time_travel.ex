defmodule Flezha.State.TimeTravel do
  @moduledoc """
  Point-in-time state queries against persisted Cell data.

  `query/2` resolves a `(cell_id, target)` tuple where target is
  `{:seq, u64}` or `{:timestamp, datetime}`. It locates the latest
  snapshot at-or-before the target, then folds the WAL records up to
  the target on the SDK side via a transient `RestoreInitState`-style
  replay (forwarded by the controller; the SDK runs the fold and ships
  back JSON-encoded terminal state).

  v1 `query/2` returns the assembled bytes payload + the resolved
  terminal seq. The replay-on-SDK round-trip is wired by
  `Flezha.State.Commands.dispatch_restore_init_state/2` and is invoked by the CLI
  command `flz state at <relay/tasklet/cell> <seq | timestamp>`.

  A target older than the retention window is refused naming the earliest
  point that is kept.
  """

  alias Flezha.State.Frame
  alias Flezha.State.Wire
  alias Flezha.State.{Paths, RecordFrame, Snapshot}

  @type target :: {:seq, non_neg_integer()} | {:timestamp, integer()}

  @type result :: %{
          snapshot_bytes: binary(),
          change_log_bytes: binary(),
          terminal_seq: non_neg_integer()
        }

  @doc """
  The Cell's state at `target`, or why it cannot be served: no persisted state
  at all, or a target older than the retention window, naming the earliest
  point that is kept.
  """
  @spec query(Paths.cell_id(), target()) ::
          {:ok, result()}
          | {:error, {:no_persisted_state, String.t()}}
          | {:error, {:outside_retention, String.t(), non_neg_integer(), non_neg_integer()}}
  def query({rc, sh, pk} = cell_id, target) do
    snaps = Paths.snapshot_paths(cell_id)
    target_seq = resolve_target_seq(cell_id, target)

    case earliest_available_seq(snaps, cell_id) do
      :no_data ->
        {:error, {:no_persisted_state, "#{rc}/#{sh}/#{pk}"}}

      earliest when target_seq < earliest ->
        {:error, {:outside_retention, "#{rc}/#{sh}/#{pk}", target_seq, earliest}}

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
          read_wal_in_range(cell_id, snapshot_seq, target_seq)

        {:ok,
         %{
           snapshot_bytes: snapshot_bytes,
           change_log_bytes: change_log_bytes,
           terminal_seq: max(snapshot_seq, terminal_seq)
         }}
    end
  end

  defp resolve_target_seq(_cell_id, {:seq, seq}) when is_integer(seq) and seq >= 0, do: seq

  # The highest seq whose record time is at or before `ts` — exactly, from the
  # record itself. Effect records carry the invocation's clock value, which under
  # a wall clock is when it ran and under an event-time clock is the event's own
  # time; either reading is the one a query for "the state as of T" wants.
  #
  # Falls back to the newest snapshot seq only when nothing is datable — a WAL of
  # pre-tier plain change records, which carry no time at all.
  defp resolve_target_seq(cell_id, {:timestamp, ts}) when is_integer(ts) do
    case scan_seq_at_or_before_path(Paths.wal_path(cell_id), ts) do
      nil -> 0
      seq -> seq
    end
  end

  defp scan_seq_at_or_before_path(path, ts) do
    RecordFrame.fold_path!(path, nil, fn {seq, header, body}, found ->
      case Wire.decode_effect_record(header, body) do
        %{event_time: event_time} when event_time <= ts -> {:cont, seq}
        _ -> {:cont, found}
      end
    end)
  end

  defp earliest_available_seq([], cell_id) do
    if File.exists?(Paths.wal_path(cell_id)), do: 0, else: :no_data
  end

  defp earliest_available_seq([{seq, _} | _], _), do: seq

  defp pick_snapshot([], _target_seq), do: nil

  defp pick_snapshot(snaps, target_seq) do
    snaps
    |> Enum.filter(fn {seq, _} -> seq <= target_seq end)
    |> List.last()
  end

  defp read_wal_in_range(cell_id, snapshot_seq, target_seq) do
    {acc, terminal} =
      RecordFrame.fold_path!(
        Paths.wal_path(cell_id),
        {[], snapshot_seq},
        &gather(&1, &2, snapshot_seq, target_seq)
      )

    {acc |> Enum.reverse() |> IO.iodata_to_binary(), terminal}
  end

  defp gather({seq, header, bytes}, {acc, terminal} = unchanged, snapshot_seq, target_seq) do
    cond do
      seq > target_seq ->
        {:halt, unchanged}

      seq > snapshot_seq ->
        {:cont, {[Frame.encode_record(seq, header, bytes) | acc], max(terminal, seq)}}

      true ->
        {:cont, unchanged}
    end
  end
end
