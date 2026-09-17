defmodule Flezha.State.TimeTravel do
  @moduledoc """
  Point-in-time state queries against persisted Entity data.

  `query/2` resolves an `(entity_id, target)` tuple where target is
  `{:seq, u64}` or `{:timestamp, datetime}`. It locates the latest
  snapshot at-or-before the target, then folds the WAL records up to
  the target on the SDK side via a transient `RestoreInitState`-style
  replay (forwarded by the controller; the SDK runs the fold and ships
  back JSON-encoded terminal state).

  v1 `query/2` returns the assembled bytes payload + the resolved
  terminal seq. The replay-on-SDK round-trip is wired by
  `Flezha.State.Commands.dispatch_restore_init_state/2` and is invoked by the CLI
  command `flz state at <operative/procedure/entity> <seq | timestamp>`.

  Out-of-retention errors are returned as `%TimeTravelError{}`.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.SDK.RecordFrame, as: Frame
  alias Flezha.SDK.Wire
  alias Flezha.State.{Paths, RecordFrame, Snapshot}

  defmodule TimeTravelError do
    @moduledoc "Reasons a `TimeTravel.query/2` cannot be served."

    defexception [:kind, :message, :earliest_available]

    @type t :: %__MODULE__{
            kind: :outside_retention | :no_data | :decode | :unsupported_for_kind,
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

  @spec query(Paths.entity_id(), target()) ::
          {:ok, result()} | {:error, TimeTravelError.t()}
  def query({operative_class, _, _} = entity_id, target) do
    if ClassRegistry.capable?(operative_class, :time_travel?) do
      do_query(entity_id, target)
    else
      {:error,
       TimeTravelError.exception(
         kind: :unsupported_for_kind,
         message: "time-travel is not supported for this operative kind"
       )}
    end
  end

  defp do_query(entity_id, target) do
    snaps = Paths.snapshot_paths(entity_id)
    target_seq = resolve_target_seq(entity_id, target)

    case earliest_available_seq(snaps, entity_id) do
      :no_data ->
        {:error,
         TimeTravelError.exception(kind: :no_data, message: "no persisted state for entity")}

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
          read_wal_in_range(entity_id, snapshot_seq, target_seq)

        {:ok,
         %{
           snapshot_bytes: snapshot_bytes,
           change_log_bytes: change_log_bytes,
           terminal_seq: max(snapshot_seq, terminal_seq)
         }}
    end
  end

  defp resolve_target_seq(_entity_id, {:seq, seq}) when is_integer(seq) and seq >= 0, do: seq

  # The highest seq whose record time is at or before `ts` — exactly, from the
  # record itself. Effect records carry the invocation's clock value, which under
  # a `WallClock` is when it ran and under an `EventTimeFrom` is the event's own
  # time; either reading is the one a query for "the state as of T" wants.
  #
  # Falls back to the newest snapshot seq only when nothing is datable — a WAL of
  # pre-tier plain change records, which carry no time at all.
  defp resolve_target_seq(entity_id, {:timestamp, ts}) when is_integer(ts) do
    case scan_seq_at_or_before_path(Paths.wal_path(entity_id), ts) do
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

  defp earliest_available_seq([], entity_id) do
    if File.exists?(Paths.wal_path(entity_id)), do: 0, else: :no_data
  end

  defp earliest_available_seq([{seq, _} | _], _), do: seq

  defp pick_snapshot([], _target_seq), do: nil

  defp pick_snapshot(snaps, target_seq) do
    snaps
    |> Enum.filter(fn {seq, _} -> seq <= target_seq end)
    |> List.last()
  end

  defp read_wal_in_range(entity_id, snapshot_seq, target_seq) do
    {acc, terminal} =
      RecordFrame.fold_path!(
        Paths.wal_path(entity_id),
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
