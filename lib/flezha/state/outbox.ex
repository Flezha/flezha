defmodule Flezha.State.Outbox do
  @moduledoc """
  Per-Entity dedup file (`<key>.outbox.dedup`): the persistence watermark
  `last_persisted_seq`, the outbox flush cursor `outbox_flushed_seq`, and
  the realign `epoch`, written together with the atomic
  `<key>.outbox.dedup.tmp` → rename pattern so a crash mid-write leaves
  either the previous triple or the new one — never a torn write.

  The file is a rebuildable cache advanced at fsync-batch cadence (plus
  snapshot, rotation, realign, and clean shutdown) — never per record. On
  boot, `WalWriter`'s WAL scan is the authoritative `last_persisted_seq`
  source; a stale `outbox_flushed_seq` only widens the restart re-drive
  (sink dedup absorbs the repeats — the cursor may lag, never lead).

  On-disk format v2 is three 8-byte big-endian integers
  `last_persisted_seq (unsigned) | outbox_flushed_seq (signed, -1 ≡ nothing
  flushed yet) | epoch (unsigned)`. A legacy 8-byte file reports its value
  as `last_persisted_seq` with cursor `-1` and epoch `0`; shorter or
  unreadable files report the empty triple.

  ## Delivery confirmation

  The cursor is durable proof that a sink HAS the output, so it advances on the
  sink's confirmation — never on the in-memory hand-off into the plugin chain.
  Every flushed output carries an opaque `flz.ack` token naming the writer that
  owns its cursor and the entry one confirmation completes. A
  delivery-guaranteed sink `hold/1`s the token as it takes ownership and
  `confirm/1`s it once its write is durable, or `park/4`s it when its retry
  ladder is permanently exhausted; the dispatcher `settled/1`s each output once
  the chain has run it. An entry completes when every output has settled and
  every hold has answered — so a chain with no guaranteed sink completes as
  soon as the chain has run, exactly as before.

  The dispatcher runs each output's chain inside `run/2`, which counts the
  holds that run takes and reports them on its settlement — one report, no
  token decode, and a hold can never be seen after the settlement it belongs
  to. A confirmation that overtakes them is still counted: completion needs the
  settlements, which only the dispatcher sends. A `hold/1` outside any run (a
  sink fed by something other than the dispatcher) reports through the token.

  The reports are batched, never the facts. One dispatch pass hands its whole
  window to the chain as one `{:io_out_batch, outputs}` cast, settles it with
  one `{:outbox_settled_many, entries}` report, and a sink answers a completed
  batch with one `confirm_many/1`. Each entry still completes on its own
  outputs and its own holds — a batch report is the same per-entry facts
  delivered at once.
  """

  require Logger

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.State.{Paths, Wire}
  alias Flezha.State.ProcJournal

  @type t :: %{
          last_persisted_seq: non_neg_integer(),
          outbox_flushed_seq: integer(),
          epoch: non_neg_integer()
        }

  @spec read(Paths.entity_id()) :: t()
  def read(entity_id) do
    path = Paths.outbox_dedup_path(entity_id)

    case File.read(path) do
      {:ok, <<seq::64-big, flushed::64-big-signed, epoch::64-big>>} ->
        %{last_persisted_seq: seq, outbox_flushed_seq: flushed, epoch: epoch}

      {:ok, <<seq::64-big>>} ->
        %{last_persisted_seq: seq, outbox_flushed_seq: -1, epoch: 0}

      _ ->
        %{last_persisted_seq: 0, outbox_flushed_seq: -1, epoch: 0}
    end
  end

  @spec write(Paths.entity_id(), t()) :: :ok | {:error, term()}
  def write(entity_id, %{
        last_persisted_seq: seq,
        outbox_flushed_seq: flushed,
        epoch: epoch
      })
      when is_integer(seq) and seq >= 0 and is_integer(flushed) and flushed >= -1 and
             is_integer(epoch) and epoch >= 0 do
    path = Paths.outbox_dedup_path(entity_id)
    tmp_path = path <> ".tmp"

    # No mkdir: the WAL open owns dir creation, so a cache write can never
    # resurrect a deleted Entity dir (it fails with :enoent instead).
    with :ok <- File.write(tmp_path, <<seq::64-big, flushed::64-big-signed, epoch::64-big>>),
         :ok <- File.rename(tmp_path, path) do
      :ok
    end
  end

  # ---------- delivery confirmation ----------

  @ack_key "flz.ack"
  # The chain run in progress in this process: `{token, holds_taken_so_far}`.
  @run_key {__MODULE__, :run}

  @typedoc """
  What one confirmation completes on the owning writer: a stream tier entry's
  cursor advance, or a workflow instance's `outbox_flushed` journal marker.
  Every output of one entry decodes to the same key — that is what makes the
  writer's tally per entry rather than per output.
  """
  @type key ::
          {:cursor, non_neg_integer()}
          | {:marker, byte(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Decoded `flz.ack` token: who owns the cursor, whose output it is, what it completes."
  @type ack :: %{owner: Paths.entity_id(), target: Paths.entity_id(), key: key()}

  @typedoc """
  One stamped output on its way to the chain: the payload, the metadata it
  carries, and the entry the dispatcher settles it under — the decoded ack, so
  neither the dispatcher nor the chain pays a token decode.
  """
  @type output :: {binary(), map(), %{owner: Paths.entity_id(), key: key()}}

  @doc "Metadata key carrying the confirmation token on a durable output."
  @spec ack_key() :: binary()
  def ack_key(), do: @ack_key

  @doc """
  Stamp one durable outbox output with its output identity, the confirmation
  token its entry is owed under, and the decoded ack the dispatcher settles it
  with (`run/2`).

  The one place a durable output is minted — which is what makes the token
  impossible to forget. `identity` is `Flezha.State.Wire.output_stamps/1`'s
  input plus `:owner` (the writer whose ledger the entry sits in), `:entry` (the
  key one confirmation completes) and optional `:metadata` the output carries of
  its own.

  The token names the entry, not the output, so every output of one entry whose
  target is also its owner carries the identical token: such a caller mints it
  once with `token/2` and passes it as `:ack`.
  """
  @spec stamp(binary(), map()) :: output()
  def stamp(payload, %{owner: owner, entry: entry, entity: entity} = identity) do
    stamps =
      identity
      |> Map.drop([:owner, :entry, :metadata])
      |> Map.put_new_lazy(:ack, fn -> token(owner, entity, entry) end)
      |> Wire.output_stamps()

    {payload, Map.merge(Map.new(Map.get(identity, :metadata, %{})), stamps),
     %{owner: owner, key: entry}}
  end

  @doc """
  Hand one dispatch pass's stamped outputs to the operative's `:on_output`
  chain as one cast — the one place a durable output leaves an outbox.
  """
  @spec dispatch(pid(), [output()]) :: :ok
  def dispatch(_overwatch, []), do: :ok
  def dispatch(overwatch, outputs), do: GenServer.cast(overwatch, {:io_out_batch, outputs})

  @doc """
  Run a dispatch batch's chains as the dispatcher: `fun` is the chain, every
  `hold/1` an output's run takes is counted against that output, and the
  batch's entries are settled with their counts in one report per owning
  writer. A chain that crashes settles nothing — not its own entry and not the
  rest of the batch — and the resettle tick re-dispatches what stayed pending.
  """
  @spec run([output()], (binary(), map() -> term())) :: :ok
  def run(outputs, fun) when is_function(fun, 2) do
    outputs
    |> Enum.reduce(%{}, fn {payload, metadata, %{owner: owner, key: key}}, settled ->
      Process.put(@run_key, {ack(metadata), 0})
      fun.(payload, metadata)
      {_token, holds} = Process.delete(@run_key)
      Map.update(settled, owner, [{key, holds}], &[{key, holds} | &1])
    end)
    |> report_per_owner(:outbox_settled_many)
  end

  @doc """
  Mint the confirmation token for one outbox entry: who owns the cursor it
  moves, whose output it is, and what one confirmation completes. `target`
  defaults to the owner — true for a stream Entity, whose writer owns its own
  output; a workflow instance's journal writer owns output belonging to other
  Entities.
  """
  @spec token(Paths.entity_id(), key()) :: binary()
  def token(owner, key), do: token(owner, owner, key)

  @spec token(Paths.entity_id(), Paths.entity_id() | binary(), key()) :: binary()
  def token(owner, target, key) do
    owner = entity_bytes(owner)
    target = entity_bytes(target)
    # An elided target reads back as the owner: the stream tier's every token
    # would otherwise carry the same identity twice.
    target = if target == owner, do: <<>>, else: target

    encode(
      <<byte_size(owner)::16-big, owner::binary, byte_size(target)::16-big, target::binary,
        encode_key(key)::binary>>
    )
  end

  @doc """
  Whether this output's delivery is guaranteed — an outbox cursor is waiting on
  it. The one test a sink uses to decide it must not drop a record.
  """
  @spec guaranteed?(map()) :: boolean()
  def guaranteed?(metadata) when is_map(metadata), do: ack(metadata) != nil

  # The confirmation token stamped on a durable output, or `nil` when no cursor
  # is waiting on it.
  defp ack(metadata) do
    case Map.get(metadata, @ack_key) do
      t when is_binary(t) and t != "" -> t
      _ -> nil
    end
  end

  @doc """
  Take ownership of a guaranteed output: the owning writer holds its entry open
  until a matching `confirm/1` or `park/4`. Called from the plugin chain, before
  the record is handed to the sink's own queue.
  """
  @spec hold(map()) :: :ok
  def hold(metadata) do
    case {ack(metadata), Process.get(@run_key)} do
      {nil, _} ->
        :ok

      {token, {token, holds}} ->
        Process.put(@run_key, {token, holds + 1})
        :ok

      _ ->
        notify(metadata, :outbox_hold)
    end
  end

  @doc """
  One output has been through the whole plugin chain outside a `run/3` — a test
  standing in for the dispatcher. Settles with no holds of its own; holds taken
  outside a run reported themselves.
  """
  @spec settled(map()) :: :ok
  def settled(metadata), do: notify(metadata, {:outbox_settled, 0})

  @doc "One hold's write is durable at the sink."
  @spec confirm(map()) :: :ok
  def confirm(metadata), do: notify(metadata, :outbox_confirmed)

  @doc """
  A whole batch's writes are durable at the sink: one report per owning writer
  naming every entry the batch completed. Records carrying no token settle
  nothing, exactly as under `confirm/1`.
  """
  @spec confirm_many([map()]) :: :ok
  def confirm_many(metadatas) when is_list(metadatas) do
    metadatas
    |> Enum.reduce(%{}, fn metadata, owed ->
      case entry_of(metadata) do
        {owner, key} -> Map.update(owed, owner, [key], &[key | &1])
        nil -> owed
      end
    end)
    |> report_per_owner(:outbox_confirmed_many)
  end

  @doc """
  A hold whose retry budget is permanently exhausted: park the payload through
  the dead-letter store — attributed to the output's own Entity and channel —
  then answer the hold so the cursor advances past exactly the parked range.
  Loud and replayable, never a silent drop.
  """
  @spec park(map(), binary() | nil, term(), non_neg_integer()) :: :ok
  def park(metadata, payload, error, attempts) when is_map(metadata) do
    with token when is_binary(token) <- ack(metadata),
         {:ok, %{target: {rc, sh, pk}}} <- decode(token) do
      entry = %{
        operative_class: rc,
        procedure_id: sh,
        entity_key: pk,
        channel: Map.get(metadata, "channel", ""),
        origin_offset: nil,
        error: inspect(error),
        attempts: attempts,
        payload: payload
      }

      case DlqStore.append(entry) do
        {:ok, ref} ->
          Logger.error(
            "[outbox] parked undeliverable output #{rc}/#{sh}/#{pk} " <>
              "seq=#{Map.get(metadata, "flz.seq")} epoch=#{Map.get(metadata, "flz.epoch")} " <>
              "after #{attempts} attempt(s): #{inspect(error)} — dead-letter #{ref}"
          )

          confirm(metadata)

        {:error, reason} ->
          # The park is the only record of this output; without it the cursor
          # must not move. Left unconfirmed, the entry re-drives.
          Logger.error(
            "[outbox] dead-letter append FAILED for #{rc}/#{sh}/#{pk} " <>
              "seq=#{Map.get(metadata, "flz.seq")}: #{inspect(reason)} — " <>
              "the outbox cursor holds and the entry will re-drive"
          )

          :ok
      end
    else
      _ -> :ok
    end
  end

  @doc """
  Run a completed entry's journal-marker completion. Called by the owning
  writer, which must not append to its own log from inside its own callback.
  """
  @spec complete(Paths.entity_id(), key()) :: :ok
  def complete({instance_id, _, _}, {:marker, for_type, proc_idx, iter, epoch}) do
    ProcJournal.mark_flushed(instance_id, for_type, proc_idx, iter, epoch)
  end

  @doc """
  Decode a token into the writer that owns it, the Entity the output belongs
  to, and the entry key one confirmation completes.
  """
  @spec decode(binary()) :: {:ok, ack()} | :error
  def decode(token) when is_binary(token) do
    with {:ok, bytes} <- Base.url_decode64(token, padding: false) do
      decode_body(bytes)
    else
      _ -> :error
    end
  end

  defp decode_body(
         <<owner_len::16-big, owner::binary-size(owner_len), target_len::16-big,
           target::binary-size(target_len), key::binary>>
       ) do
    with {:ok, owner_id} <- Wire.decode_entity_id(owner),
         {:ok, target_id} <- decode_target(target, owner_id),
         {:ok, key} <- decode_key(key) do
      {:ok, %{owner: owner_id, target: target_id, key: key}}
    else
      _ -> :error
    end
  end

  defp decode_body(_), do: :error

  defp decode_target(<<>>, owner_id), do: {:ok, owner_id}
  defp decode_target(bytes, _owner_id), do: Wire.decode_entity_id(bytes)

  defp encode_key({:cursor, seq}), do: <<1, seq::64-big>>

  defp encode_key({:marker, for_type, proc_idx, iter, epoch}),
    do: <<2, for_type::8, proc_idx::16-big, iter::32-big, epoch::32-big>>

  defp decode_key(<<1, seq::64-big>>), do: {:ok, {:cursor, seq}}

  defp decode_key(<<2, for_type::8, proc_idx::16-big, iter::32-big, epoch::32-big>>),
    do: {:ok, {:marker, for_type, proc_idx, iter, epoch}}

  defp decode_key(_), do: :error

  defp notify(metadata, tag) when is_map(metadata) do
    case entry_of(metadata) do
      {owner, key} -> report(owner, report_for(tag, key))
      nil -> :ok
    end
  end

  # The writer this output's entry belongs to, and the entry — `nil` when no
  # cursor is waiting on it.
  defp entry_of(metadata) do
    with token when is_binary(token) <- ack(metadata),
         {:ok, %{owner: owner, key: key}} <- decode(token) do
      {owner, key}
    else
      _ -> nil
    end
  end

  defp report_for({:outbox_settled, holds}, key), do: {:outbox_settled, key, holds}
  defp report_for(tag, key) when is_atom(tag), do: {tag, key}

  # One batch report per owning writer, in the order the batch collected them.
  defp report_per_owner(by_owner, tag) do
    Enum.each(by_owner, fn {owner, entries} -> report(owner, {tag, Enum.reverse(entries)}) end)
  end

  # A writer that is gone cannot be told anything, and needs no telling: its
  # cursor never advanced, so the restart re-drive owes the entry anyway.
  defp report(owner, message) do
    case Flezha.State.Supervisor.lookup(owner) do
      {:ok, pid} -> GenServer.cast(pid, message)
      :error -> :ok
    end
  end

  defp entity_bytes({rc, sh, pk}), do: Wire.encode_entity_id(rc, sh, pk)
  defp entity_bytes(bytes) when is_binary(bytes), do: bytes

  defp encode(bytes), do: Base.url_encode64(bytes, padding: false)
end
