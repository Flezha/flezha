defmodule Flezha.State.Outbox do
  @moduledoc """
  Two jobs for a Cell's durable outputs: the identity each one carries
  (`output_stamps/1`, `output_identity/1`), and the delivery token that
  confirms it, the seam over the Cell's flush cursor (whose file
  `Flezha.State.Lineage` owns).

  The cursor is durable proof that a sink HAS the output, so it advances on the
  sink's confirmation, never on the in-memory hand-off. Every flushed output
  carries an opaque `flz.ack` token; a delivery-guaranteed sink `hold/1`s it and
  `confirm/1`s or `park/4`s it, while the dispatcher `settled/1`s each output.
  An entry completes when every output has settled and every hold has answered.
  """

  import Bitwise, only: [<<<: 2]

  require Logger

  alias Flezha.Dlq.Entry, as: DlqEntry
  alias Flezha.Error
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.State.Wire
  alias Flezha.State.{Paths, WriterRegistry}

  @ack_key "flz.ack"
  # The chain run in progress in this process: `{token, holds_taken_so_far}`.
  @run_key {__MODULE__, :run}
  @delivery_hold 1 <<< 32

  @typedoc """
  What one confirmation completes on the owning writer: the entry of the
  effect record at that seq, whose completion lets the cursor pass it. Every
  output of one entry decodes to the same key, which is what makes the
  writer's tally per entry rather than per output. A re-dispatch numbers its
  key (`{n, seq}` for the entry's nth), so a superseded copy's reports name an
  entry the writer no longer holds.
  """
  @type key :: {:cursor | pos_integer(), non_neg_integer()}

  @typedoc """
  Decoded `flz.ack` token: the Cell whose writer owns the cursor and whose
  output it is, and what it completes.
  """
  @type ack :: %{owner: Paths.cell_id(), key: key()}

  @typedoc """
  One stamped output on its way to the chain: the payload, the metadata it
  carries, and the entry the dispatcher settles it under — the decoded ack, so
  neither the dispatcher nor the chain pays a token decode.
  """
  @type output :: {binary(), map(), %{owner: Paths.cell_id(), key: key()}}

  @doc "Metadata key carrying the confirmation token on a durable output."
  @spec ack_key() :: binary()
  def ack_key(), do: @ack_key

  @doc """
  Stamp one durable outbox output with its output identity, the confirmation
  token its entry is owed under, and the decoded ack the dispatcher settles it
  with (`run/3`).

  The one place a durable output is minted — which is what makes the token
  impossible to forget. `identity` is `output_stamps/1`'s
  input plus `:owner` (the writer whose ledger the entry sits in), `:entry` (the
  key one confirmation completes) and optional `:metadata` the output carries of
  its own.

  The token names the entry, not the output, so every output of one entry
  carries the identical token: a caller mints it once with `token/2` and passes
  it as `:ack`.
  """
  @spec stamp(binary(), map()) :: output()
  def stamp(payload, %{owner: owner, entry: entry} = identity) do
    stamps =
      identity
      |> Map.drop([:owner, :entry, :metadata])
      |> Map.put_new_lazy(:ack, fn -> token(owner, entry) end)
      |> output_stamps()

    {payload, Map.merge(Map.new(Map.get(identity, :metadata, %{})), stamps),
     %{owner: owner, key: entry}}
  end

  @doc """
  Hand one dispatch pass's stamped outputs to the relay's `:on_output`
  chain as one cast — the one place a durable output leaves an outbox.
  """
  @spec dispatch(pid(), [output()]) :: :ok
  def dispatch(_overwatch, []), do: :ok
  def dispatch(overwatch, outputs), do: GenServer.cast(overwatch, {:io_out_batch, outputs})

  @doc """
  Run a dispatch batch's chains as the dispatcher: `fun` is the chain, folding
  `acc` from one output's run to the next, and the last `acc` is returned.
  Every `hold/1` an output's run takes is counted against that output, and the
  batch's entries are settled with their counts in one report per owning
  writer. A chain that crashes settles nothing — not its own entry and not the
  rest of the batch — and the resettle tick re-dispatches what stayed pending.
  """
  @spec run([output()], acc, (binary(), map(), acc -> acc)) :: acc when acc: term()
  def run(outputs, acc, fun) when is_function(fun, 3) do
    {acc, settled} =
      Enum.reduce(outputs, {acc, %{}}, fn {payload, metadata, %{owner: owner, key: key}},
                                          {acc, settled} ->
        Process.put(@run_key, {ack(metadata), 0})
        acc = fun.(payload, metadata, acc)
        {_token, holds} = Process.delete(@run_key)
        {acc, Map.update(settled, owner, [{key, holds}], &[{key, holds} | &1])}
      end)

    report_per_owner(settled, :outbox_settled_many)
    acc
  end

  @doc """
  Mint the confirmation token for one outbox entry: the Cell whose writer owns
  the cursor it moves (a Cell's writer owns its own output) and what one
  confirmation completes.
  """
  @spec token(Paths.cell_id(), key()) :: binary()
  def token(owner, {:cursor, seq}) do
    owner = cell_bytes(owner)
    encode(<<byte_size(owner)::16-big, owner::binary, 1, seq::64-big>>)
  end

  def token(owner, {n, seq}) do
    owner = cell_bytes(owner)
    encode(<<byte_size(owner)::16-big, owner::binary, 2, n::64-big, seq::64-big>>)
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
  Take the hold a relay's call or its answer keeps until its delivery answers
  it with `confirm_delivery/1`. An entry with a delivery hold outstanding is
  dispatched again at the resettle tick, while one only a sink holds waits.
  Inside a run it rides the run's one count, weighted by `delivery_hold/0`,
  so it costs no report of its own.
  """
  @spec hold_delivery(map()) :: :ok
  def hold_delivery(metadata) do
    # `hold/1`'s shape with its own weight: `hold/1` is every sink's per-output
    # call, so it does not take the weight as an argument.
    case {ack(metadata), Process.get(@run_key)} do
      {nil, _} ->
        :ok

      {token, {token, holds}} ->
        Process.put(@run_key, {token, holds + @delivery_hold})
        :ok

      _ ->
        notify(metadata, :outbox_delivery_hold)
    end
  end

  @doc """
  The weight of one delivery hold and its confirmation: above any number of
  sink holds, so one count carries both kinds and the run's one batched report
  stays as it is.
  """
  @spec delivery_hold() :: pos_integer()
  def delivery_hold(), do: @delivery_hold

  @doc "A delivery hold is answered: the caller took the call or its answer, or gave it up."
  @spec confirm_delivery(map()) :: :ok
  def confirm_delivery(metadata), do: notify(metadata, :outbox_delivery_confirmed)

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
  the dead-letter store — attributed to the output's own Cell and channel —
  then answer the hold so the cursor advances past exactly the parked range.
  Loud and replayable, never a silent drop. The entry's `error` is `error`'s
  catalogue object.
  """
  @spec park(map(), binary() | nil, Error.t(), non_neg_integer()) :: :ok
  def park(metadata, payload, %Error{} = error, attempts) when is_map(metadata) do
    with token when is_binary(token) <- ack(metadata),
         {:ok, %{owner: {rc, sh, pk}}} <- decode(token) do
      entry =
        DlqEntry.new!(%{
          relay_class: rc,
          tasklet_id: sh,
          cell_key: pk,
          channel: Map.get(metadata, "channel", ""),
          error: Error.object(error),
          attempts: attempts,
          payload: payload
        })

      case DlqStore.append(entry) do
        {parked, ref} when parked in [:ok, :existing] ->
          Logger.error(
            "[outbox] parked undeliverable output #{rc}/#{sh}/#{pk} " <>
              "seq=#{Map.get(metadata, "flz.seq")} epoch=#{Map.get(metadata, "flz.epoch")} " <>
              "after #{attempts} attempt(s): #{error.message} (#{error.code}) — dead-letter #{ref}"
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

  @doc "Decode a token into the Cell whose writer owns it and the entry one confirmation completes."
  @spec decode(binary()) :: {:ok, ack()} | :error
  def decode(token) when is_binary(token) do
    with {:ok, bytes} <- Base.url_decode64(token, padding: false) do
      decode_body(bytes)
    else
      _ -> :error
    end
  end

  defp decode_body(<<owner_len::16-big, owner::binary-size(owner_len), 1, seq::64-big>>) do
    case Wire.decode_cell_id(owner) do
      {:ok, owner_id} -> {:ok, %{owner: owner_id, key: {:cursor, seq}}}
      _ -> :error
    end
  end

  defp decode_body(
         <<owner_len::16-big, owner::binary-size(owner_len), 2, n::64-big, seq::64-big>>
       ) do
    case Wire.decode_cell_id(owner) do
      {:ok, owner_id} -> {:ok, %{owner: owner_id, key: {n, seq}}}
      _ -> :error
    end
  end

  defp decode_body(_), do: :error

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
    case WriterRegistry.lookup(owner) do
      {:ok, pid} -> GenServer.cast(pid, message)
      :error -> :ok
    end
  end

  defp cell_bytes({rc, sh, pk}), do: Wire.encode_cell_id(rc, sh, pk)
  defp cell_bytes(bytes) when is_binary(bytes), do: bytes

  defp encode(bytes), do: Base.url_encode64(bytes, padding: false)

  # ---------- output identity ----------

  @doc """
  Canonical label of a Cell identity — `<relay_class>/<tasklet_id>/<cell_key>`,
  the value carried by the `flz.cell` output stamp. Accepts the decoded triple
  or the on-wire identity bytes (undecodable bytes fall back to lowercase hex, so a
  corrupt identity is still traceable rather than fatal).
  """
  @spec cell_label({binary(), binary(), binary()} | binary()) :: binary()
  def cell_label({relay_class, tasklet_id, cell_key}),
    do: "#{relay_class}/#{tasklet_id}/#{cell_key}"

  def cell_label(cell_id) when is_binary(cell_id) do
    case Wire.decode_cell_id(cell_id) do
      {:ok, cell} -> cell_label(cell)
      {:error, _} -> Base.encode16(cell_id, case: :lower)
    end
  end

  @doc """
  The reserved output-identity metadata stamped on every durable output as it
  reaches the `:on_output` plugin chain — the ONE definition of the contract a
  conforming sink deduplicates on (`Flezha.Plugin.Provider.Postgres`, and a
  package's producer that writes it as a record header).

  The one durable outbox mints it, for a flow relay Cell under the
  effectively-once tier and a durable steps relay Cell alike
  (`Flezha.State.WalWriter`, `seq` = `<record seq>.<emit_ordinal>`). `seq` is
  a numeric dotted sequence so a sink can order it; `key` is omitted when the
  output carries no natural key.

  `ack` is the delivery-confirmation token (`ack_key/0`) the owning writer
  waits on before advancing its cursor. Every durable output carries one;
  live non-durable output carries none, and a sink treats its absence as "no
  cursor is waiting on this".
  """
  @spec output_stamps(%{
          required(:cell) => {binary(), binary(), binary()} | binary(),
          required(:seq) => binary(),
          required(:epoch) => non_neg_integer(),
          required(:channel) => binary(),
          optional(:key) => binary(),
          optional(:ack) => binary()
        }) :: %{binary() => binary()}
  def output_stamps(identity) do
    stamps = %{
      "channel" => identity.channel,
      "flz.cell" => cell_label(identity.cell),
      "flz.seq" => identity.seq,
      "flz.epoch" => Integer.to_string(identity.epoch)
    }

    stamps
    |> put_stamp("flz.key", Map.get(identity, :key))
    |> put_stamp(@ack_key, Map.get(identity, :ack))
  end

  defp put_stamp(stamps, key, value) when is_binary(value) and value != "",
    do: Map.put(stamps, key, value)

  defp put_stamp(stamps, _key, _value), do: stamps

  @doc """
  The identity `output_stamps/1` put on an output, read back: the Cell, the
  seq, the epoch, and a `sort` key ordering two identities of one Cell. The
  one reading every sink that guards on `(epoch, seq)` shares.

  An output has an identity when it names a Cell and a numeric dotted seq,
  not when it carries an epoch: a `.keyed()` channel off the effectively-once
  tier is stamped by the SDK, which cannot mint one. An absent epoch is epoch 0,
  where the outbox lineage starts, and every controller-side path that bumps it
  stamps it. `sort` compares the epoch, then the seq numerically, so `"10.0"`
  follows `"9.1"`.
  """
  @spec output_identity(map()) ::
          {:ok,
           %{
             cell: binary(),
             seq: binary(),
             epoch: integer(),
             sort: {integer(), [non_neg_integer()]}
           }}
          | :none
  def output_identity(metadata) when is_map(metadata) do
    cell = Map.get(metadata, "flz.cell", "")
    seq = Map.get(metadata, "flz.seq", "")

    if cell != "" and is_binary(seq) and seq =~ ~r/^\d+(\.\d+)*$/ do
      epoch = parse_epoch(Map.get(metadata, "flz.epoch"))
      sort = {epoch, seq |> String.split(".") |> Enum.map(&String.to_integer/1)}
      {:ok, %{cell: cell, seq: seq, epoch: epoch, sort: sort}}
    else
      :none
    end
  end

  @doc """
  The durable identity of an effectively-once output, `<flz.cell>/<flz.seq>/<flz.epoch>`,
  or `nil` for an output the tier did not stamp. A re-driven output carries the same
  one, so a sink hands it on as the key a receiver deduplicates on: the HTTP
  sender's `idempotency-key`, and the `flezha.dedup_key` record header of a
  package's producer.
  """
  @spec dedup_key(map()) :: binary() | nil
  def dedup_key(%{"flz.epoch" => epoch, "flz.cell" => cell, "flz.seq" => seq}),
    do: "#{cell}/#{seq}/#{epoch}"

  def dedup_key(_metadata), do: nil

  defp parse_epoch(epoch) when is_integer(epoch), do: epoch

  defp parse_epoch(epoch) when is_binary(epoch) do
    case Integer.parse(epoch) do
      {parsed, ""} -> parsed
      _unparsable -> 0
    end
  end

  defp parse_epoch(_absent), do: 0
end
