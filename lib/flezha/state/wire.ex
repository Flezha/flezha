defmodule Flezha.State.Wire do
  @moduledoc """
  Wire-protocol helpers for the per-Entity persistence path.

  Two new payload-id strings are reserved on the data stream:

    * `"entity_state_change"` — SDK → controller. Carries one Change record
      destined for the per-Entity WAL.
    * `"entity_state_snapshot"` — SDK → controller. Carries a serialized
      `Self::State` snapshot produced by an controller-initiated `CompactEntity`.

  ## Frame layout

  Both payload-ids share the same on-wire structure (the difference is
  the semantic of `change_or_snapshot_bytes`):

  ```
  [entity_id_len: u16 BE]
  [entity_id_bytes]
  [seq: u64 BE]
  [header: 16B]
  [body_len: u32 BE]
  [body]
  [crc32: u32 BE over body]
  ```

  ## `entity_id_bytes` structure

  ```
  [operative_class_len: u8][operative_class]
  [procedure_id_len: u8][procedure_id]
  [entity_key_len: u16 BE][entity_key]
  ```

  This is structured (not opaque) so the controller can route to the right
  per-Entity `WalWriter` without a side-table mapping. The controller's
  `runner_id` is implicit — the controller knows which runner
  sent the frame from the connection.
  """

  @payload_id_entity_state_change "entity_state_change"
  @payload_id_entity_state_snapshot "entity_state_snapshot"
  @payload_id_entity_effect_record "entity_effect_record"
  @payload_id_delete_entity_state "delete_entity_state"
  @payload_id_compensation_intent "compensation_intent"
  @payload_id_compensation_done "compensation_done"
  @payload_id_park_payload "park_payload"

  @header_size 16

  # Header byte 9 (`format_id`): 1 = plain postcard change, 2 = effect record.
  @format_id_effect_record 2

  @spec payload_id_entity_state_change() :: binary()
  def payload_id_entity_state_change(), do: @payload_id_entity_state_change

  @doc """
  Payload-id for an atomic effect record (effectively-once tier,
  SDK → controller): one invocation's state change(s), plugin outputs, and
  consumed input-log offset, persisted as ONE WAL record under ONE fsync.
  Same frame layout as `entity_state_change` with a `format_id = 2` header;
  the body is the postcard `EffectRecordBody` decoded by
  `decode_effect_body/1`.
  """
  @spec payload_id_entity_effect_record() :: binary()
  def payload_id_entity_effect_record(), do: @payload_id_entity_effect_record

  @spec payload_id_entity_state_snapshot() :: binary()
  def payload_id_entity_state_snapshot(), do: @payload_id_entity_state_snapshot

  @doc """
  Payload-id for the dynamic-mode hard-delete RPC (SDK → controller). The
  frame body is the bare structured `entity_id` (`decode_entity_id/1`);
  there is no seq / header / crc. The SDK ships it fire-and-forget when it
  evicts a persistence-enabled dynamic Entity, so the controller drops
  the WAL/snapshot/outbox files and a later recreate cold-starts fresh.

  The response (`{entity_id, status}`) is returned synchronously to the
  CLI delete path (`flz state delete`); the reaper path does not read a
  reply (fire-and-forget), so no response frame is written back on the
  data stream.
  """
  @spec payload_id_delete_entity_state() :: binary()
  def payload_id_delete_entity_state(), do: @payload_id_delete_entity_state

  @doc """
  Payload-ids reserved on the data stream for the workflow-operative saga
  change-log (SDK → controller), same frame layout as `entity_state_change`:
  `compensation_intent` records a procedure's completion (so a crash
  mid-compensation knows what to undo) and `compensation_done` records a
  compensation hook's completion (so a respawn skips already-undone steps).
  """
  @spec payload_id_compensation_intent() :: binary()
  def payload_id_compensation_intent(), do: @payload_id_compensation_intent

  @spec payload_id_compensation_done() :: binary()
  def payload_id_compensation_done(), do: @payload_id_compensation_done

  @doc """
  Payload-id for a dead-letter park (SDK → controller, tier-off sessions):
  a poison payload whose retry budget is exhausted, shipped with its error
  context for the durable DLQ store. Same frame layout as
  `entity_state_change` (zero header); the body is the postcard `ParkBody`
  decoded by `decode_park_body/1`. The controller answers with a
  `ParkPayloadAck` control command once the entry is fsynced.
  """
  @spec payload_id_park_payload() :: binary()
  def payload_id_park_payload(), do: @payload_id_park_payload

  @spec header_size() :: pos_integer()
  def header_size(), do: @header_size

  @doc """
  Encode an `entity_id` triple into its on-wire byte form.
  """
  @spec encode_entity_id(binary(), binary(), binary()) :: binary()
  def encode_entity_id(operative_class, procedure_id, entity_key)
      when is_binary(operative_class) and is_binary(procedure_id) and is_binary(entity_key) do
    rc_len = byte_size(operative_class)
    sh_len = byte_size(procedure_id)
    pk_len = byte_size(entity_key)

    if rc_len > 255 or sh_len > 255 do
      raise ArgumentError, "operative_class / procedure_id must be ≤ 255 bytes"
    end

    if pk_len > 0xFFFF do
      raise ArgumentError, "entity_key must be ≤ 65535 bytes"
    end

    <<rc_len::8, operative_class::binary, sh_len::8, procedure_id::binary, pk_len::16-big,
      entity_key::binary>>
  end

  @doc """
  Decode the on-wire `entity_id_bytes` into `{operative_class, procedure_id, entity_key}`.
  """
  @spec decode_entity_id(binary()) :: {:ok, {binary(), binary(), binary()}} | {:error, term()}
  def decode_entity_id(
        <<rc_len::8, operative_class::binary-size(rc_len), sh_len::8,
          procedure_id::binary-size(sh_len), pk_len::16-big, entity_key::binary-size(pk_len)>>
      ) do
    {:ok, {operative_class, procedure_id, entity_key}}
  end

  def decode_entity_id(_), do: {:error, :bad_entity_id}

  @doc """
  Canonical label of an Entity identity — `<operative_class>/<procedure_id>/<entity_key>`,
  the value carried by the `flz.entity` output stamp. Accepts the decoded triple
  or the on-wire identity bytes (undecodable bytes fall back to lowercase hex, so a
  corrupt identity is still traceable rather than fatal).
  """
  @spec entity_label({binary(), binary(), binary()} | binary()) :: binary()
  def entity_label({operative_class, procedure_id, entity_key}),
    do: "#{operative_class}/#{procedure_id}/#{entity_key}"

  def entity_label(entity_id) when is_binary(entity_id) do
    case decode_entity_id(entity_id) do
      {:ok, entity} -> entity_label(entity)
      {:error, _} -> Base.encode16(entity_id, case: :lower)
    end
  end

  @doc """
  The reserved output-identity metadata stamped on every durable output as it
  reaches the `:on_output` plugin chain — the ONE definition of the contract a
  conforming sink deduplicates on (`Flezha.Plugin.Provider.Postgres`,
  `Flezha.Plugin.Provider.Kafka`).

  Both durable outbox flushes mint it: the stream effectively-once tier
  (`Flezha.State.WalWriter`, `seq` = `<change_seq>.<emit_ordinal>`) and the
  workflow transactional outbox (`Flezha.State.ProcJournal`, `seq` =
  `<proc_idx>.<seq_in_proc>`, an undo carrying a trailing `.1`). `seq` is a numeric
  dotted sequence so a sink can order it; `key` is omitted when the output
  carries no natural key.

  `ack` is the delivery-confirmation token (`Flezha.State.Outbox`) the owning
  writer waits on before advancing its cursor. Both durable flushes mint one;
  live non-durable output carries none, and a sink treats its absence as "no
  cursor is waiting on this".
  """
  @spec output_stamps(%{
          required(:entity) => {binary(), binary(), binary()} | binary(),
          required(:seq) => binary(),
          required(:epoch) => non_neg_integer(),
          required(:channel) => binary(),
          optional(:key) => binary(),
          optional(:ack) => binary()
        }) :: %{binary() => binary()}
  def output_stamps(identity) do
    stamps = %{
      "channel" => identity.channel,
      "flz.entity" => entity_label(identity.entity),
      "flz.seq" => identity.seq,
      "flz.epoch" => Integer.to_string(identity.epoch)
    }

    stamps
    |> put_stamp("flz.key", Map.get(identity, :key))
    |> put_stamp(Flezha.State.Outbox.ack_key(), Map.get(identity, :ack))
  end

  defp put_stamp(stamps, key, value) when is_binary(value) and value != "",
    do: Map.put(stamps, key, value)

  defp put_stamp(stamps, _key, _value), do: stamps

  @doc """
  Decode an inbound `entity_state_change` / `entity_state_snapshot`
  frame.

  Returns `{:ok, %{entity_id: {operative_class, procedure_id, entity_key},
                   seq: u64, header: <<_::128>>, body: bytes}}` or an error.
  Verifies the trailing crc32 over the body.
  """
  @spec decode_frame(binary()) ::
          {:ok,
           %{
             entity_id: {binary(), binary(), binary()},
             seq: non_neg_integer(),
             header: <<_::128>>,
             body: binary()
           }}
          | {:error, term()}
  def decode_frame(<<entity_id_len::16-big, rest::binary>>) do
    case rest do
      <<entity_id_bytes::binary-size(^entity_id_len), seq::64-big,
        header::binary-size(@header_size), body_len::32-big, body::binary-size(body_len),
        crc::32-big>> ->
        if :erlang.crc32(body) == crc do
          case decode_entity_id(entity_id_bytes) do
            {:ok, identity} ->
              {:ok, %{entity_id: identity, seq: seq, header: header, body: body}}

            err ->
              err
          end
        else
          {:error, :crc_mismatch}
        end

      _ ->
        {:error, :short_read}
    end
  end

  def decode_frame(_), do: {:error, :short_read}

  @doc """
  Whether a WAL record header marks an effect record (`format_id = 2` at
  header byte 9) — the durable marker that keeps mixed WALs readable.
  """
  @spec effect_record_header?(binary()) :: boolean()
  def effect_record_header?(<<_::binary-size(9), format_id::8, _::binary>>),
    do: format_id == @format_id_effect_record

  def effect_record_header?(_), do: false

  @doc """
  Decode a postcard `EffectRecordBody` (the body of a
  `entity_effect_record` frame / format-2 WAL record):

  ```
  changes:         Vec<Vec<u8>>            — postcard S::Change bytes each
  outputs:         Vec<OutputRecord>       — {channel, key, metadata, payload}
  consumed_offset: Option<u64>
  dlq:             Option<DlqEntryBody>    — {channel, error, attempts, payload}
  event_time:      u64                     — the invocation's declared clock value
  timers:          Vec<TimerOp>            — {kind, scheduled_at, register}
  ```

  `event_time` is the extracted event time under an `EventTimeFrom` clock —
  the column a late-input correction resolves its baseline seq against.

  `timers` carries the SDK's mutations of the Entity's pending-timer set. The
  controller stores them and hands them back on restore without interpreting
  them — the same property that makes `changes` SDK-owned bytes — but decoding
  the field is what keeps the body's trailing-byte check honest.

  Postcard encodes `Vec`/`String` as a LEB128 varint length + contents,
  tuples/structs as concatenated fields, and `Option` as `0x00 | 0x01 ++ T`.
  """
  @spec decode_effect_body(binary()) ::
          {:ok,
           %{
             changes: [binary()],
             outputs: [
               %{
                 channel: binary(),
                 key: binary(),
                 metadata: [{binary(), binary()}],
                 payload: binary()
               }
             ],
             consumed_offset: non_neg_integer() | nil,
             dlq: map() | nil,
             event_time: non_neg_integer(),
             timers: [
               %{
                 kind: :event_time | :processing_time,
                 scheduled_at: non_neg_integer(),
                 register: boolean()
               }
             ]
           }}
          | {:error, term()}
  def decode_effect_body(body) when is_binary(body) do
    with {:ok, changes, rest} <- decode_vec(body, &decode_bytes/1),
         {:ok, outputs, rest} <- decode_vec(rest, &decode_output/1),
         {:ok, consumed_offset, rest} <- decode_option(rest, &decode_varint/1),
         {:ok, dlq, rest} <- decode_option(rest, &decode_dlq_entry/1),
         {:ok, event_time, rest} <- decode_varint(rest),
         {:ok, timers, <<>>} <- decode_vec(rest, &decode_timer_op/1) do
      {:ok,
       %{
         changes: changes,
         outputs: outputs,
         consumed_offset: consumed_offset,
         dlq: dlq,
         event_time: event_time,
         timers: timers
       }}
    else
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  @doc """
  Encode an effect body back to postcard bytes (byte-exact inverse of
  `decode_effect_body/1`). Used by tests and the WAL crash-tear fixtures.
  """
  @spec encode_effect_body(%{
          required(:changes) => [binary()],
          required(:outputs) => [
            %{
              channel: binary(),
              key: binary(),
              metadata: [{binary(), binary()}],
              payload: binary()
            }
          ],
          required(:consumed_offset) => non_neg_integer() | nil,
          optional(:dlq) => map() | nil,
          optional(:event_time) => non_neg_integer(),
          optional(:timers) => [map()]
        }) :: binary()
  def encode_effect_body(%{changes: changes, outputs: outputs, consumed_offset: offset} = body) do
    IO.iodata_to_binary([
      encode_vec(changes, &encode_bytes/1),
      encode_vec(outputs, &encode_output/1),
      encode_option(offset, &encode_varint/1),
      encode_option(Map.get(body, :dlq), &encode_dlq_entry/1),
      encode_varint(Map.get(body, :event_time, 0)),
      encode_vec(Map.get(body, :timers, []), &encode_timer_op/1)
    ])
  end

  @doc """
  Decode a postcard `ParkBody` (the body of a `park_payload` frame,
  tier-off dead-letter park):

  ```
  park_id:       String              — SDK-minted ack-correlation id
  origin_offset: Option<u64>
  entry:         DlqEntryBody        — {channel, error, attempts, payload},
                                       the same record a tier-on park embeds
                                       in its effect record
  ```

  Returned flattened (`park_id` + `origin_offset` merged into the entry
  map) — the store consumes one shape for both park paths.
  """
  @spec decode_park_body(binary()) ::
          {:ok,
           %{
             park_id: binary(),
             channel: binary(),
             origin_offset: non_neg_integer() | nil,
             error: binary(),
             attempts: non_neg_integer(),
             payload: binary() | nil
           }}
          | {:error, term()}
  def decode_park_body(body) when is_binary(body) do
    with {:ok, park_id, rest} <- decode_bytes(body),
         {:ok, origin_offset, rest} <- decode_option(rest, &decode_varint/1),
         {:ok, entry, <<>>} <- decode_dlq_entry(rest) do
      {:ok, Map.merge(entry, %{park_id: park_id, origin_offset: origin_offset})}
    else
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  @doc """
  Decode a WAL record as an effect record, or `nil` when it is not one (a plain
  change record, or a body this build cannot read).

  The header check and the body decode always travel together — every reader
  that wants an effect record's fields wants exactly this — so they live here,
  beside the codec they belong to, rather than being re-paired at each call site.
  """
  @spec decode_effect_record(binary(), binary()) :: map() | nil
  def decode_effect_record(header, body) do
    with true <- effect_record_header?(header),
         {:ok, decoded} <- decode_effect_body(body) do
      decoded
    else
      _ -> nil
    end
  end

  @doc """
  Decode a postcard `CorrectionRequestBody` (the body of a `correction_request`
  frame): a single `target_event_time: u64`. The Entity identity rides the
  frame envelope, so the body carries nothing else — the controller re-reads the
  input-log range itself, because the late input is already in it.
  """
  @spec decode_correction_request(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def decode_correction_request(body) when is_binary(body) do
    case decode_varint(body) do
      {:ok, target, <<>>} -> {:ok, target}
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  # ---------- postcard primitives ----------

  defp decode_vec(bin, item_fun) do
    case decode_varint(bin) do
      {:ok, count, rest} -> decode_items(rest, count, item_fun, [])
      err -> err
    end
  end

  defp decode_items(rest, 0, _fun, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_items(rest, n, fun, acc) do
    case fun.(rest) do
      {:ok, item, rest} -> decode_items(rest, n - 1, fun, [item | acc])
      err -> err
    end
  end

  defp decode_bytes(bin) do
    with {:ok, len, rest} <- decode_varint(bin),
         <<bytes::binary-size(^len), rest::binary>> <- rest do
      {:ok, bytes, rest}
    else
      {:error, _} = err -> err
      _ -> {:error, :short_read}
    end
  end

  defp decode_output(bin) do
    with {:ok, channel, rest} <- decode_bytes(bin),
         {:ok, key, rest} <- decode_bytes(rest),
         {:ok, metadata, rest} <- decode_vec(rest, &decode_pair/1),
         {:ok, payload, rest} <- decode_bytes(rest) do
      {:ok, %{channel: channel, key: key, metadata: metadata, payload: payload}, rest}
    end
  end

  defp decode_pair(bin) do
    with {:ok, k, rest} <- decode_bytes(bin),
         {:ok, v, rest} <- decode_bytes(rest) do
      {:ok, {k, v}, rest}
    end
  end

  defp decode_dlq_entry(bin) do
    with {:ok, channel, rest} <- decode_bytes(bin),
         {:ok, error, rest} <- decode_bytes(rest),
         {:ok, attempts, rest} <- decode_varint(rest),
         {:ok, payload, rest} <- decode_option(rest, &decode_bytes/1) do
      {:ok, %{channel: channel, error: error, attempts: attempts, payload: payload}, rest}
    end
  end

  defp encode_dlq_entry(%{channel: channel, error: error, attempts: attempts, payload: payload}) do
    [
      encode_bytes(channel),
      encode_bytes(error),
      encode_varint(attempts),
      encode_option(payload, &encode_bytes/1)
    ]
  end

  # One pending-timer mutation: `[kind varint][scheduled_at varint][register u8]`.
  # The kind's ordinal order is the SDKs' `TimerKind`: 0 event time, 1 processing
  # time.
  defp decode_timer_op(bin) do
    with {:ok, kind, rest} <- decode_varint(bin),
         {:ok, scheduled_at, rest} <- decode_varint(rest),
         <<register::8, rest::binary>> <- rest,
         true <- register <= 1 do
      {:ok, %{kind: timer_kind(kind), scheduled_at: scheduled_at, register: register == 1}, rest}
    else
      {:error, _} = err -> err
      _ -> {:error, :bad_timer_op}
    end
  end

  defp timer_kind(0), do: :event_time
  defp timer_kind(1), do: :processing_time
  defp timer_kind(other), do: {:unknown, other}

  defp encode_timer_op(%{kind: kind, scheduled_at: scheduled_at, register: register}) do
    [
      encode_varint(timer_kind_ordinal(kind)),
      encode_varint(scheduled_at),
      <<if(register, do: 1, else: 0)>>
    ]
  end

  defp timer_kind_ordinal(:event_time), do: 0
  defp timer_kind_ordinal(:processing_time), do: 1

  defp decode_option(<<0, rest::binary>>, _item_fun), do: {:ok, nil, rest}
  defp decode_option(<<1, rest::binary>>, item_fun), do: item_fun.(rest)
  defp decode_option(_, _), do: {:error, :bad_option}

  defp encode_option(nil, _item_fun), do: <<0>>
  defp encode_option(value, item_fun), do: [<<1>>, item_fun.(value)]

  defp decode_varint(bin, shift \\ 0, acc \\ 0)

  defp decode_varint(<<byte::8, rest::binary>>, shift, acc) when byte < 128,
    do: {:ok, acc + Bitwise.bsl(byte, shift), rest}

  defp decode_varint(<<byte::8, rest::binary>>, shift, acc) when shift < 64,
    do: decode_varint(rest, shift + 7, acc + Bitwise.bsl(byte - 128, shift))

  defp decode_varint(_, _, _), do: {:error, :bad_varint}

  defp encode_vec(items, item_fun),
    do: [encode_varint(length(items)) | Enum.map(items, item_fun)]

  defp encode_bytes(bytes), do: [encode_varint(byte_size(bytes)), bytes]

  defp encode_output(%{channel: channel, key: key, metadata: metadata, payload: payload}) do
    [
      encode_bytes(channel),
      encode_bytes(key),
      encode_vec(metadata, fn {k, v} -> [encode_bytes(k), encode_bytes(v)] end),
      encode_bytes(payload)
    ]
  end

  defp encode_varint(n) when n < 128, do: <<n>>

  defp encode_varint(n),
    do: <<Bitwise.bor(Bitwise.band(n, 127), 128), encode_varint(Bitwise.bsr(n, 7))::binary>>
end
