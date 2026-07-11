defmodule Flezha.State.Wire do
  @moduledoc """
  Wire-protocol helpers for the per-Segment persistence path.

  Two new payload-id strings are reserved on the data stream:

    * `"segment_state_change"` — SDK → controller. Carries one Change record
      destined for the per-Segment WAL.
    * `"segment_state_snapshot"` — SDK → controller. Carries a serialized
      `Self::State` snapshot produced by an controller-initiated `CompactSegment`.

  ## Frame layout

  Both payload-ids share the same on-wire structure (the difference is
  the semantic of `change_or_snapshot_bytes`):

  ```
  [segment_id_len: u16 BE]
  [segment_id_bytes]
  [seq: u64 BE]
  [header: 16B]
  [body_len: u32 BE]
  [body]
  [crc32: u32 BE over body]
  ```

  ## `segment_id_bytes` structure

  ```
  [operative_class_len: u8][operative_class]
  [operation_id_len: u8][operation_id]
  [segment_key_len: u16 BE][segment_key]
  ```

  This is structured (not opaque) so the controller can route to the right
  per-Segment `WalWriter` without a side-table mapping. The controller's
  `executor_id` is implicit — the controller knows which executor
  sent the frame from the connection.
  """

  @payload_id_segment_state_change "segment_state_change"
  @payload_id_segment_state_snapshot "segment_state_snapshot"
  @payload_id_segment_effect_record "segment_effect_record"
  @payload_id_delete_segment_state "delete_segment_state"
  @payload_id_compensation_intent "compensation_intent"
  @payload_id_compensation_done "compensation_done"

  @header_size 16

  # Header byte 9 (`format_id`): 1 = plain postcard change, 2 = effect record.
  @format_id_effect_record 2

  @spec payload_id_segment_state_change() :: binary()
  def payload_id_segment_state_change(), do: @payload_id_segment_state_change

  @doc """
  Payload-id for an atomic effect record (effectively-once tier,
  SDK → controller): one invocation's state change(s), plugin outputs, and
  consumed input-log offset, persisted as ONE WAL record under ONE fsync.
  Same frame layout as `segment_state_change` with a `format_id = 2` header;
  the body is the postcard `EffectRecordBody` decoded by
  `decode_effect_body/1`.
  """
  @spec payload_id_segment_effect_record() :: binary()
  def payload_id_segment_effect_record(), do: @payload_id_segment_effect_record

  @spec payload_id_segment_state_snapshot() :: binary()
  def payload_id_segment_state_snapshot(), do: @payload_id_segment_state_snapshot

  @doc """
  Payload-id for the dynamic-mode hard-delete RPC (SDK → controller). The
  frame body is the bare structured `segment_id` (`decode_segment_id/1`);
  there is no seq / header / crc. The SDK ships it fire-and-forget when it
  evicts a persistence-enabled dynamic Segment, so the controller drops
  the WAL/snapshot/outbox files and a later recreate cold-starts fresh.

  The response (`{segment_id, status}`) is returned synchronously to the
  CLI delete path (`flz state delete`); the reaper path does not read a
  reply (fire-and-forget), so no response frame is written back on the
  data stream.
  """
  @spec payload_id_delete_segment_state() :: binary()
  def payload_id_delete_segment_state(), do: @payload_id_delete_segment_state

  @doc """
  Payload-ids reserved on the data stream for the bounded-operative saga
  change-log (SDK → controller), same frame layout as `segment_state_change`:
  `compensation_intent` records an operation's completion (so a crash
  mid-compensation knows what to undo) and `compensation_done` records a
  compensation hook's completion (so a respawn skips already-undone steps).
  """
  @spec payload_id_compensation_intent() :: binary()
  def payload_id_compensation_intent(), do: @payload_id_compensation_intent

  @spec payload_id_compensation_done() :: binary()
  def payload_id_compensation_done(), do: @payload_id_compensation_done

  @spec header_size() :: pos_integer()
  def header_size(), do: @header_size

  @doc """
  Encode a `segment_id` triple into its on-wire byte form.
  """
  @spec encode_segment_id(binary(), binary(), binary()) :: binary()
  def encode_segment_id(operative_class, operation_id, segment_key)
      when is_binary(operative_class) and is_binary(operation_id) and is_binary(segment_key) do
    rc_len = byte_size(operative_class)
    sh_len = byte_size(operation_id)
    pk_len = byte_size(segment_key)

    if rc_len > 255 or sh_len > 255 do
      raise ArgumentError, "operative_class / operation_id must be ≤ 255 bytes"
    end

    if pk_len > 0xFFFF do
      raise ArgumentError, "segment_key must be ≤ 65535 bytes"
    end

    <<rc_len::8, operative_class::binary, sh_len::8, operation_id::binary, pk_len::16-big,
      segment_key::binary>>
  end

  @doc """
  Decode the on-wire `segment_id_bytes` into `{operative_class, operation_id, segment_key}`.
  """
  @spec decode_segment_id(binary()) :: {:ok, {binary(), binary(), binary()}} | {:error, term()}
  def decode_segment_id(
        <<rc_len::8, operative_class::binary-size(rc_len), sh_len::8,
          operation_id::binary-size(sh_len), pk_len::16-big, segment_key::binary-size(pk_len)>>
      ) do
    {:ok, {operative_class, operation_id, segment_key}}
  end

  def decode_segment_id(_), do: {:error, :bad_segment_id}

  @doc """
  Decode an inbound `segment_state_change` / `segment_state_snapshot`
  frame.

  Returns `{:ok, %{segment_id: {operative_class, operation_id, segment_key},
                   seq: u64, header: <<_::128>>, body: bytes}}` or an error.
  Verifies the trailing crc32 over the body.
  """
  @spec decode_frame(binary()) ::
          {:ok,
           %{
             segment_id: {binary(), binary(), binary()},
             seq: non_neg_integer(),
             header: <<_::128>>,
             body: binary()
           }}
          | {:error, term()}
  def decode_frame(<<segment_id_len::16-big, rest::binary>>) do
    case rest do
      <<segment_id_bytes::binary-size(segment_id_len), seq::64-big,
        header::binary-size(@header_size), body_len::32-big, body::binary-size(body_len),
        crc::32-big>> ->
        if :erlang.crc32(body) == crc do
          case decode_segment_id(segment_id_bytes) do
            {:ok, identity} ->
              {:ok, %{segment_id: identity, seq: seq, header: header, body: body}}

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
  `segment_effect_record` frame / format-2 WAL record):

  ```
  changes:         Vec<Vec<u8>>            — postcard S::Change bytes each
  outputs:         Vec<OutputRecord>       — {channel, key, metadata, payload}
  consumed_offset: Option<u64>
  ```

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
             consumed_offset: non_neg_integer() | nil
           }}
          | {:error, term()}
  def decode_effect_body(body) when is_binary(body) do
    with {:ok, changes, rest} <- decode_vec(body, &decode_bytes/1),
         {:ok, outputs, rest} <- decode_vec(rest, &decode_output/1),
         {:ok, consumed_offset, <<>>} <- decode_option_varint(rest) do
      {:ok, %{changes: changes, outputs: outputs, consumed_offset: consumed_offset}}
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
          changes: [binary()],
          outputs: [
            %{
              channel: binary(),
              key: binary(),
              metadata: [{binary(), binary()}],
              payload: binary()
            }
          ],
          consumed_offset: non_neg_integer() | nil
        }) :: binary()
  def encode_effect_body(%{changes: changes, outputs: outputs, consumed_offset: offset}) do
    IO.iodata_to_binary([
      encode_vec(changes, &encode_bytes/1),
      encode_vec(outputs, &encode_output/1),
      encode_option_varint(offset)
    ])
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
         <<bytes::binary-size(len), rest::binary>> <- rest do
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

  defp decode_option_varint(<<0, rest::binary>>), do: {:ok, nil, rest}

  defp decode_option_varint(<<1, rest::binary>>) do
    case decode_varint(rest) do
      {:ok, value, rest} -> {:ok, value, rest}
      err -> err
    end
  end

  defp decode_option_varint(_), do: {:error, :bad_option}

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

  defp encode_option_varint(nil), do: <<0>>
  defp encode_option_varint(value), do: [<<1>>, encode_varint(value)]

  defp encode_varint(n) when n < 128, do: <<n>>

  defp encode_varint(n),
    do: <<Bitwise.bor(Bitwise.band(n, 127), 128), encode_varint(Bitwise.bsr(n, 7))::binary>>
end
