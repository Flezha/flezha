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
  @payload_id_delete_segment_state "delete_segment_state"
  @payload_id_compensation_intent "compensation_intent"
  @payload_id_compensation_done "compensation_done"

  @header_size 16

  @spec payload_id_segment_state_change() :: binary()
  def payload_id_segment_state_change(), do: @payload_id_segment_state_change

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
end
