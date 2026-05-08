defmodule Flezha.State.Wire do
  @moduledoc """
  Wire-protocol helpers for the per-Partition persistence path.

  Two new payload-id strings are reserved on the data stream:

    * `"partition_state_change"` — SDK → controller. Carries one Change record
      destined for the per-Partition WAL.
    * `"partition_state_snapshot"` — SDK → controller. Carries a serialized
      `Self::State` snapshot produced by an controller-initiated `CompactPartition`.

  ## Frame layout

  Both payload-ids share the same on-wire structure (the difference is
  the semantic of `change_or_snapshot_bytes`):

  ```
  [partition_id_len: u16 BE]
  [partition_id_bytes]
  [seq: u64 BE]
  [header: 16B]
  [body_len: u32 BE]
  [body]
  [crc32: u32 BE over body]
  ```

  ## `partition_id_bytes` structure

  ```
  [relay_class_len: u8][relay_class]
  [shard_id_len: u8][shard_id]
  [partition_key_len: u16 BE][partition_key]
  ```

  This is structured (not opaque) so the controller can route to the right
  per-Partition `WalWriter` without a side-table mapping. The controller's
  `executor_id` is implicit — the controller knows which executor
  sent the frame from the connection.
  """

  @payload_id_partition_state_change "partition_state_change"
  @payload_id_partition_state_snapshot "partition_state_snapshot"

  @header_size 16

  @spec payload_id_partition_state_change() :: binary()
  def payload_id_partition_state_change(), do: @payload_id_partition_state_change

  @spec payload_id_partition_state_snapshot() :: binary()
  def payload_id_partition_state_snapshot(), do: @payload_id_partition_state_snapshot

  @spec header_size() :: pos_integer()
  def header_size(), do: @header_size

  @doc """
  Encode a `partition_id` triple into its on-wire byte form.
  """
  @spec encode_partition_id(binary(), binary(), binary()) :: binary()
  def encode_partition_id(relay_class, shard_id, partition_key)
      when is_binary(relay_class) and is_binary(shard_id) and is_binary(partition_key) do
    rc_len = byte_size(relay_class)
    sh_len = byte_size(shard_id)
    pk_len = byte_size(partition_key)

    if rc_len > 255 or sh_len > 255 do
      raise ArgumentError, "relay_class / shard_id must be ≤ 255 bytes"
    end

    if pk_len > 0xFFFF do
      raise ArgumentError, "partition_key must be ≤ 65535 bytes"
    end

    <<rc_len::8, relay_class::binary, sh_len::8, shard_id::binary, pk_len::16-big,
      partition_key::binary>>
  end

  @doc """
  Decode the on-wire `partition_id_bytes` into `{relay_class, shard_id, partition_key}`.
  """
  @spec decode_partition_id(binary()) :: {:ok, {binary(), binary(), binary()}} | {:error, term()}
  def decode_partition_id(
        <<rc_len::8, relay_class::binary-size(rc_len), sh_len::8,
          shard_id::binary-size(sh_len), pk_len::16-big,
          partition_key::binary-size(pk_len)>>
      ) do
    {:ok, {relay_class, shard_id, partition_key}}
  end

  def decode_partition_id(_), do: {:error, :bad_partition_id}

  @doc """
  Decode an inbound `partition_state_change` / `partition_state_snapshot`
  frame.

  Returns `{:ok, %{partition_id: {relay_class, shard_id, partition_key},
                   seq: u64, header: <<_::128>>, body: bytes}}` or an error.
  Verifies the trailing crc32 over the body.
  """
  @spec decode_frame(binary()) ::
          {:ok,
           %{
             partition_id: {binary(), binary(), binary()},
             seq: non_neg_integer(),
             header: <<_::128>>,
             body: binary()
           }}
          | {:error, term()}
  def decode_frame(<<partition_id_len::16-big, rest::binary>>) do
    case rest do
      <<partition_id_bytes::binary-size(partition_id_len), seq::64-big,
        header::binary-size(@header_size), body_len::32-big,
        body::binary-size(body_len), crc::32-big>> ->
        if :erlang.crc32(body) == crc do
          case decode_partition_id(partition_id_bytes) do
            {:ok, identity} ->
              {:ok, %{partition_id: identity, seq: seq, header: header, body: body}}

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
