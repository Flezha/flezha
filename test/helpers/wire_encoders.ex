defmodule FlezhaTest.Helper.WireEncoders do
  @moduledoc """
  The write side of the per-Cell persistence codec, which only tests need: a
  runner writes these frames and the controller only decodes them. Each
  function is the byte-exact inverse of its decoder in `Flezha.State.Wire` or
  `Flezha.State.Frame`. `test/fixtures/durable_frame.txt` pins the header and
  `test/fixtures/postcard_bodies.txt` the effect and park bodies.
  """

  alias Flezha.State.Frame

  @header_size Frame.header_size()

  @doc """
  Assemble a durable frame: the Cell identity, its seq, the 16-byte record
  header and the body under its CRC. The inverse of `Wire.decode_frame/1` and
  the byte-for-byte twin of the runner core's `build_cell_frame`.
  """
  @spec encode_frame(binary(), non_neg_integer(), binary(), binary()) :: binary()
  def encode_frame(cell_id, seq, header, body)
      when byte_size(header) == @header_size do
    <<byte_size(cell_id)::16-big, cell_id::binary, seq::64-big, header::binary,
      byte_size(body)::32-big, body::binary, :erlang.crc32(body)::32-big>>
  end

  @doc """
  Stamp the 16-byte header for `body`: language, SDK version, schema version,
  format, two reserved zero bytes, and the crc32 over the body. The inverse of
  `Frame.decode_header/1`.
  """
  @spec encode_header(byte(), non_neg_integer(), non_neg_integer(), byte(), binary()) ::
          <<_::128>>
  def encode_header(sdk_lang, sdk_version, schema_version, format_id, body)
      when is_binary(body) do
    <<sdk_lang, sdk_version::32-big, schema_version::32-big, format_id, 0, 0,
      :erlang.crc32(body)::32-big>>
  end

  @doc "An effect body as postcard bytes, the inverse of `Wire.decode_effect_body/1`."
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
          optional(:timers) => [map()],
          optional(:edges) => [Flezha.State.Wire.edge()]
        }) :: binary()
  def encode_effect_body(%{changes: changes, outputs: outputs, consumed_offset: offset} = body) do
    IO.iodata_to_binary([
      encode_vec(changes, &encode_bytes/1),
      encode_vec(outputs, &encode_output/1),
      encode_option(offset, &encode_varint/1),
      encode_option(Map.get(body, :dlq), &encode_dlq_entry/1),
      encode_varint(Map.get(body, :event_time, 0)),
      encode_vec(Map.get(body, :timers, []), &encode_timer_op/1),
      encode_vec(Map.get(body, :edges, []), &encode_edge/1)
    ])
  end

  @doc """
  A `ParkBody` as postcard bytes, the inverse of `Wire.decode_park_body/1`.
  The map carries the flattened DLQ entry: `park_id`, `origin_offset`,
  `channel`, `error`, `attempts`, `payload`, `reply_to`.
  """
  @spec encode_park_body(%{
          required(:park_id) => binary(),
          optional(:origin_offset) => non_neg_integer() | nil,
          required(:channel) => binary(),
          required(:error) => binary(),
          required(:attempts) => non_neg_integer(),
          optional(:payload) => binary() | nil,
          optional(:reply_to) => binary() | nil
        }) :: binary()
  def encode_park_body(
        %{park_id: park_id, channel: channel, error: error, attempts: attempts} = body
      ) do
    entry = %{
      channel: channel,
      error: error,
      attempts: attempts,
      payload: Map.get(body, :payload)
    }

    IO.iodata_to_binary([
      encode_bytes(park_id),
      encode_option(Map.get(body, :origin_offset), &encode_varint/1),
      encode_dlq_entry(entry),
      encode_option(Map.get(body, :reply_to), &encode_bytes/1)
    ])
  end

  defp encode_edge(%{channel: channel, consumer: consumer, origin: origin, payload: payload}) do
    [
      encode_varint(channel),
      encode_varint(consumer),
      encode_option(origin, &encode_varint/1),
      encode_bytes(payload)
    ]
  end

  defp encode_dlq_entry(%{channel: channel, error: error, attempts: attempts, payload: payload}) do
    [
      encode_bytes(channel),
      encode_bytes(error),
      encode_varint(attempts),
      encode_option(payload, &encode_bytes/1)
    ]
  end

  defp encode_timer_op(%{kind: kind, scheduled_at: scheduled_at, register: register}) do
    [
      encode_varint(timer_kind_ordinal(kind)),
      encode_varint(scheduled_at),
      <<if(register, do: 1, else: 0)>>
    ]
  end

  defp timer_kind_ordinal(:event_time), do: 0
  defp timer_kind_ordinal(:processing_time), do: 1

  defp encode_option(nil, _item_fun), do: <<0>>
  defp encode_option(value, item_fun), do: [<<1>>, item_fun.(value)]

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
