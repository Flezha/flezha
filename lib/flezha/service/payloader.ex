defmodule Flezha.Service.Payloader do
  alias Flezha.Computation.Builder, as: ComputationBuilder

  def json_to_map(""), do: %{}

  def json_to_map(json) do
    case Jason.decode(json) do
      {:ok, map} -> map
      {:error, reason} -> raise reason
    end
  end

  def build_from_bin(id, message, metadata \\ %{}) when is_binary(id) and is_binary(message) do
    # Ensure id is exactly 32 bytes.
    payload_id_padded = String.pad_trailing(id, 32)

    # Calculate message size in bytes (4 bytes integer for 4GB limit).
    message_size = byte_size(message)
    message_size_binary = <<message_size::32>>

    # Serialize metadata
    metadata_binary = serialize_metadata(metadata)
    metadata_size = byte_size(metadata_binary)
    metadata_size_binary = <<metadata_size::16>>

    # Construct the payload: payload_id (32) + message_size (4) + message + metadata_size (2) + metadata
    payload_id_padded <> message_size_binary <> message <> metadata_size_binary <> metadata_binary
  end

  defp serialize_metadata(metadata) when is_map(metadata) do
    metadata
    |> Enum.flat_map(fn {key, value} ->
      key_str = to_string(key)
      value_str = to_string(value)
      [<<byte_size(key_str)::8>>, <<byte_size(value_str)::8>>, key_str, value_str]
    end)
    |> :erlang.iolist_to_binary()
  end

  defp deserialize_metadata(<<>>), do: %{}

  defp deserialize_metadata(metadata_binary) do
    deserialize_metadata_pairs(metadata_binary, %{})
  end

  defp deserialize_metadata_pairs(<<>>, acc), do: acc

  defp deserialize_metadata_pairs(<<key_size::8, value_size::8, rest::binary>>, acc) do
    case rest do
      <<key::binary-size(key_size), value::binary-size(value_size), remaining::binary>> ->
        deserialize_metadata_pairs(remaining, Map.put(acc, key, value))

      _ ->
        acc
    end
  end

  # Binary message format:
  # - First 32 bytes: payload_id (padded with spaces if needed)
  # - Next 4 bytes: message_size (32-bit big-endian integer, 4GB limit)
  # - Next {message_size} bytes: the actual message
  # - Next 2 bytes: metadata_size (16-bit big-endian integer)
  # - Next {metadata_size} bytes: metadata (key-value pairs with 1-byte size prefixes)
  def parse_frame(frame), do: parse_frame({:empty, <<>>}, frame, [])

  def parse_frame(buffer, frame, payloads) do
    {buffer, bytes_left} = fill_buffer(buffer, frame)

    case buffer do
      {:complete, payload} ->
        # Append to the payload list to preserve the order of the payloads.
        payloads = payloads ++ [payload]

        if bytes_left === <<>> do
          {payloads, {:empty, <<>>}}
        else
          parse_frame({:empty, <<>>}, bytes_left, payloads)
        end

      _ ->
        # If we have bytes left and buffer is incomplete, continue processing
        if bytes_left !== <<>> do
          parse_frame(buffer, bytes_left, payloads)
        else
          {payloads, buffer}
        end
    end
  end

  def construct_computation!(input, type) do
    result =
      case type do
        :json -> ComputationBuilder.from_json(input)
        :map -> ComputationBuilder.from_map(input)
      end

    case result do
      {:ok, computation} -> computation
      {:error, reason} -> raise reason
    end
  end

  defp handle_incomplete_msg_buf(buf, payload_id, frame, bytes_to_take) do
    frame_size = byte_size(frame)

    if frame_size >= bytes_to_take do
      <<message::binary-size(bytes_to_take), bytes_left::binary>> = frame
      complete_message = buf <> message

      {{:incomplete_metadata, {payload_id, complete_message}}, bytes_left}
    else
      <<message::binary-size(frame_size), _bytes_left::binary>> = frame
      bytes_to_take = bytes_to_take - frame_size

      {{:incomplete_msg, {payload_id, buf <> message, bytes_to_take}}, <<>>}
    end
  end

  defp handle_incomplete_metadata_buf(payload_id, message, frame, metadata_bytes_to_take) do
    frame_size = byte_size(frame)

    if frame_size >= metadata_bytes_to_take do
      <<metadata_binary::binary-size(metadata_bytes_to_take), bytes_left::binary>> = frame
      metadata = deserialize_metadata(metadata_binary)

      {{:complete, {payload_id, message, metadata}}, bytes_left}
    else
      metadata_bytes_to_take = metadata_bytes_to_take - frame_size

      {{:incomplete_metadata, {payload_id, message, frame, metadata_bytes_to_take}}, <<>>}
    end
  end

  def handle_incomplete_frame_buf(buf, frame) do
    new_frame = buf <> frame

    parse_payload_new(new_frame)
  end

  defp fill_buffer(buffer, <<>>), do: {buffer, <<>>}

  defp fill_buffer(buffer, frame) do
    case buffer do
      # The existing buffer was empty, treat the frame as a new payload.
      {:empty, _buf} ->
        parse_payload_new(frame)

      {:incomplete_frame, buf} ->
        handle_incomplete_frame_buf(buf, frame)

      {:incomplete_msg, {payload_id, message_buf, bytes_to_take}} ->
        handle_incomplete_msg_buf(message_buf, payload_id, frame, bytes_to_take)

      {:incomplete_metadata, {payload_id, message}} ->
        handle_incomplete_metadata_size(payload_id, message, frame)

      {:incomplete_metadata, {payload_id, message, metadata_size_buf}} ->
        handle_incomplete_metadata_size_buffered(payload_id, message, metadata_size_buf, frame)

      {:incomplete_metadata, {payload_id, message, metadata_buf, metadata_bytes_to_take}} ->
        handle_incomplete_metadata_buf(
          payload_id,
          message,
          metadata_buf <> frame,
          metadata_bytes_to_take
        )

      any ->
        raise "Unexpected buffer state: #{inspect(any)}"
    end
  end

  defp handle_incomplete_metadata_size(payload_id, message, frame) do
    case frame do
      <<metadata_size::16, metadata_frame::binary>> ->
        if byte_size(metadata_frame) >= metadata_size do
          <<metadata_binary::binary-size(metadata_size), bytes_left::binary>> = metadata_frame
          metadata = deserialize_metadata(metadata_binary)
          {{:complete, {payload_id, message, metadata}}, bytes_left}
        else
          metadata_bytes_to_take = metadata_size - byte_size(metadata_frame)

          {{:incomplete_metadata, {payload_id, message, metadata_frame, metadata_bytes_to_take}},
           <<>>}
        end

      # If we don't have enough bytes for metadata size, buffer what we have
      _ when byte_size(frame) < 2 ->
        {{:incomplete_metadata, {payload_id, message, frame}}, <<>>}

      _ ->
        {{:incomplete_metadata, {payload_id, message}}, <<>>}
    end
  end

  defp handle_incomplete_metadata_size_buffered(payload_id, message, metadata_size_buf, frame) do
    combined_buf = metadata_size_buf <> frame

    case combined_buf do
      <<metadata_size::16, metadata_frame::binary>> ->
        if byte_size(metadata_frame) >= metadata_size do
          <<metadata_binary::binary-size(metadata_size), bytes_left::binary>> = metadata_frame
          metadata = deserialize_metadata(metadata_binary)
          {{:complete, {payload_id, message, metadata}}, bytes_left}
        else
          metadata_bytes_to_take = metadata_size - byte_size(metadata_frame)

          {{:incomplete_metadata, {payload_id, message, metadata_frame, metadata_bytes_to_take}},
           <<>>}
        end

      # Still don't have enough bytes for metadata size
      _ when byte_size(combined_buf) < 2 ->
        {{:incomplete_metadata, {payload_id, message, combined_buf}}, <<>>}

      _ ->
        {{:incomplete_metadata, {payload_id, message}}, <<>>}
    end
  end

  defp parse_payload_new(frame) do
    case frame do
      # A frame that may be missing some message bytes - new format: payload_id (32) + message_size (4) + message + metadata_size (2) + metadata
      <<id::binary-size(32), message_size::32, frame_bytes::binary>> ->
        payload_id_trimmed = String.trim_trailing(id)

        handle_incomplete_msg_buf(
          <<>>,
          payload_id_trimmed,
          frame_bytes,
          message_size
        )

      # Can't be parsed as a new frame.
      _ ->
        {{:incomplete_frame, frame}, <<>>}
    end
  end
end
