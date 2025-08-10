defmodule Flezha.Service.Payloader do
  alias Flezha.Processor.Builder, as: ProcessorBuilder

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

    # Calculate payload size: message_size (4) + message + metadata_size (2) + metadata
    payload_size = 4 + byte_size(message) + 2 + byte_size(metadata_binary)
    payload_size_binary = <<payload_size::32>>

    # Construct the payload: payload_id (32) + payload_size (4) + [message_size (4) + message + metadata_size (2) + metadata]
    payload_id_padded <>
      payload_size_binary <>
      message_size_binary <> message <> metadata_size_binary <> metadata_binary
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
  # - Next 4 bytes: payload_size (32-bit big-endian integer)
  # - Next {payload_size} bytes: [message_size (4) + message + metadata_size (2) + metadata]
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

  def construct_processor!(input, type) do
    result =
      case type do
        :json -> ProcessorBuilder.from_json(input)
        :map -> ProcessorBuilder.from_map(input)
      end

    case result do
      {:ok, processor} -> processor
      {:error, reason} -> raise reason
    end
  end

  defp handle_incomplete_header(buf, frame) do
    combined_buf = buf <> frame

    if byte_size(combined_buf) >= 36 do
      <<payload_id::binary-size(32), payload_size::32, bytes_left::binary>> = combined_buf
      payload_id_trimmed = String.trim_trailing(payload_id)

      handle_incomplete_payload(payload_id_trimmed, payload_size, <<>>, bytes_left)
    else
      {{:incomplete_header, combined_buf}, <<>>}
    end
  end

  defp handle_incomplete_payload(payload_id, payload_size, buf, frame) do
    combined_buf = buf <> frame

    if byte_size(combined_buf) >= payload_size do
      <<payload_data::binary-size(payload_size), bytes_left::binary>> = combined_buf

      case parse_payload_data(payload_id, payload_data) do
        {:ok, parsed_payload} ->
          {{:complete, parsed_payload}, bytes_left}

        {:error, _reason} ->
          # Skip this payload and continue with bytes_left
          {{:empty, <<>>}, bytes_left}
      end
    else
      {{:incomplete_payload, payload_id, payload_size, combined_buf}, <<>>}
    end
  end

  defp parse_payload_data(payload_id, payload_data) do
    try do
      <<message_size::32, rest::binary>> = payload_data
      <<message::binary-size(message_size), metadata_rest::binary>> = rest
      <<metadata_size::16, metadata_binary::binary-size(metadata_size)>> = metadata_rest

      metadata = deserialize_metadata(metadata_binary)
      {:ok, {payload_id, message, metadata}}
    rescue
      _ -> {:error, :invalid_payload}
    end
  end

  defp fill_buffer(buffer, <<>>), do: {buffer, <<>>}

  defp fill_buffer(buffer, frame) do
    case buffer do
      {:empty, _buf} ->
        # Start parsing from the beginning
        handle_incomplete_header(<<>>, frame)

      {:incomplete_header, buf} ->
        handle_incomplete_header(buf, frame)

      {:incomplete_payload, payload_id, payload_size, buf} ->
        handle_incomplete_payload(payload_id, payload_size, buf, frame)

      any ->
        raise "Unexpected buffer state: #{inspect(any)}"
    end
  end
end
