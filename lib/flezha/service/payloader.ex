defmodule Flezha.Service.Payloader do
  alias Flezha.Pipeline.Builder, as: PipelineBuilder

  def json_to_map(""), do: %{}

  def json_to_map(json) do
    case Jason.decode(json) do
      {:ok, map} -> map
      {:error, reason} -> raise reason
    end
  end

  def build_from_bin(id, virtual_channel, message) when is_binary(id) and is_binary(message) do
    # Ensure id is exactly 32 bytes.
    payload_id_padded = String.pad_trailing(id, 32)

    # Calculate message size in bytes (8 bytes integer).
    message_size = byte_size(message)
    message_size_binary = :binary.encode_unsigned(message_size, :big)

    # Ensure message_size_binary is exactly 8 bytes
    # If message_size_binary is less than 8 bytes, pad it to the left with zeros.
    message_size_padded =
      <<0::size(8 * (8 - byte_size(message_size_binary)))>> <> message_size_binary

    # Construct the payload.
    payload_id_padded <> <<virtual_channel>> <> message_size_padded <> message
  end

  # TODO: update the doc, describe what is going on.
  # Second 32 bytes are the id.
  # Next 8 bytes are the message size.
  # Message itself is {message_size} bytes long.
  # The rest of the payload.
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
        {payloads, buffer}
    end
  end

  def construct_pipeline!(input, type) do
    result =
      case type do
        :json -> PipelineBuilder.from_json(input)
        :map -> PipelineBuilder.from_map(input)
      end

    case result do
      {:ok, pipeline} -> pipeline
      {:error, reason} -> raise reason
    end
  end

  defp handle_incomplete_msg_buf(buf, payload_id, virtual_channel, frame, bytes_to_take) do
    frame_size = byte_size(frame)

    if frame_size >= bytes_to_take do
      <<message::binary-size(bytes_to_take), bytes_left::binary>> = frame

      {{:complete, {payload_id, virtual_channel, buf <> message}}, bytes_left}
    else
      <<message::binary-size(frame_size), _bytes_left::binary>> = frame
      bytes_to_take = bytes_to_take - frame_size

      {{:incomplete_msg, {payload_id, virtual_channel, buf <> message, bytes_to_take}}, <<>>}
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

      {:incomplete_msg, {payload_id, virtual_channel, message_buf, bytes_to_take}} ->
        handle_incomplete_msg_buf(message_buf, payload_id, virtual_channel, frame, bytes_to_take)

      any ->
        raise "Unexpected buffer state: #{inspect(any)}"
    end
  end

  defp parse_payload_new(frame) do
    case frame do
      # A frame that may be missing some message bytes.
      <<id::binary-size(32), virtual_channel::size(8), payload_size::64, frame_bytes::binary>> ->
        payload_id_trimmed = String.trim_trailing(id)

        handle_incomplete_msg_buf(
          <<>>,
          payload_id_trimmed,
          virtual_channel,
          frame_bytes,
          payload_size
        )

      # Can't be parsed as a new frame.
      _ ->
        {{:incomplete_frame, frame}, <<>>}
    end
  end
end
