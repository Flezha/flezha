defmodule Flecha.Service.Payloader do
  def build_payload(id, message)
      when is_binary(id) and is_binary(message) do
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
    payload_id_padded <> message_size_padded <> message
  end

  def parse_payload(payload) when is_binary(payload) do
    parse_messages(payload, [])
  end

  defp parse_messages(<<>>, messages), do: Enum.reverse(messages)

  defp parse_messages(payload, messages) do
    case parse_single_message(payload) do
      {:ok, {id, message}, rest} ->
        parse_messages(rest, [{id, message} | messages])

      :error ->
        # Handle the case where parsing fails or there's not enough data for another message
        Enum.reverse(messages)
    end
  end

  defp parse_single_message(payload) do
    # First 32 bytes are the id.
    # Next 8 bytes are the message size.
    # Message itself is {nessage_size} bytes long.
    # The rest of the payload.
    with <<id::binary-size(32), message_size::64, rest::binary>> <- payload,
         <<message::binary-size(message_size), payload_rest::binary>> <- rest do
      payload_id_trimmed = String.trim_trailing(id)
      {:ok, {payload_id_trimmed, message}, payload_rest}
    else
      _ -> :error
    end
  end
end
