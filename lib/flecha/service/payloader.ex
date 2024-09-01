defmodule Flecha.Service.Payloader do
  def build_payload(computation_id, message)
      when is_binary(computation_id) and is_binary(message) do
    # Ensure computation_id is exactly 32 bytes.
    computation_id_padded = String.pad_trailing(computation_id, 32)

    # Calculate message size in bytes (8 bytes integer).
    message_size = byte_size(message)
    message_size_binary = :binary.encode_unsigned(message_size, :big)

    # Ensure message_size_binary is exactly 8 bytes
    # If message_size_binary is less than 8 bytes, pad it to the left with zeros.
    message_size_padded =
      <<0::size(8 * (8 - byte_size(message_size_binary)))>> <> message_size_binary

    # Construct the payload.
    message_size_padded <> computation_id_padded <> message
  end

  def parse_payload(payload) when is_binary(payload) do
    parse_messages(payload, [])
  end

  defp parse_messages(<<>>, messages), do: Enum.reverse(messages)

  defp parse_messages(payload, messages) do
    case parse_single_message(payload) do
      {:ok, {computation_id, message}, rest} ->
        parse_messages(rest, [{computation_id, message} | messages])

      :error ->
        # Handle the case where parsing fails or there's not enough data for another message
        Enum.reverse(messages)
    end
  end

  defp parse_single_message(payload) do
    # Extract the first 8 bytes for the message size
    case <<message_size::64, _rest::binary>> = payload do
      <<_::size(64), rest::binary>> ->
        # Extract the next 32 bytes for the computation_id
        case <<computation_id::binary-size(32), _message::binary>> = rest do
          <<_::binary-size(32), message::binary>> ->
            # Extract the message based on the message size
            case <<message_content::binary-size(message_size), _rest_of_payload::binary>> = message do
              <<_::binary-size(message_size), rest_of_payload::binary>> ->
                computation_id_trimmed = String.trim_trailing(computation_id)
                {:ok, {computation_id_trimmed, message_content}, rest_of_payload}
            end

          _ -> :error
        end

      _ -> :error
    end
  end
end
