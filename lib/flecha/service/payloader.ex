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
    message_size_padded <> payload_id_padded <> message
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
    # Extract the first 8 bytes for the message size
    case <<message_size::64, _rest::binary>> = payload do
      <<_::size(64), rest::binary>> ->
        # Extract the next 32 bytes for the id
        case <<id::binary-size(32), _message::binary>> = rest do
          <<_::binary-size(32), message::binary>> ->
            case message do
              <<message_content::binary-size(message_size), rest_of_payload::binary>> ->
                case <<message_content::binary-size(message_size), rest_of_payload::binary>> do
                  <<_::binary-size(message_size), rest_of_payload::binary>> ->
                    payload_id_trimmed = String.trim_trailing(id)
                    {:ok, {payload_id_trimmed, message_content}, rest_of_payload}

                  _ ->
                    :error
                end

              _ ->
                :error
            end

          _ ->
            :error
        end

      _ ->
        :error
    end
  end
end
