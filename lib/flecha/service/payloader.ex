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
    # Extract the first 8 bytes for the message size
    <<message_size::64, rest::binary>> = payload

    # Extract the next 32 bytes for the computation_id
    <<computation_id::binary-size(32), message::binary>> = rest

    # Trim any trailing padding from the computation_id
    computation_id_trimmed = String.trim_trailing(computation_id)

    # Extract the message based on the message size
    <<message::binary-size(message_size)>> = message

    {computation_id_trimmed, message}
  end
end
