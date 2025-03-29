defmodule Aflecha.Service.Payloader do
  alias Aflecha.Pipeline.Builder, as: PipelineBuilder

  # TODO: Remove or update when dealing with new CLI commands.
  def extract_io_settings(json_str) do
    settings_map = json_to_map(json_str)

    io_type =
      case Map.fetch(settings_map, "io_type") do
        {:ok, io_type} -> io_type
        :error -> raise "No io_type found in the input"
      end

    settings_map = Map.delete(settings_map, "io_type")

    io_struct =
      case io_type do
        "websocket" ->
          post_connect = Map.get(settings_map, "post_connect", %{})

          _payload =
            if Map.has_key?(post_connect, "payload") do
              payload = Map.get(post_connect, "payload")

              {
                Enum.at(payload, 0) |> String.to_existing_atom(),
                Enum.at(payload, 1) |> Jason.encode!()
              }
            else
              {}
            end

        # TODO: replace this legacy code.
        # %Aflecha.IO.Handler.Websocket{
        #   uri: Map.fetch!(settings_map, "uri"),
        #   post_connect: %{
        #     payload: payload
        #   }
        # }

        _ ->
          raise "Unknown io_type: #{io_type}"
      end

    io_struct
  end

  def json_to_map(json) do
    case Jason.decode(json) do
      {:ok, map} -> map
      {:error, reason} -> raise reason
    end
  end

  def build_from_bin(id, message)
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
