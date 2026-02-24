defmodule Flezha.Service.Payloader do
  @moduledoc """
  Binary frame building and utility functions.

  Frame parsing (parse_frame) has been moved to the Rust NIF (FrameBuffer).
  The NIF delivers complete `{payload_id, message, metadata}` tuples to Elixir.

  This module retains frame building for Elixir-side frame construction
  (e.g., control commands) and JSON utility functions.
  """

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
end
