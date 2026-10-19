defmodule Flezha.Service.RelayApi do
  @moduledoc """
  A relay's API: its factory's entry channels as the runner hosting the relay
  announces them, and the encoding a call of one channel needs.
  `Flezha.Service.Relay.api/3` and `Flezha.Service.Relay.call_channel/6` publish
  it, so the CLI, HTTP and MCP serve it with no code of their own.

  The API is read at the call and bind boundaries, where a channel it does
  not name is refused; nothing on the delivery path consults it.
  """

  alias Flezha.Relay.PartitionGroup
  alias Flezha.Runner.FleetManager.Cluster
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager

  @binary ["bytes", "msgpack", "custom"]
  @no_graph %{graph: nil, disagree: false}

  @typedoc "One entry channel of a factory's API, as `Relay.api` answers it."
  @type entry :: %{
          channel: String.t(),
          codec: String.t(),
          schema: map() | nil,
          description: String.t() | nil,
          answer_codec: String.t() | nil,
          answer_schema: map() | nil
        }

  @doc """
  The factories announced in `namespace` across the cluster, each with its
  description, its API entries and whether its runners announce different
  ones. Where nodes disagree, the latest announcement wins.
  """
  @spec list(String.t()) :: [map()]
  def list(namespace) do
    namespace
    |> Cluster.global_factories()
    |> Enum.group_by(& &1.id)
    |> Enum.map(fn {factory, rows} -> api_of(factory, rows) end)
    |> Enum.sort_by(& &1.factory)
  end

  # One factory's API from the rows its nodes answer, the latest winning.
  defp api_of(factory, rows) do
    latest = Enum.max_by(rows, &(&1.api_at || 0))

    %{
      factory: factory,
      description: latest.description,
      channels: latest.api,
      disagree: Enum.any?(rows, & &1.api_disagree) or length(Enum.uniq_by(rows, & &1.api)) > 1
    }
  end

  @doc """
  The factory row of the relay `relay_id`, read from its own registry entry
  as the runner hosting it announced it; a partition group's is its first
  member's. A relay with no runner reads the factory's latest announcement,
  and a factory no runner announces has no channels. A relay not declared yet
  reads `factory_id`'s, when one is given.
  """
  @spec factory_of(String.t(), String.t(), String.t() | nil) :: {:ok, map()} | {:error, term()}
  def factory_of(namespace, relay_id, factory_id \\ nil) do
    with {:ok, relay, host} <- declared(namespace, relay_id, factory_id) do
      case RelayManager.factory_row(namespace, relay.factory_id, host) do
        none when not is_map(none) ->
          {:ok, %{factory: relay.factory_id, description: nil, channels: [], disagree: false}}

        row ->
          {:ok, api_of(row.id, [row])}
      end
    end
  end

  @doc """
  The graph of the relay `relay_id`'s factory as `factory_of/2` reads its API,
  `%{factory:, graph:, disagree:}` with `graph: nil` when none was announced.
  """
  @spec graph_of(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def graph_of(namespace, relay_id) do
    with {:ok, %{factory_id: factory}, host} <- declared(namespace, relay_id),
         %{graph: _} = read <- RelayManager.factory_graph(namespace, factory, host) || @no_graph,
         do: {:ok, Map.put(read, :factory, factory)}
  end

  defp declared(namespace, relay_id, factory_id \\ nil) do
    case HubManager.relay_spec(namespace, relay_id) do
      {:ok, _relay, _host} = declared ->
        declared

      {:error, _not_one_relay} ->
        case PartitionGroup.members(namespace, relay_id) do
          [member | _] -> HubManager.relay_spec(namespace, member.id)
          [] when is_binary(factory_id) -> {:ok, %{factory_id: factory_id}, %{runner: nil}}
          [] -> {:error, {:relay_not_found, namespace, relay_id}}
        end
    end
  end

  @doc """
  The entry of `channel` in `factory`'s API; `""` names its first. A factory
  whose runners announced no API is `api.not_advertised`, and a channel it
  lacks `api.channel_unknown` naming the ones it has.
  """
  @spec entry(map(), String.t(), String.t(), String.t()) :: {:ok, entry()} | {:error, term()}
  def entry(%{channels: []} = factory, namespace, _relay_id, _channel),
    do:
      {:error,
       Flezha.Error.new("api.not_advertised", namespace: namespace, factory: factory.factory)}

  def entry(%{channels: [first | _]}, _namespace, _relay_id, ""), do: {:ok, first}

  def entry(factory, _namespace, relay_id, channel) do
    case Enum.find(factory.channels, &(&1.channel == channel)) do
      nil ->
        {:error,
         Flezha.Error.new("api.channel_unknown",
           relay: relay_id,
           channel: channel,
           channels: Enum.map_join(factory.channels, ", ", & &1.channel)
         )}

      entry ->
        {:ok, entry}
    end
  end

  @doc """
  Refuse `channel` as an entry channel of the relay `relay_id` the way
  `entry/4` does, when the relay's runner announced channels and it is none
  of them; a relay not declared yet is checked against `factory_id`'s, as
  `factory_of/3` reads it. A `nil` channel, an undeclared relay with no
  `factory_id` and one whose runner announced no API are not checked.
  """
  @spec refuse_unknown_entry(String.t(), String.t(), String.t() | nil, String.t() | nil) ::
          :ok | {:error, Flezha.Error.t()}
  def refuse_unknown_entry(namespace, relay_id, channel, factory_id \\ nil)
  def refuse_unknown_entry(_namespace, _relay_id, nil, _factory_id), do: :ok

  def refuse_unknown_entry(namespace, relay_id, channel, factory_id) do
    with {:ok, %{channels: [_ | _]} = factory} <- factory_of(namespace, relay_id, factory_id),
         {:error, _} = unknown <- entry(factory, namespace, relay_id, channel),
         do: unknown,
         else: (_announced_or_unchecked -> :ok)
  end

  @doc """
  The payload bytes of `value` for `entry`. With `payload` `"text"` or
  `"base64"`, `value` is the payload itself, as text or as the base64 of its
  bytes, and is sent unchanged; otherwise it is encoded by the entry's format:
  any JSON value for `json`, a string for `text`, a base64 string for the
  binary formats.
  """
  @spec encode(entry(), term(), String.t() | nil) :: {:ok, binary()} | {:error, Flezha.Error.t()}
  def encode(entry, value, payload) do
    with {:ok, bytes} <- payload(entry, value, payload), do: within_schema(entry, bytes)
  end

  defp payload(entry, value, "text") when is_binary(value), do: text(entry, value)
  defp payload(entry, value, "text"), do: invalid(entry, "a string", value)
  defp payload(entry, value, "base64"), do: base64(entry, value)
  defp payload(%{codec: "text"} = entry, value, nil), do: text(entry, value)

  defp payload(%{codec: codec} = entry, value, nil) when codec in @binary,
    do: base64(entry, value)

  defp payload(_entry, value, nil), do: {:ok, Jason.encode!(value)}

  defp payload(entry, _value, payload),
    do: invalid(entry, "a value with payload text or base64", payload)

  # An object schema's `required` and `additionalProperties: false` are held
  # at the call, whatever surface it came from. Bytes that are not a JSON
  # object pass, and fail at the relay's decode as any input does.
  defp within_schema(%{codec: "json", schema: %{"type" => "object"} = schema} = entry, bytes) do
    with {:ok, %{} = value} <- Jason.decode(bytes),
         {:missing, []} <- {:missing, Map.get(schema, "required", []) -- Map.keys(value)},
         {:undeclared, []} <- {:undeclared, undeclared(schema, value)} do
      {:ok, bytes}
    else
      {:missing, names} ->
        refuse_fields(
          entry,
          "an object with #{Enum.join(schema["required"], ", ")}",
          "missing",
          names
        )

      {:undeclared, names} ->
        declared = Enum.join(Map.keys(schema["properties"] || %{}), ", ")
        refuse_fields(entry, "an object with only #{declared}", "undeclared", names)

      _not_an_object ->
        {:ok, bytes}
    end
  end

  defp within_schema(_entry, bytes), do: {:ok, bytes}

  defp undeclared(%{"additionalProperties" => false} = schema, value),
    do: Map.keys(value) -- Map.keys(Map.get(schema, "properties", %{}))

  defp undeclared(_schema, _value), do: []

  defp refuse_fields(entry, expected, what, names) do
    {:error,
     Flezha.Error.new("api.input_invalid",
       channel: entry.channel,
       expected: expected,
       reason: "#{what} #{Enum.join(names, ", ")}"
     )}
  end

  defp text(_entry, value) when is_binary(value), do: {:ok, value}
  defp text(entry, value), do: invalid(entry, "a string", value)

  defp base64(entry, value) when is_binary(value) do
    case Base.decode64(value) do
      {:ok, bytes} -> {:ok, bytes}
      :error -> invalid(entry, "a base64 string", value)
    end
  end

  defp base64(entry, value), do: invalid(entry, "a base64 string", value)

  defp invalid(entry, expected, value) do
    {:error,
     Flezha.Error.new("api.input_invalid",
       channel: entry.channel,
       expected: expected,
       reason: "got #{inspect(value, limit: 5, printable_limit: 40)}"
     )}
  end

  @doc """
  An answer's bytes as `codec` reads them: a JSON value (or the text, when
  they are not JSON), a string, or base64 for the binary formats. Bytes that
  are not text are answered as base64 under `bytes`, whatever was declared.
  """
  @spec decode(String.t() | nil, binary()) :: %{answer: term(), answer_codec: String.t()}
  def decode(codec, bytes) when codec in [nil, "json"] do
    case Jason.decode(bytes) do
      {:ok, value} -> %{answer: value, answer_codec: "json"}
      {:error, _not_json} -> decode("text", bytes)
    end
  end

  def decode("text", bytes) do
    if String.valid?(bytes),
      do: %{answer: bytes, answer_codec: "text"},
      else: decode("bytes", bytes)
  end

  def decode(codec, bytes) do
    codec = if codec in @binary, do: codec, else: "bytes"
    %{answer: Base.encode64(bytes), answer_codec: codec}
  end
end
