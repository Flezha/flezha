defmodule Flezha.Mcp do
  @moduledoc """
  The one MCP server behind every transport: stateless, tools only, one JSON
  answer per request, no session minted. `POST /mcp` of the HTTP API plugin and
  `flz mcp` over stdio both answer through `answer/4`.

  It serves revision `2026-07-28`, whose requests name their version in
  `_meta`, and over HTTP in headers too, and the handshake revision
  `2025-11-25` for a client that opens with `initialize`. The tools are the
  knowledge tools (`Flezha.Mcp.Knowledge`), which every caller sees, and one
  tool per catalogue entry the caller can reach, named, described and given an
  input schema from its declaration; a call is the door's, through `door`.
  """

  alias Flezha.Error
  alias Flezha.Mcp.Knowledge
  alias Flezha.Service.RelayApi

  @modern "2026-07-28"
  @legacy "2025-11-25"
  @supported [@modern, @legacy]

  @version_key "io.modelcontextprotocol/protocolVersion"
  @capabilities_key "io.modelcontextprotocol/clientCapabilities"

  @typedoc """
  A request header's value by its lowercase name, `nil` when it is absent or
  repeated; `nil` in its place is a transport that carries no headers.
  """
  @type headers :: (String.t() -> String.t() | nil) | nil

  @typedoc "The catalogue entries the caller may see, asked for when a list is."
  @type catalogue :: (-> [Flezha.Service.entry()])

  @type door :: (String.t(), [term()] -> {:ok, String.t()} | {:error, Error.t()})

  @typedoc """
  The Streamable HTTP status, the JSON-RPC message (`nil` for none) and the
  operation a tool call named. A transport without statuses reads the message
  alone.
  """
  @type answer :: {100..599, map() | nil, String.t() | nil}

  @doc """
  The answer to one JSON-RPC message `body`. `namespace` is the one the
  caller's token is bound to, whose relays' described channels are its relay
  tools; `""` is the default namespace.
  """
  @spec answer(binary(), headers(), catalogue(), door(), String.t()) :: answer()
  def answer(body, headers, catalogue, door, namespace \\ "") do
    door = relay_door(door, namespace)

    case Jason.decode(body) do
      {:ok, %{"jsonrpc" => "2.0", "method" => method} = message} when is_binary(method) ->
        message(message, headers, catalogue, door)

      {:ok, _not_a_request} ->
        reply(400, error(nil, -32_600, "Invalid Request"))

      {:error, _reason} ->
        reply(400, error(nil, -32_700, "Parse error"))
    end
  end

  defp message(%{"id" => id} = message, headers, catalogue, door)
       when is_binary(id) or is_integer(id) do
    params = Map.get(message, "params", %{})

    with :ok <- params_object(params, id),
         {:ok, era} <- era(headers, message, params) do
      request(era, id, message["method"], params, catalogue, door)
    else
      {:refused, status, answer} -> reply(status, answer)
    end
  end

  defp message(message, _headers, _catalogue, _door) do
    if Map.has_key?(message, "id"),
      do: reply(400, error(nil, -32_600, "Invalid Request")),
      else: {202, nil, nil}
  end

  defp params_object(params, _id) when is_map(params), do: :ok
  defp params_object(_params, id), do: {:refused, 400, error(id, -32_602, "Invalid params")}

  # ---------- the revision a request speaks ----------

  # A request naming its revision in `_meta` speaks `2026-07-28`, whose headers
  # must repeat its body. One that does not, or that opens with `initialize`,
  # is a client of the handshake.
  defp era(headers, %{"id" => id, "method" => method} = message, params) do
    case Map.get(params, "_meta", %{}) do
      meta when not is_map(meta) -> {:refused, 400, error(id, -32_602, "Invalid params")}
      _meta when method == "initialize" -> legacy(headers, id)
      %{@version_key => @modern} -> modern(headers, message, params)
      %{@version_key => requested} -> {:refused, 400, unsupported(id, requested)}
      _meta -> legacy(headers, id)
    end
  end

  # A legacy request may name the legacy revision in its header; one naming
  # the pinned revision lacks the `_meta` that revision requires.
  defp legacy(headers, id) do
    case header(headers, "mcp-protocol-version") do
      nil ->
        {:ok, :legacy}

      @legacy ->
        {:ok, :legacy}

      @modern ->
        {:refused, 400, error(id, -32_602, "Invalid params: _meta names no protocolVersion")}

      requested ->
        {:refused, 400, unsupported(id, requested)}
    end
  end

  defp modern(headers, %{"id" => id} = message, params) do
    if is_map(get_in(params, ["_meta", @capabilities_key])),
      do: modern_headers(headers, message, params),
      else:
        {:refused, 400, error(id, -32_602, "Invalid params: _meta names no clientCapabilities")}
  end

  defp modern_headers(nil, _message, _params), do: {:ok, :modern}

  defp modern_headers(headers, %{"id" => id, "method" => method}, params) do
    cond do
      headers.("mcp-protocol-version") != @modern ->
        mismatch(id, "MCP-Protocol-Version")

      headers.("mcp-method") != method ->
        mismatch(id, "Mcp-Method")

      method == "tools/call" and decoded(headers.("mcp-name")) != params["name"] ->
        mismatch(id, "Mcp-Name")

      true ->
        {:ok, :modern}
    end
  end

  defp header(nil, _name), do: nil
  defp header(headers, name), do: headers.(name)

  # A header value outside plain ASCII arrives as `=?base64?<value>?=`.
  defp decoded("=?base64?" <> rest) do
    with true <- String.ends_with?(rest, "?="),
         {:ok, value} <- rest |> String.trim_trailing("?=") |> Base.decode64() do
      value
    else
      _ -> nil
    end
  end

  defp decoded(value), do: value

  defp mismatch(id, header),
    do: {:refused, 400, error(id, -32_020, "Header mismatch: #{header} is missing or differs")}

  # The requested revision is echoed only when it is text: other bytes could
  # not be encoded as JSON.
  defp unsupported(id, requested) do
    data = %{supported: @supported}

    data =
      if is_binary(requested) and String.valid?(requested),
        do: Map.put(data, :requested, requested),
        else: data

    error(id, -32_022, "Unsupported protocol version", data)
  end

  # ---------- methods ----------

  defp request(_era, id, "server/discover", _params, _catalogue, _door) do
    reply(
      200,
      result(id, %{
        supportedVersions: @supported,
        capabilities: capabilities(),
        instructions: Knowledge.rules(),
        ttlMs: 3_600_000,
        cacheScope: "public"
      })
    )
  end

  defp request(_era, id, "initialize", _params, _catalogue, _door) do
    reply(
      200,
      result(id, %{
        protocolVersion: @legacy,
        capabilities: capabilities(),
        serverInfo: server_info(),
        instructions: Knowledge.rules()
      })
    )
  end

  defp request(_era, id, "ping", _params, _catalogue, _door),
    do: reply(200, result(id, %{}))

  defp request(_era, id, "tools/list", _params, catalogue, door) do
    # The list follows the token's role, so a client keeps it per token, and
    # a role changed since is seen within the minute.
    entries = catalogue.()
    tools = Enum.map(Knowledge.entries() ++ entries, &tool/1) ++ relay_tools(entries, door)
    reply(200, result(id, %{tools: tools, ttlMs: 60_000, cacheScope: "private"}))
  end

  defp request(_era, id, "tools/call", %{"name" => name} = params, _catalogue, door)
       when is_binary(name),
       do: tool_call(id, name, Map.get(params, "arguments", %{}), door)

  defp request(_era, id, "tools/call", _params, _catalogue, _door),
    do: reply(200, error(id, -32_602, "Invalid params"))

  defp request(era, id, _method, _params, _catalogue, _door) do
    reply(if(era == :modern, do: 404, else: 200), error(id, -32_601, "Method not found"))
  end

  defp capabilities, do: %{tools: %{listChanged: false}}

  defp server_info,
    do: %{name: "flezha", version: to_string(Application.spec(:flezha, :vsn) || "")}

  # ---------- tools ----------

  @doc """
  The tool name of the entry named `name`: in snake case, with `.`
  replaced by `_`, and only the characters a model's tool name may hold.
  """
  @spec tool_name(String.t()) :: String.t()
  def tool_name(name) do
    [module, function] = String.split(name, ".", parts: 2)
    String.replace(Macro.underscore(module) <> "_" <> function, ~r/[^a-z0-9_]/, "")
  end

  @max_tool_name 64

  @doc """
  The tool name of each `{factory, channel}` in order: `{:ok, name}`, or
  `{:taken, name}` when a catalogue or knowledge tool, or an earlier pair,
  already holds it. A name is both parts in snake case joined by `_`, with
  every character a model's tool name cannot hold made `_`; past 64 bytes it
  is cut and ends in a hash of the whole, so it stays the same name.
  """
  @spec relay_tool_names([{String.t(), String.t()}]) :: %{
          {String.t(), String.t()} => {:ok | :taken, String.t()}
        }
  def relay_tool_names(pairs) do
    held = MapSet.new(Knowledge.entries() ++ Flezha.Service.catalogue(), &tool_name(&1.name))

    pairs
    |> Enum.map_reduce(held, fn {factory, channel} = pair, held ->
      name = capped(tool_segment(factory) <> "_" <> tool_segment(channel))

      if MapSet.member?(held, name),
        do: {{pair, {:taken, name}}, held},
        else: {{pair, {:ok, name}}, MapSet.put(held, name)}
    end)
    |> elem(0)
    |> Map.new()
  end

  defp tool_segment(text),
    do: text |> Macro.underscore() |> String.replace(~r/[^a-zA-Z0-9_-]/u, "_")

  defp capped(name) when byte_size(name) <= @max_tool_name, do: name

  defp capped(name) do
    hash = :sha256 |> :crypto.hash(name) |> Base.encode16(case: :lower) |> binary_part(0, 8)
    binary_part(name, 0, @max_tool_name - 9) <> "_" <> hash
  end

  @doc "The MCP tool `entry` is."
  @spec tool(Flezha.Service.entry()) :: map()
  def tool(entry) do
    read? = not entry.audited

    %{
      name: tool_name(entry.name),
      description: description(entry, read?),
      inputSchema: input_schema(entry.args),
      annotations: %{
        readOnlyHint: read?,
        destructiveHint:
          String.ends_with?(Atom.to_string(entry.action), [".drop", ".delete", ".stop"]),
        openWorldHint: false
      }
    }
  end

  defp description(entry, true), do: entry.doc || ""

  defp description(entry, false),
    do: String.trim("#{entry.doc} It needs a role that grants `#{entry.action}`.")

  @doc "The JSON Schema of an entry's arguments: one property per row, in order."
  @spec input_schema([Flezha.Service.arg()]) :: map()
  def input_schema(args) do
    required = for {name, row} <- args, row[:required], do: Atom.to_string(name)

    %{
      type: "object",
      properties: Map.new(args, fn {name, row} -> {Atom.to_string(name), property(row)} end),
      additionalProperties: false
    }
    |> then(&if required == [], do: &1, else: Map.put(&1, :required, required))
  end

  defp property(row) do
    row
    |> Keyword.get(:type)
    |> schema_type(row)
    |> Map.put(:description, row[:doc] || "")
  end

  defp schema_type(:string, _row), do: %{type: "string"}
  defp schema_type(:bool, _row), do: %{type: "boolean"}
  defp schema_type(type, _row) when type in [:keyword, :map], do: %{type: "object"}
  defp schema_type(:list, _row), do: %{type: "array"}

  defp schema_type(:int, row),
    do: Map.merge(%{type: "integer"}, Map.new(Keyword.take(row, [:min, :max]), &bound/1))

  defp schema_type(type, row) when type in [:atom, :enum] do
    case row[:values] do
      [_ | _] = values -> %{type: "string", enum: Enum.map(values, &to_string/1)}
      _none -> %{type: "string"}
    end
  end

  defp schema_type(_any, _row), do: %{}

  defp bound({:min, value}), do: {:minimum, value}
  defp bound({:max, value}), do: {:maximum, value}

  # A name no tool has is a protocol error; an operation the caller cannot
  # reach is one too, once the door has said so. A knowledge entry is answered
  # by its own door, an operation by the caller's.
  defp tool_call(id, name, arguments, door) do
    knowledge = Knowledge.entries()

    case {Enum.find(knowledge ++ Flezha.Service.catalogue(), &(tool_name(&1.name) == name)),
          arguments} do
      {nil, arguments} when is_map(arguments) ->
        relay_tool_call(id, name, arguments, door)

      {nil, _arguments} ->
        reply(200, unknown_tool(id, name))

      {entry, arguments} when is_map(arguments) ->
        door = if entry in knowledge, do: &Knowledge.call/2, else: door
        {200, call(entry, arguments, id, door), entry.name}

      {_entry, _not_an_object} ->
        reply(200, error(id, -32_602, "Invalid params"))
    end
  end

  defp unknown_tool(id, name), do: error(id, -32_602, "Unknown tool: #{name}")

  defp call(entry, arguments, id, door) do
    case with(
           {:ok, positional} <- positional(entry.args, arguments),
           do: door.(entry.name, positional)
         ) do
      {:error, %Error{code: "control.unknown_function"}} ->
        unknown_tool(id, tool_name(entry.name))

      answer ->
        tool_result(id, answer)
    end
  end

  defp tool_result(id, {:text, body}),
    do: result(id, %{content: [text(body)], isError: false})

  defp tool_result(id, {:ok, json}) do
    value = Jason.decode!(json)
    structured = if is_map(value), do: value, else: %{"result" => value}
    result(id, %{content: [text(json)], structuredContent: structured, isError: false})
  end

  defp tool_result(id, {:error, %Error{} = error}) do
    document = Error.render(error, :json)

    result(id, %{
      content: [text(document)],
      structuredContent: Jason.decode!(document),
      isError: true
    })
  end

  # The named arguments, in the rows' order; one left out is `nil`, which the
  # door reads as its default or refuses as missing.
  defp positional(args, arguments) do
    names = Enum.map(args, fn {name, _row} -> Atom.to_string(name) end)

    case Map.keys(arguments) -- names do
      [] -> {:ok, Enum.map(names, &Map.get(arguments, &1))}
      [unknown | _] -> {:error, Error.new("control.unknown_parameter", parameter: unknown)}
    end
  end

  defp text(json), do: %{type: "text", text: json}

  # ---------- relay tools ----------

  # The door, with the namespace the relay tools act in bound to it.
  defp relay_door(door, namespace),
    do: fn
      :relay_api, [] -> door.("Relay.api", [namespace])
      :call_channel, args -> door.("Relay.call_channel", [namespace | args])
      name, args -> door.(name, args)
    end

  # Listed only to a caller whose role may call relays: the catalogue it
  # reaches holds `Relay.call_channel` exactly then.
  defp relay_tools(entries, door) do
    if Enum.any?(entries, &(&1.name == "Relay.call_channel")),
      do: listed_relay_tools(door),
      else: []
  end

  defp listed_relay_tools(door), do: Enum.map(relay_entries(door), &relay_tool/1)

  # Every described entry channel of the namespace's factories that holds its
  # tool name, with its factory: one tool per factory and channel, however
  # many relays run it.
  defp relay_entries(door) do
    case door.(:relay_api, []) do
      {:ok, json} ->
        described =
          for factory <- Jason.decode!(json),
              entry <- factory["channels"],
              entry["description"] != nil,
              do: {factory, entry}

        names = relay_tool_names(for {f, e} <- described, do: {f["factory"], e["channel"]})

        for {factory, entry} <- described,
            {:ok, name} <- [names[{factory["factory"], entry["channel"]}]],
            do: {factory, Map.put(entry, "tool", name)}

      {:error, _refused} ->
        []
    end
  end

  defp relay_tool({factory, entry}) do
    tool = %{
      name: entry["tool"],
      description:
        Enum.join(Enum.reject([factory["description"], entry["description"]], &is_nil/1), ": "),
      inputSchema: relay_input_schema(entry),
      annotations: %{readOnlyHint: false, destructiveHint: false, openWorldHint: false}
    }

    case entry["answer_schema"] do
      nil -> tool
      %{"type" => "object"} = schema -> Map.put(tool, :outputSchema, schema)
      schema -> Map.put(tool, :outputSchema, object(%{"result" => schema}, ["result"]))
    end
  end

  # The arguments every relay tool takes beside the channel's input.
  @relay_arguments %{
    "relay" => %{"type" => "string", "description" => "The relay to call."},
    "key" => %{
      "type" => "string",
      "description" =>
        "The key whose partition answers; needed when `relay` names a partition group."
    }
  }

  # An object schema with neither a `relay` nor a `key` property takes them
  # beside its own properties; any other input rides in one argument of its
  # own.
  defp relay_input_schema(entry) do
    case value_argument(entry) do
      :merged ->
        schema = entry["schema"]
        properties = Map.merge(Map.get(schema, "properties", %{}), @relay_arguments)

        Map.merge(schema, %{
          "properties" => properties,
          "required" => ["relay" | Map.get(schema, "required", [])]
        })

      {name, schema} ->
        object(Map.put(@relay_arguments, name, schema), ["relay", name])
    end
  end

  defp value_argument(%{"schema" => %{"type" => "object"} = schema}) do
    properties = Map.get(schema, "properties", %{})

    if Enum.any?(Map.keys(@relay_arguments), &Map.has_key?(properties, &1)),
      do: {"input", schema},
      else: :merged
  end

  defp value_argument(%{"codec" => "json"} = entry), do: {"input", entry["schema"] || %{}}
  defp value_argument(%{"codec" => "text"}), do: {"text", %{"type" => "string"}}

  defp value_argument(_binary),
    do: {"base64", %{"type" => "string", "contentEncoding" => "base64"}}

  defp object(properties, required),
    do: %{"type" => "object", "properties" => properties, "required" => required}

  defp relay_tool_call(id, name, arguments, door) do
    case Enum.find(relay_entries(door), fn {_factory, entry} -> entry["tool"] == name end) do
      nil ->
        reply(200, unknown_tool(id, name))

      {_factory, entry} ->
        answer = door.(:call_channel, call_channel_args(entry, arguments))
        {200, relay_tool_result(id, answer), "Relay.call_channel"}
    end
  end

  # A relay tool's arguments read back the way `relay_input_schema/1` laid
  # them out: the relay, the channel's value and a text key as the call's.
  defp call_channel_args(entry, arguments) do
    value =
      case value_argument(entry) do
        :merged -> Map.drop(arguments, Map.keys(@relay_arguments))
        {argument, _schema} -> Map.get(arguments, argument)
      end

    opts = if is_binary(arguments["key"]), do: %{"key" => arguments["key"]}, else: %{}
    [arguments["relay"], entry["channel"], value, opts]
  end

  # A JSON answer is structured content, text is text, bytes an embedded
  # blob, and a send `accepted`.
  defp relay_tool_result(id, {:ok, json}) do
    case RelayApi.answer(json) do
      :accepted ->
        result(id, %{content: [text("accepted")], isError: false})

      {:json, value} ->
        structured = if is_map(value), do: value, else: %{"result" => value}

        result(id, %{
          content: [text(Jason.encode!(value))],
          structuredContent: structured,
          isError: false
        })

      {:text, value} ->
        result(id, %{content: [text(value)], isError: false})

      {:bytes, bytes} ->
        resource = %{
          uri: "flezha:answer",
          mimeType: "application/octet-stream",
          blob: Base.encode64(bytes)
        }

        result(id, %{content: [%{type: "resource", resource: resource}], isError: false})
    end
  end

  defp relay_tool_result(id, refused), do: tool_result(id, refused)

  # ---------- JSON-RPC ----------

  defp result(id, result) do
    meta = %{"io.modelcontextprotocol/serverInfo" => server_info()}
    %{jsonrpc: "2.0", id: id, result: Map.merge(result, %{resultType: "complete", _meta: meta})}
  end

  defp error(id, code, message, data \\ nil) do
    error =
      if data,
        do: %{code: code, message: message, data: data},
        else: %{code: code, message: message}

    # JSON-RPC answers a message whose id could not be read under a null id.
    %{jsonrpc: "2.0", id: id, error: error}
  end

  defp reply(status, answer), do: {status, answer, nil}
end
