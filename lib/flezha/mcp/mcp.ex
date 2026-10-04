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

  @doc "The answer to one JSON-RPC message `body`."
  @spec answer(binary(), headers(), catalogue(), door()) :: answer()
  def answer(body, headers, catalogue, door) do
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

  defp request(_era, id, "tools/list", _params, catalogue, _door) do
    # The list follows the token's role, so a client keeps it per token, and
    # a role changed since is seen within the minute.
    tools = Enum.map(Knowledge.entries() ++ catalogue.(), &tool/1)
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
      {:error, %Error{code: "control.unknown_function"}} -> unknown_tool(id, tool_name(entry.name))
      answer -> tool_result(id, answer)
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
