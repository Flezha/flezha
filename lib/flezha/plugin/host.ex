defmodule Flezha.Plugin.Host do
  @moduledoc """
  Macro used by a module that exposes hooks (e.g.
  `Flezha.Operator.Overwatch`).

      use Flezha.Plugin.Host, extensions: [
        {:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO},
        {:on_interval, :timer,  Flezha.Plugin.Extension.Ctx.Tick},
        ...
      ]

  Each catalog entry is `{name :: atom, kind :: kind(), ctx_module :: module}`.
  Bit indices are assigned from declaration order and used by the
  active-mask fast path in `Flezha.Plugin.Extension.Table`.

  It is also where a source hands the host a record: `inject/4` is the one call
  every producer makes, and the mode says what the producer needs back.
  """

  alias Flezha.Operator.Calls

  @valid_kinds [:fold, :transform, :fanout, :request, :timer]

  @typedoc """
  What a producer needs back from the host.

  * `:confirming` waits for the journal ack, so a zero-credit park blocks the
    producer and the upstream is back-pressured rather than buffered.
  * `:verdict` waits and answers what the host decided, which a poller reads to
    know whether to advance its content hash.
  * `:fire_and_forget` does not wait. The host owns the payload from the cast.
  * `:request` is `:confirming` without the wait: it answers the request id
    whose reply (`:gen_server.check_response/2`) is the journal ack, for a
    producer that must keep taking records meanwhile.
  * `:call` sends one input and waits for the host's answer to it, the output
    a `response()` end of its graph produced.
  """
  @type inject_mode() :: :confirming | :verdict | :fire_and_forget | :request | :call

  @doc """
  Hand one record to the host operator. The ONE call a source makes: five
  spellings of "the payload is yours now" were five chances for a producer to
  pick the wrong one, and the mode is what actually differs between them.

  A `:verdict` answers `:ok` or `{:error, reason}`, and a host it could not reach
  is `{:error, {:unreachable, reason}}`. A `:call` takes the call's deadline in
  milliseconds as its last argument and answers `{:ok, payload, %{"channel" =>
  channel}}` or `{:error, %Flezha.Error{}}`: `call.timeout` at the deadline,
  `call.unavailable` for a host it could not reach. A `:request` answers its
  request id. The other two answer `:ok`, because there is nothing else to say.
  """
  @spec inject(GenServer.server(), binary(), map(), inject_mode(), timeout()) ::
          :ok | {:ok, binary(), map()} | {:error, term()} | :gen_server.request_id()
  def inject(overwatch, message, metadata, mode, timeout \\ :infinity)

  def inject(overwatch, message, metadata, :confirming, timeout) do
    _ = GenServer.call(overwatch, {:io_in, message, metadata}, timeout)
    :ok
  end

  def inject(overwatch, message, metadata, :verdict, timeout) do
    case GenServer.call(overwatch, {:ingest, message, metadata}, timeout) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  catch
    # The exit carries the call, and the call carries the payload, which a
    # verdict is logged and reported with and must never repeat.
    :exit, {reason, {GenServer, :call, _call}} -> {:error, {:unreachable, reason}}
    :exit, reason -> {:error, {:unreachable, reason}}
  end

  def inject(overwatch, message, metadata, :request, _timeout),
    do: :gen_server.send_request(overwatch, {:io_in, message, metadata})

  def inject(overwatch, message, metadata, :fire_and_forget, _timeout) do
    GenServer.cast(overwatch, {:io_in, message, metadata})
  end

  # A plugin calls the operator it is enabled on: its own input, so nothing
  # resolves or authorizes it again.
  def inject(overwatch, message, metadata, :call, deadline_ms),
    do: Calls.call(overwatch, message, metadata, deadline_ms, inspect(overwatch))

  @doc """
  Commit a value a provider's worker holds of its own onto the plugin's options,
  through its host: the live table a plugin restart prepares the plugin from, and
  the declared manifest a controller restart rebuilds the operator from. It does
  not wait. A worker with no host identity has nowhere to write, and writes
  nothing.
  """
  @spec persist_opt(pid() | nil, binary() | nil, atom(), term()) :: :ok
  def persist_opt(host, plugin_id, key, value) when is_pid(host) and is_binary(plugin_id),
    do: GenServer.cast(host, {:plugin_opt_persist, plugin_id, key, value})

  def persist_opt(_host, _plugin_id, _key, _value), do: :ok

  @doc """
  The extension table a host operator holds: shareable plugins on it are served
  by their shared copy on this controller (`Flezha.Plugin.Shared`). A partition
  group's member runs every plugin it holds itself, because each member asks
  upstream for its own share of the key range only.
  """
  @spec operator_table(map(), keyword()) :: Flezha.Plugin.Extension.Table.t()
  def operator_table(catalog, operator_opts) do
    shared =
      if Flezha.Partition.identity(operator_opts), do: nil, else: Flezha.Plugin.Shared

    Flezha.Plugin.Extension.Table.new(catalog, shared: shared)
  end

  defmacro __using__(opts) do
    extensions_ast = Keyword.fetch!(opts, :extensions)
    extensions = extensions_from_ast(extensions_ast, __CALLER__)
    catalog = build_catalog(extensions, __CALLER__)

    quote do
      @extensions unquote(Macro.escape(extensions))
      @extensions_catalog unquote(Macro.escape(catalog))

      @doc false
      def __extensions__, do: @extensions

      @doc false
      def __extensions_catalog__, do: @extensions_catalog
    end
  end

  defp extensions_from_ast(list, caller) when is_list(list) do
    Enum.map(list, fn
      {:{}, _, [name, kind, ctx_ast]} ->
        {name, kind, Macro.expand(ctx_ast, caller)}

      {name, kind, ctx_ast} ->
        {name, kind, Macro.expand(ctx_ast, caller)}

      other ->
        raise CompileError,
          file: caller.file,
          line: caller.line,
          description: "Invalid extension entry in AST: #{inspect(other)}"
    end)
  end

  @doc false
  def build_catalog(extensions, caller) when is_list(extensions) do
    extensions
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {entry, bit_index}, acc ->
      case entry do
        {name, kind, ctx_module}
        when is_atom(name) and kind in @valid_kinds and is_atom(ctx_module) ->
          refuse_duplicate(acc, name, caller)

          Map.put(acc, name, %{
            name: name,
            kind: kind,
            ctx_module: ctx_module,
            bit_index: bit_index
          })

        other ->
          raise CompileError,
            file: caller.file,
            line: caller.line,
            description:
              "Invalid extension catalog entry: #{inspect(other)}. " <>
                "Expected {name, kind, ctx_module} with kind in #{inspect(@valid_kinds)}."
      end
    end)
  end

  @doc "Returns the set of valid dispatch kinds."
  def valid_kinds, do: @valid_kinds

  defp refuse_duplicate(acc, name, caller) do
    if Map.has_key?(acc, name) do
      raise CompileError,
        file: caller.file,
        line: caller.line,
        description: "Duplicate extension name: #{inspect(name)}"
    end

    :ok
  end
end
