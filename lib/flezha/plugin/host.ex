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

  @valid_kinds [:fold, :transform, :fanout, :request, :timer]

  @typedoc """
  What a producer needs back from the host.

  * `:confirming` waits for the journal ack, so a zero-credit park blocks the
    producer and the upstream is back-pressured rather than buffered.
  * `:verdict` waits and answers what the host decided, which a poller reads to
    know whether to advance its content hash.
  * `:fire_and_forget` does not wait. The host owns the payload from the cast.
  """
  @type inject_mode() :: :confirming | :verdict | :fire_and_forget

  @doc """
  Hand one record to the host operator. The ONE call a source makes: five
  spellings of "the payload is yours now" were five chances for a producer to
  pick the wrong one, and the mode is what actually differs between them.

  A `:verdict` answers `:ok` or `{:error, reason}`, and a host it could not reach
  is `{:error, {:unreachable, reason}}`; the other two answer `:ok`, because there
  is nothing else to say.
  """
  @spec inject(GenServer.server(), binary(), map(), inject_mode(), timeout()) ::
          :ok | {:error, term()}
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

  def inject(overwatch, message, metadata, :fire_and_forget, _timeout) do
    GenServer.cast(overwatch, {:io_in, message, metadata})
  end

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
