defmodule Flezha.Plugin.Host do
  @moduledoc """
  Macro used by a module that exposes hooks (e.g.
  `Flezha.Relay.Overwatch`).

      use Flezha.Plugin.Host, extensions: [
        {:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO},
        {:on_interval, :timer,  Flezha.Plugin.Extension.Ctx.Tick},
        ...
      ]

  Each catalog entry is `{name :: atom, kind :: kind(), ctx_module :: module}`.
  Bit indices are assigned from declaration order and used by the
  active-mask fast path in `Flezha.Plugin.Extension.Table`.

  It is also where a source hands the host a record: `inject/4` is the one call
  every producer makes, and the mode says what the producer needs back;
  `inject_many/4` hands over a batch and waits once.
  """

  alias Flezha.Config
  alias Flezha.Relay.Calls

  @valid_kinds [:fold, :transform, :fanout, :request, :timer]

  @typedoc """
  What a producer needs back from the host.

  * `:confirming` waits for the journal ack, so a zero-credit park blocks the
    producer and the upstream is back-pressured rather than buffered.
  * `:verdict` waits and answers what the host decided, which a poller reads to
    know whether to advance its content hash.
  * `:fire_and_forget` does not wait. The host owns the payload from the cast.
  * `:call` sends one input and waits for the host's answer to it, the output
    a `response()` end of its graph produced.
  """
  @type inject_mode() :: :confirming | :verdict | :fire_and_forget | :call

  @doc """
  Hand one record to the host relay. The ONE call a source makes: five
  spellings of "the payload is yours now" were five chances for a producer to
  pick the wrong one, and the mode is what actually differs between them.

  A `:verdict` answers `:ok` or `{:error, reason}`, and a host it could not reach
  is `{:error, {:unreachable, reason}}`. A `:call` takes the call's deadline in
  milliseconds as its last argument and answers `{:ok, payload, %{"channel" =>
  channel}}` or `{:error, %Flezha.Error{}}`: `call.timeout` at the deadline,
  `call.unavailable` for a host it could not reach. The other two answer `:ok`,
  because there is nothing else to say.
  """
  @spec inject(GenServer.server(), binary(), map(), inject_mode(), timeout()) ::
          :ok | {:ok, binary(), map()} | {:error, term()}
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

  # A plugin calls the relay it is enabled on: its own input, so nothing
  # resolves or authorizes it again.
  def inject(overwatch, message, metadata, :call, deadline_ms),
    do: Calls.call(overwatch, message, metadata, deadline_ms, inspect(overwatch))

  # A raw `{:ingest, …}` reply as `inject/5` answers it.
  defp verdict({:ok, _}), do: :ok
  defp verdict(other), do: other

  @typedoc """
  A relay a feeding worker injects into: its Overwatch, the entry channel
  its binding names, and whether it is effectively-once, which makes its every
  record a confirming injection.
  """
  @type target() :: %{pid: pid() | nil, channel: binary() | nil, effectively_once: boolean()}

  @doc "`overwatch` as a target, for its binding's entry `channel`."
  @spec target(pid() | nil, binary() | nil, boolean()) :: target()
  def target(overwatch, channel, effectively_once?),
    do: %{pid: overwatch, channel: channel, effectively_once: effectively_once?}

  @doc """
  The relays a feeding worker started from `opts` injects into: the ones its
  shared copy names, or else the one host that started it. A worker holds them
  as its state and is told of a change by `retarget/2`, so none is looked up
  per record.
  """
  @spec targets(map()) :: [target()]
  def targets(%{targets: targets}), do: targets

  def targets(opts),
    do: [
      target(
        opts[:overwatcher_pid],
        opts[:entry_channel],
        Map.get(opts, :effectively_once) not in [nil, false]
      )
    ]

  @doc """
  The relays the shared copy `copy` serves now, which a feeding worker it
  started reads again on each connect: a `retarget/2` sent while the worker
  could take no message is lost.
  """
  @spec targets_of(pid()) :: {:ok, [target()]} | :error
  def targets_of(copy) do
    case Flezha.Plugin.Shared.subscribers(copy) do
      targets when is_list(targets) -> {:ok, targets}
      {:error, _unreachable} -> :error
    end
  end

  @doc "Tell a feeding `worker` the relays it injects into from now on."
  @spec retarget(pid(), [target()]) :: :ok
  def retarget(worker, targets) do
    send(worker, {:targets, targets})
    :ok
  end

  @doc """
  Hand a batch of records to the host relay. With `:verdict` up to
  `plugin_ingest_window` records are in flight at once, the next sent as each
  answer arrives, and the answer is one `inject/5` verdict per record, in the
  records' order, `{:error, {:unreachable, :timeout}}` for each one the relay
  had not answered by `timeout`: a source that confirms a fetched batch waits
  once, and the batch shares the relay's journal syncs. With
  `:fire_and_forget` every record is cast and the answer is `:ok`.
  """
  @spec inject_many(GenServer.server(), [{binary(), map()}], :verdict, timeout()) ::
          [:ok | {:error, term()}]
  @spec inject_many(GenServer.server(), [{binary(), map()}], :fire_and_forget, timeout()) :: :ok
  def inject_many(overwatch, records, :verdict, timeout) do
    overwatch
    |> ingest_window(records, timeout, Config.get(:plugin_ingest_window))
    |> Enum.map(&verdict/1)
  end

  def inject_many(overwatch, records, :fire_and_forget, _timeout),
    do:
      Enum.each(records, fn {message, md} -> inject(overwatch, message, md, :fire_and_forget) end)

  @doc """
  The `{:ingest, …}` request of every record, with at most `window` in flight,
  answering each record's raw reply in the records' order. The relay runs
  its ingest path once per record, in order; only the caller's round trips
  overlap. A host that went away answers `{:error, {:unreachable, reason}}`,
  and a record unanswered at `timeout` after the call began answers
  `{:error, {:unreachable, :timeout}}`, as `inject/5` does; the requests still
  open then are abandoned, so no late reply reaches the caller.
  """
  @spec ingest_window(GenServer.server(), [{binary(), map()}], timeout(), pos_integer()) ::
          [term()]
  def ingest_window(overwatch, records, timeout, window) do
    deadline =
      if timeout == :infinity,
        do: :infinity,
        else: {:abs, System.monotonic_time(:millisecond) + timeout}

    overwatch
    |> pipeline(Enum.with_index(records), deadline, window, :gen_server.reqids_new(), %{})
    |> Enum.sort()
    |> Enum.map(&elem(&1, 1))
  end

  # Fill the window, then take one reply per free slot. `answers` is keyed by
  # record index, so the order replies are handed out in cannot matter.
  defp pipeline(overwatch, pending, deadline, window, reqids, answers) do
    cond do
      pending != [] and :gen_server.reqids_size(reqids) < window ->
        [{{message, metadata}, index} | rest] = pending
        reqids = :gen_server.send_request(overwatch, {:ingest, message, metadata}, index, reqids)
        pipeline(overwatch, rest, deadline, window, reqids, answers)

      :gen_server.reqids_size(reqids) == 0 ->
        answers

      true ->
        case :gen_server.receive_response(reqids, deadline, true) do
          {{:reply, reply}, index, reqids} ->
            pipeline(overwatch, pending, deadline, window, reqids, Map.put(answers, index, reply))

          {{:error, {reason, _server}}, index, reqids} ->
            answers = Map.put(answers, index, {:error, {:unreachable, reason}})
            pipeline(overwatch, pending, deadline, window, reqids, answers)

          # Both the unsent records and the open requests carry their index last.
          :timeout ->
            for {_, index} <- pending ++ :gen_server.reqids_to_list(reqids),
                into: answers,
                do: {index, {:error, {:unreachable, :timeout}}}
        end
    end
  end

  @doc """
  Commit a value a provider's worker holds of its own onto the plugin's options,
  through its host: the live table a plugin restart prepares the plugin from, and
  the declared manifest a controller restart rebuilds the relay from. It does
  not wait. A worker with no host identity has nowhere to write, and writes
  nothing.
  """
  @spec persist_opt(pid() | nil, binary() | nil, atom(), term()) :: :ok
  def persist_opt(host, plugin_id, key, value) when is_pid(host) and is_binary(plugin_id),
    do: GenServer.cast(host, {:plugin_opt_persist, plugin_id, key, value})

  def persist_opt(_host, _plugin_id, _key, _value), do: :ok

  @doc """
  Commit a pull source's position, the `key` of its worker's `state`, through
  `persist_opt/4` once it is due, answering the state with `committed` and
  `committed_at` moved. `next` is what the source does next: `:more` while it
  reads without waiting, anything else once it has caught up, failed or stopped.

  A position the relay journaled (`durable_input`) is due at once, because a
  record read again after a restart is absorbed while the input log holds it.
  Otherwise it is due once the source stops reading without waiting, and every
  `commit_interval_ms` while it does not: one registry write per run of
  deliveries rather than per record.
  """
  @spec commit_position(map(), atom(), atom()) :: map()
  def commit_position(state, key, next) do
    position = Map.fetch!(state, key)
    now = :erlang.monotonic_time(:millisecond)

    if position_due?(state, position, next, now) do
      persist_opt(state.overwatcher_pid, Map.get(state, :source_plugin_id), key, position)
      %{state | committed: position, committed_at: now}
    else
      state
    end
  end

  @doc """
  Whether a source's `position` is due to be confirmed at `now`, by the rule
  `commit_position/3` commits by: it differs from `state.committed`, and the
  relay journaled it (`durable_input`), or the source stops reading without
  waiting (`next` other than `:more`), or `commit_interval_ms` has passed since
  `state.committed_at`. A source that confirms upstream rather than through
  `persist_opt/4` asks the same question.
  """
  @spec position_due?(map(), term(), atom(), integer()) :: boolean()
  def position_due?(state, position, next, now),
    do:
      position != state.committed and
        (next != :more or Map.get(state, :durable_input) == true or
           now - state.committed_at >= state.commit_interval_ms)

  @doc """
  The extension table a host relay holds: shareable plugins on it are served
  by their shared copy on this controller (`Flezha.Plugin.Shared`). A partition
  group's member runs every plugin it holds itself, because each member asks
  upstream for its own share of the key range only.
  """
  @spec relay_table(map(), keyword()) :: Flezha.Plugin.Extension.Table.t()
  def relay_table(catalog, relay_opts) do
    shared =
      if Flezha.Partition.identity(relay_opts), do: nil, else: Flezha.Plugin.Shared

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
