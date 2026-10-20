defmodule Flezha.Relay.Calls.Inbound do
  @moduledoc """
  The calls one relay answers: the table of its waiting callers the Overwatch
  holds, taking a call into it, and the client call every outside caller
  makes.

  A call is an ordinary input carrying a token the controller mints
  (`flz.reply_to`, see `Flezha.Relay.Calls.Token.entry_metadata/2`). The
  runner carries the token to every output the input causes, and stamps it on
  every sink output. `divert/4` hands the first such output, on the call's
  answer channel when it names one, to the row its token names, and lets every
  one of them continue to the output routes without the token.

  Two kinds of caller wait here, each a clause set of its own: an outside
  caller, whose row holds the process waiting, and a relay, whose call reaches
  the table as a row keyed by a relay token
  (`o:<caller>:<call id>:<created>:<deadline>:<answer channel>`), built only
  from the controller's own delivery
  (`Flezha.Relay.Calls.Token.relay_token/1`); its answer is handed to the
  delivery for the caller rather than replied to a waiting process. A relay's
  own calls are `Flezha.Relay.Calls.Outbound`'s, kept in the same table.

  Every row carries its deadline on the wall clock (`deadline_at`), which a
  handover carries unchanged, and the Overwatch arms it under the timer family
  `:call` (`arm_call/3`).
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Error
  alias Flezha.Process.CallDelivery
  alias Flezha.Process.Timer
  alias Flezha.Relay.Calls.{Outbound, Token}
  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Ingest.Verdict
  alias Flezha.Relay.Overwatch

  require Verdict

  @reply_to Token.reply_to_key()
  @call Token.call_key()
  @answer_channel Token.answer_channel_key()
  @source Token.source_key()

  defstruct rows: %{}, out: %{}

  @type token :: Token.t()
  @type row :: %{
          required(:from) => GenServer.from(),
          required(:channel) => binary() | nil,
          required(:answer_channel) => binary() | nil,
          required(:started_ms) => integer(),
          required(:deadline_at) => integer(),
          required(:deadline_ms) => non_neg_integer(),
          required(:answered?) => boolean(),
          optional(:sent_as) => {binary() | nil, binary()} | nil
        }
  @type t :: %__MODULE__{
          rows: %{token() => row()},
          out: %{Outbound.call_id() => Outbound.out_row()}
        }

  @typedoc """
  Who the table answers for, named on every error and telemetry event, and
  the function that hands one call event to its `on_call` listeners, `nil`
  while there are none (`Flezha.Relay.Overwatch.callee/1`).
  """
  @type callee :: %{
          required(:namespace) => binary(),
          required(:relay_id) => binary(),
          optional(:activity) => (binary(), map() -> :ok) | nil
        }

  # ---------------------------------------------------------------
  # The caller's side
  # ---------------------------------------------------------------

  @doc """
  Call the Overwatch `overwatch` with one input and wait for its answer. The
  wait is the deadline plus `call_grace_ms`, so the Overwatch's own
  `call.timeout` always arrives first; an Overwatch that goes away answers
  `call.unavailable` at once. `relay` names the callee in that error.
  `source` says who calls (`cli`, `ui`, `plugin`, ...) and is written as
  `flz.call_source` over whatever `metadata` carries there.

  The wait follows its call through a handover: the departing Overwatch says
  the call is moving (`depart/1`) and the arriving one says where it now
  lives (`absorb/1`), so the caller stops watching the first and watches the
  second. A plain `GenServer.call` would take the departing process's exit for
  a crash.
  """
  @spec call(pid(), binary(), map(), pos_integer(), binary(), binary()) ::
          {:ok, binary(), map()} | {:error, Error.t()}
  def call(overwatch, message, metadata, deadline_ms, relay, source)
      when is_pid(overwatch) and is_integer(deadline_ms) and deadline_ms > 0 and
             is_binary(source) do
    metadata = Map.put(metadata, @source, source)
    tag = Process.alias()
    until = now_ms() + deadline_ms + Durations.get(:call_grace_ms)
    monitor = Process.monitor(overwatch)
    # The request `GenServer.call` sends, so the Overwatch answers it with
    # `GenServer.reply/2` wherever the call has moved to.
    send(
      overwatch,
      {:"$gen_call", {self(), [:alias | tag]}, {:call, message, metadata, deadline_ms}}
    )

    result = await([:alias | tag], {overwatch, monitor}, until, relay)
    Process.unalias(tag)
    result
  end

  # `watched` is the process the call lives in and its monitor, or `nil`
  # while the call moves between two.
  defp await(tag, watched, until, relay) do
    receive do
      {^tag, {:call_moving, from}} ->
        await(
          tag,
          if(match?({^from, _}, watched), do: unwatch(watched), else: watched),
          until,
          relay
        )

      {^tag, {:call_moved, to}} ->
        unwatch(watched)
        await(tag, {to, Process.monitor(to)}, until, relay)

      {^tag, reply} ->
        unwatch(watched)
        reply

      {:DOWN, monitor, :process, _pid, reason}
      when watched != nil and elem(watched, 1) == monitor ->
        unavailable(relay, reason)
    after
      max(until - now_ms(), 0) ->
        unwatch(watched)
        unavailable(relay, :timeout)
    end
  end

  defp unwatch({_pid, monitor}) do
    Process.demonitor(monitor, [:flush])
    nil
  end

  defp unwatch(nil), do: nil

  defp unavailable(relay, reason),
    do: {:error, Error.new("call.unavailable", %{relay: relay, reason: down_reason(reason)})}

  defp down_reason(:timeout), do: "no answer from its controller process"
  defp down_reason(reason) when reason in [:noproc, :normal], do: "its process is gone"
  defp down_reason(:noconnection), do: "its node is unreachable"
  defp down_reason(reason), do: "its process exited (#{inspect(reason)})"

  # ---------------------------------------------------------------
  # The table
  # ---------------------------------------------------------------

  @doc "How many callers still wait for an answer."
  @spec waiting(t()) :: non_neg_integer()
  def waiting(%__MODULE__{rows: rows}),
    do: Enum.count(rows, fn {_token, row} -> not row.answered? end)

  @doc """
  A row for a new call, or `{:error, busy}` when the relay already owes
  `call_pending_max` answers. An outside call (an integer `deadline`, in ms)
  keeps the entry channel and the answer channel its `metadata` names, and
  `from` waits on it. A relay's call (`deadline` the call itself) is keyed by
  its relay token; one sent again while its first row waits keeps that row.
  """
  @spec admit(t(), callee(), GenServer.from() | nil, map(), pos_integer() | map()) ::
          {:ok, token(), t()} | {:error, Error.t()}
  def admit(%__MODULE__{} = calls, callee, from, metadata, deadline_ms)
      when is_integer(deadline_ms) do
    max = Config.get(:call_pending_max)

    if waiting(calls) >= max do
      {:error, refused(callee, "call.busy", %{pending_max: max})}
    else
      token = Token.mint()

      row = %{
        from: from,
        channel: Map.get(metadata, "channel"),
        answer_channel: answer_channel(metadata),
        started_ms: now_ms(),
        deadline_at: System.system_time(:millisecond) + deadline_ms,
        deadline_ms: deadline_ms,
        answered?: false
      }

      {:ok, token, %{calls | rows: Map.put(calls.rows, token, row)}}
    end
  end

  def admit(%__MODULE__{} = calls, callee, _from, _metadata, call) do
    token = Token.relay_token(call)
    max = Config.get(:call_pending_max)

    cond do
      Map.has_key?(calls.rows, token) ->
        {:ok, token, calls}

      waiting(calls) >= max ->
        {:error, refused(callee, "call.busy", %{pending_max: max})}

      true ->
        {:ok, token, %{calls | rows: Map.put(calls.rows, token, relay_row(call))}}
    end
  end

  # A relay caller's row: nobody waits on it here, since the caller times out
  # on its own clock.
  defp relay_row(call) do
    %{
      from: nil,
      channel: nil,
      answer_channel: Map.get(call, :answer_channel),
      started_ms: now_ms(),
      deadline_at: call.deadline_at,
      deadline_ms: left_ms(call.deadline_at),
      answered?: false
    }
  end

  @doc "The answer channel `metadata` names, `nil` for none."
  @spec answer_channel(map()) :: binary() | nil
  def answer_channel(metadata) do
    case Map.get(metadata, @answer_channel) do
      "" -> nil
      channel -> channel
    end
  end

  @doc """
  Drop the row `token` names, answering nobody: its call was never taken, for
  the refusal `code` its listeners are told.
  """
  @spec forget(t(), callee(), token(), binary(), map()) :: t()
  def forget(%__MODULE__{} = calls, callee, token, code, details) do
    row = Map.get(calls.rows, token)
    outcome(callee, :refused, token, row, row && row.channel, {code, details})
    %{calls | rows: Map.delete(calls.rows, token)}
  end

  @doc """
  One output, before any plugin sees it: `:output` when it carries neither a
  request nor a call's token; `:call` when it is a request of this relay's own
  call. An output carrying a token answers the call it names when that call
  still waits and its answer channel is unset or the output's own, and is then
  an output like any other: `{:answered, calls, metadata}`, or
  `{:held, calls, metadata}` when it was handed to the delivery for a relay
  caller, whose outbox hold the Overwatch takes. `metadata` is the output's
  own without the token.
  """
  @spec divert(t(), callee(), binary(), map()) ::
          :output | :call | {:answered, t(), map()} | {:held, t(), map()}
  def divert(%__MODULE__{} = calls, callee, message, metadata) do
    case {Map.has_key?(metadata, @call), Map.fetch(metadata, @reply_to)} do
      {true, _} -> :call
      {false, :error} -> :output
      {false, {:ok, token}} -> answer(calls, callee, token, message, metadata)
    end
  end

  # The first answer reaches its caller and marks its row answered with the
  # outbox identity it left under; a relay's answer is handed to the
  # delivery, whose outbox hold the Overwatch takes. The outbox dispatching
  # that answer again, its delivery lost, hands it on again.
  defp answer(calls, callee, token, message, metadata) do
    output = Map.delete(metadata, @reply_to)
    caller = waiting(calls, token, sent_as(metadata))

    if answers?(caller, Map.get(calls.rows, token), Map.get(metadata, "channel")) do
      reply(callee, caller, {:ok, message}, metadata)

      calls =
        case {Map.get(calls.rows, token), caller} do
          {%{answered?: false} = row, _caller} ->
            emit(:answered, %{duration_us: duration_us(row)}, callee, %{})
            outcome(callee, :answered, token, row, metadata["channel"], message)
            row = Map.merge(row, %{answered?: true, sent_as: sent_as(metadata)})
            %{calls | rows: Map.put(calls.rows, token, row)}

          # A callee restarted since it took the call keeps no row: the one it
          # leaves now makes every later output of the call an ordinary one.
          {nil, {:relay, call}} ->
            outcome(callee, :answered, token, nil, metadata["channel"], message)
            row = Map.merge(relay_row(call), %{answered?: true, sent_as: sent_as(metadata)})
            %{calls | rows: Map.put(calls.rows, token, row)}

          {_answered, _caller} ->
            calls
        end

      if match?({:relay, _call}, caller),
        do: {:held, calls, output},
        else: {:answered, calls, output}
    else
      {:answered, calls, output}
    end
  end

  # The answer channel a relay's call names rides its token, which a row-less
  # callee still reads; an outside call's rides its row.
  defp answers?({:relay, call}, _row, channel), do: call.answer_channel in [nil, channel]
  defp answers?({:outside, _from}, row, channel), do: row.answer_channel in [nil, channel]
  defp answers?(_answered_or_unclaimed, _row, _channel), do: false

  # The outbox identity a durable answer leaves under, or `nil`.
  defp sent_as(%{"flz.seq" => seq} = metadata) when is_binary(seq),
    do: {metadata["flz.cell"], seq}

  defp sent_as(_metadata), do: nil

  # Who waits on `token`: an outside caller's process, or, for a relay's call,
  # the caller its token names, with a row or without one (a callee restarted
  # since it took the call has none, and an answer sent again as `sent_as` finds
  # its own) until the call's deadline, when the caller has answered itself
  # `call.timeout`; else why nobody does.
  defp waiting(calls, token, sent_as \\ nil) do
    now = System.system_time(:millisecond)

    case {Map.get(calls.rows, token), Token.caller_of(token)} do
      {%{sent_as: ^sent_as}, {:ok, call}} when sent_as != nil -> {:relay, call}
      {%{answered?: true}, _call} -> :answered
      {nil, {:ok, %{deadline_at: at}}} when at <= now -> :unclaimed
      {_row, {:ok, call}} -> {:relay, call}
      {%{from: from}, :error} when from != nil -> {:outside, from}
      {_row, :error} -> :unclaimed
    end
  end

  defp reply(_callee, {:outside, from}, {:ok, message}, metadata),
    do: GenServer.reply(from, {:ok, message, %{"channel" => Map.get(metadata, "channel")}})

  defp reply(_callee, {:outside, from}, {:error, _error} = result, _metadata),
    do: GenServer.reply(from, result)

  defp reply(callee, {:relay, call}, result, metadata) do
    outcome = %{result: result, target: callee.relay_id, created: call.created}
    CallDelivery.answer(callee.namespace, callee.relay_id, call, outcome, metadata)
  end

  @doc """
  A row's deadline passed: a caller still waiting is answered `call.timeout`;
  an answered row is retired. The input stays accepted either way. A relay
  caller times out on its own clock, so its unanswered row is retired
  answering nobody; its listeners are told it timed out all the same.
  """
  @spec expire(t(), callee(), token()) :: t()
  def expire(%__MODULE__{} = calls, callee, token) do
    case Map.pop(calls.rows, token) do
      {%{answered?: false, from: nil} = row, rows} ->
        outcome(
          callee,
          :timed_out,
          token,
          row,
          row.channel,
          {"call.timeout", %{deadline_ms: row.deadline_ms}}
        )

        %{calls | rows: rows}

      {%{answered?: false} = row, rows} ->
        emit(:timeout, %{count: 1}, callee, %{})
        error = error(callee, "call.timeout", %{deadline_ms: row.deadline_ms})
        GenServer.reply(row.from, {:error, error})
        outcome(callee, :timed_out, token, row, row.channel, error)
        %{calls | rows: rows}

      {_answered_or_gone, rows} ->
        %{calls | rows: rows}
    end
  end

  @doc """
  Answer the call `token` names with `code` at once, if it still waits, and
  retire its row. A relay's call waits on its token alone, row or not; an
  answered row, or an outside call with no row, is left as it is.
  """
  @spec refuse(t(), callee(), token() | nil, binary(), map()) :: t()
  def refuse(%__MODULE__{} = calls, callee, token, code, details \\ %{}) do
    case waiting(calls, token) do
      reason when reason in [:answered, :unclaimed] ->
        calls

      caller ->
        reply(callee, caller, {:error, refused(callee, code, details)}, %{})
        forget(calls, callee, token, code, details)
    end
  end

  @doc """
  Answer every waiting caller `call.callee_ended` with `status`, and empty the
  table. `tail` names the relay tokens of the calls the callee took and
  never ran, which are answered the same way.
  """
  @spec end_all(t(), callee(), binary(), [token()]) :: t()
  def end_all(%__MODULE__{} = calls, callee, status, tail \\ []) do
    # Best effort for a relay caller: one whose answer is lost times out.
    tokens = for({token, %{answered?: false}} <- calls.rows, do: token) ++ tail

    for token <- Enum.uniq(tokens),
        do: refuse(calls, callee, token, "call.callee_ended", %{status: status})

    %__MODULE__{}
  end

  @doc "The table without its relay callers' rows: its outside callers alone."
  @spec outside(t()) :: t()
  def outside(%__MODULE__{} = calls),
    do: %{
      calls
      | rows: Map.filter(calls.rows, fn {token, _row} -> Token.caller_of(token) == :error end)
    }

  @doc """
  The rows a handover carries, each with its deadline on the wall clock, so
  the time the handover spends in flight counts against it: a `from` is valid
  on any node, so the answer can still reach its caller from there.
  """
  @spec export(t()) :: [{token() | {:out, Outbound.call_id()}, map()}]
  def export(%__MODULE__{rows: rows, out: out}),
    do: Map.to_list(rows) ++ for({id, row} <- out, do: {{:out, id}, %{row | settling: false}})

  @doc """
  The rows left with a handover: tell every caller still waiting that its call
  is moving, so it stops watching this process, and empty the table.
  """
  @spec depart(t()) :: t()
  def depart(%__MODULE__{} = calls) do
    reply_waiting(calls.rows, fn -> {:call_moving, self()} end)
    %__MODULE__{}
  end

  @doc """
  The table a handover arrived with, each row keeping its deadline; the
  caller arms them (`arm_call/3`). Every caller still waiting is told its call
  now lives in this process.
  """
  @spec absorb([{token(), map()}]) :: t()
  def absorb(exported) do
    now = now_ms()
    shift = System.system_time(:millisecond) - now
    out = for {{:out, id}, row} <- exported, into: %{}, do: {id, row}

    rows =
      for {token, row} when is_binary(token) <- exported, into: %{} do
        # Re-based on this node's clock, so the call's duration stays measured.
        {token, %{row | started_ms: row.deadline_at - shift - row.deadline_ms}}
      end

    reply_waiting(rows, fn -> {:call_moved, self()} end)
    %__MODULE__{rows: rows, out: out}
  end

  @doc "The time left before the wall-clock `deadline_at`, in ms; `0` once it passed."
  @spec left_ms(integer()) :: non_neg_integer()
  def left_ms(deadline_at), do: max(deadline_at - System.system_time(:millisecond), 0)

  @doc """
  Until when a call whose deadline is `deadline_at` is retained: its outcome
  may still arrive within `call_retention_margin_ms` after it.
  """
  @spec retained_until(integer()) :: integer()
  def retained_until(deadline_at), do: deadline_at + Durations.get(:call_retention_margin_ms)

  # ---------------------------------------------------------------
  # Taking a call
  # ---------------------------------------------------------------

  @doc """
  The `:call` entry point. An outside caller's call carries an integer
  deadline: it is refused at once when nothing can answer it, else it gets a
  row with its deadline and the confirming ingest of an input carrying the
  token the row is keyed by, and every answer arrives later, from the row.

  A relay's call is delivered by the controller alone, carrying the call
  (its caller, its id, its wall-clock deadline, whether this delivery started
  the instance and, for a factory call, the factory): it gets a row keyed by
  a relay token built from those fields, counted against
  `call_pending_max`, and its ingest's verdict is answered at once. Its input
  carries the call id as `flz.msg_id`, so a request sent again is absorbed
  while the first is retained. Answers `handle_call/3`'s reply.
  """
  @spec take_call(Overwatch.t(), binary(), map(), pos_integer() | map(), GenServer.from() | nil) ::
          {:reply, term(), Overwatch.t()} | {:noreply, Overwatch.t()}
  def take_call(%Overwatch{} = state, message, metadata, deadline, from) do
    callee = Overwatch.callee(state)

    with nil <- call_refusal(state) || factory_refusal(state, deadline),
         {:ok, token, calls} <- admit(state.calls, callee, from, metadata, deadline) do
      # A relay's call sent again while its row waits was announced the first time.
      unless Map.has_key?(state.calls.rows, token),
        do: announce(callee, token, message, metadata, deadline)

      %{state | calls: calls}
      |> arm_call({:call_deadline, token}, calls.rows[token].deadline_at)
      |> ingest_call(message, metadata, token, deadline, from)
    else
      {:error, error} ->
        unadmitted(state, callee, message, metadata, deadline, error)

      {code, details} ->
        unadmitted(state, callee, message, metadata, deadline, refused(callee, code, details))
    end
  end

  defp unadmitted(state, callee, message, metadata, deadline, error) do
    announce(callee, nil, message, metadata, deadline, error)
    {:reply, {:error, error}, state}
  end

  # An outside caller keeps waiting on an input that was accepted, even by an
  # instance that was asleep, and is answered from its row; a relay's delivery
  # is answered the ingest's verdict at once.
  defp ingest_call(state, message, metadata, token, deadline_ms, _from)
       when is_integer(deadline_ms) do
    metadata = Token.entry_metadata(metadata, token)

    case Ingest.confirming_ingest(state, message, metadata, {:call, token}) do
      {:pending, state} -> {:noreply, state}
      {verdict, state} -> {:noreply, answer_call(state, token, verdict)}
    end
  end

  defp ingest_call(state, message, metadata, token, call, from) do
    metadata =
      metadata
      |> Token.entry_metadata(token)
      |> Map.put("flz.msg_id", call.call)
      |> Map.put("flz.deadline_at", Integer.to_string(call.deadline_at))

    case Ingest.confirming_ingest(state, message, metadata, {:relay_call, from, token}) do
      {:pending, state} -> {:noreply, state}
      {verdict, state} -> {:reply, verdict, forget_unless_taken(state, token, verdict)}
    end
  end

  # Why a call cannot be taken at all: a halted relay, or a single-pass
  # steps instance that has already been given its input.
  defp call_refusal(%Overwatch{halt_reason: reason}) when reason != nil,
    do: {"call.unavailable", %{reason: "the relay is halted"}}

  defp call_refusal(%Overwatch{} = state) do
    if Ingest.single_input?(state) and (state.fed or not Ingest.first_input?(state)),
      do: {"call.not_accepted", %{}}
  end

  # A factory call reaches only an instance of that factory, whatever the key
  # that named it said.
  defp factory_refusal(%Overwatch{factory_id: actual} = state, %{factory: factory})
       when is_binary(factory) and factory != actual,
       do:
         {"relay.factory_mismatch",
          %{namespace: state.namespace, factory: factory, actual: actual}}

  defp factory_refusal(_state, _call), do: nil

  @doc """
  The journal's verdict on a relay's call input reached `from`, the delivery
  that is owed it; a call its callee did not take leaves no row behind.
  """
  @spec taken(Overwatch.t(), GenServer.from() | nil, token(), term()) :: Overwatch.t()
  def taken(%Overwatch{} = state, from, token, verdict) do
    Ingest.reply(from, verdict)
    forget_unless_taken(state, token, verdict)
  end

  defp forget_unless_taken(%Overwatch{} = state, token, verdict) do
    if Verdict.taken?(verdict) do
      state
    else
      {code, details} = refusal(verdict)

      %Overwatch{
        state
        | calls: forget(state.calls, Overwatch.callee(state), token, code, details),
          timers: Timer.cancel(state.timers, {:call, token})
      }
    end
  end

  @doc """
  The journal's verdict on an outside call's input: an accepted input leaves
  the caller waiting for its answer, an asleep instance's too, since the
  append wakes it; every other verdict ends the call.
  """
  @spec answer_call(Overwatch.t(), token(), term()) :: Overwatch.t()
  def answer_call(%Overwatch{} = state, _token, verdict) when verdict in [:ok, {:ok, :suspended}],
    do: state

  def answer_call(%Overwatch{} = state, token, verdict) do
    {code, details} = refusal(verdict)
    %Overwatch{state | calls: refuse(state.calls, Overwatch.callee(state), token, code, details)}
  end

  @doc "The refusal a call is answered with for an ingest verdict that ends it."
  @spec refusal(term()) :: {binary(), map()}
  def refusal({:ok, :absorbed}), do: {"call.absorbed", %{}}

  def refusal({:error, :refused}),
    do: {"call.unavailable", %{reason: "its input log refused the input"}}

  def refusal({:error, :not_ready}),
    do: {"call.unavailable", %{reason: "no runner can take the input"}}

  @doc """
  Arm the deadline that retires the row of the relay call `token` names,
  unless one is armed: a held answer of a callee restarted since it took the
  call has just made that row.
  """
  @spec retire_at_deadline(Overwatch.t(), token()) :: Overwatch.t()
  def retire_at_deadline(%Overwatch{} = state, token) do
    if Timer.armed?(state.timers, {:call, token}) or not Map.has_key?(state.calls.rows, token),
      do: state,
      else: arm_call(state, {:call_deadline, token}, state.calls.rows[token].deadline_at)
  end

  @doc """
  Arm the `:call` timer that sends `message` (`{kind, key}`, keyed by `key`)
  at the wall-clock `deadline_at`, at once when it has passed.
  """
  @spec arm_call(Overwatch.t(), {atom(), binary()}, integer()) :: Overwatch.t()
  def arm_call(%Overwatch{} = state, {_kind, key} = message, deadline_at),
    do: %{
      state
      | timers: Timer.arm(state.timers, {:call, key}, left_ms(deadline_at), message)
    }

  @doc "An outcome or answer for a caller nobody waits on any more was dropped, for `reason`."
  @spec answer_dropped(callee(), binary(), map()) :: :ok
  def answer_dropped(callee, reason, metadata \\ %{}),
    do: emit(:answer_dropped, %{count: 1}, callee, Map.put(metadata, :reason, reason))

  @doc """
  The error a call the relay cannot take is refused with, counted as
  `[:flezha, :call, :refused]`.
  """
  @spec refused(callee(), binary(), map()) :: Error.t()
  def refused(callee, code, details) do
    emit(:refused, %{count: 1}, callee, %{code: code})
    error(callee, code, details)
  end

  @doc "The error `code` names for a call to `callee`, counting nothing."
  @spec error(callee(), String.t(), map()) :: Error.t()
  def error(callee, code, details),
    do: Error.new(code, Map.put(details, :relay, callee.relay_id))

  # ---------------------------------------------------------------
  # Call events, for the `on_call` listeners
  # ---------------------------------------------------------------

  @doc """
  Announce a call the relay takes, `message` as its entry point took it:
  `deadline` is an outside call's deadline in ms, or a relay's call. With a
  `refusal` the call has no row: it is announced under an id of its own,
  together with its `refused` outcome. Nothing is built while nobody listens.
  """
  @spec announce(callee(), token() | nil, binary(), map(), pos_integer() | map(), Error.t() | nil) ::
          :ok
  def announce(callee, token, message, metadata, deadline, refusal \\ nil)

  def announce(%{activity: activity} = callee, token, message, metadata, deadline, refusal)
      when activity != nil do
    token = token || Token.mint()

    called =
      %{
        "flz.call_id" => Token.call_id(token),
        "flz.call_at" => System.system_time(:millisecond),
        "channel" => metadata["channel"]
      }
      |> Map.merge(called_by(metadata, deadline))
      |> named()

    activity.(message, Map.put(called, "flz.call_event", "call"))

    if refusal,
      do: outcome(callee, :refused, token, nil, metadata["channel"], refusal)

    :ok
  end

  def announce(_callee, _token, _message, _metadata, _deadline, _refusal), do: :ok

  defp called_by(metadata, deadline_ms) when is_integer(deadline_ms),
    do: %{
      @source => metadata[@source],
      @answer_channel => answer_channel(metadata),
      "flz.call_deadline_ms" => deadline_ms
    }

  defp called_by(_metadata, call),
    do: %{
      @source => "relay",
      "flz.call_caller" => call.caller,
      @answer_channel => Map.get(call, :answer_channel),
      "flz.call_deadline_ms" => left_ms(call.deadline_at)
    }

  # How the call `token` names ended, to its listeners: `answer` for an
  # answer, an error, or an error's code and details, built only for them.
  defp outcome(%{activity: activity} = callee, kind, token, row, channel, answer)
       when activity != nil do
    ended =
      named(%{
        "flz.call_event" => Atom.to_string(kind),
        "flz.call_id" => Token.call_id(token),
        "flz.call_duration_ms" => if(row, do: now_ms() - row.started_ms, else: 0),
        "channel" => channel
      })

    activity.(event_payload(callee, answer), ended)
  end

  defp outcome(_callee, _kind, _token, _row, _channel, _answer), do: :ok

  # A channel or a source the call does not name is left out.
  defp named(metadata), do: Map.reject(metadata, fn {_key, value} -> value in [nil, ""] end)

  defp event_payload(_callee, answer) when is_binary(answer), do: answer
  defp event_payload(_callee, %Error{} = error), do: Error.render(error, :json)

  defp event_payload(callee, {code, details}),
    do: event_payload(callee, error(callee, code, details))

  # Every caller still waiting is answered `reply.()`, built (and an error
  # counted) once per caller.
  defp reply_waiting(rows, reply) do
    for {_token, %{answered?: false, from: from}} when from != nil <- rows,
        do: GenServer.reply(from, reply.())

    :ok
  end

  defp duration_us(row), do: (now_ms() - row.started_ms) * 1_000

  @doc "Count one call event, `[:flezha, :call, event]`, for `callee`."
  @spec emit(atom(), map(), callee(), map()) :: :ok
  def emit(event, measurements, callee, metadata) do
    :telemetry.execute(
      [:flezha, :call, event],
      measurements,
      Map.merge(%{namespace: callee.namespace, relay_id: callee.relay_id}, metadata)
    )
  end

  defp now_ms, do: System.monotonic_time(:millisecond)
end
