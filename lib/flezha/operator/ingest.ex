defmodule Flezha.Operator.Ingest do
  @moduledoc """
  The operator's input path: type a payload, run the `:on_input` chain, count
  it, make it durable, and forward it to the bound stream channel.

  Every way in — the `:io_in` cast, the blocking `:io_in` call, the confirming
  `:ingest` call, an outside `:call`, a suspended instance's deferred append
  and a spawn's seed — runs exactly `ingest/4`. Only a call's input carries a
  `flz.reply_to`: every other way in loses the key, and no `:on_input` hook
  can add, drop or rewrite it.

  A durable append is asynchronous: the payload is handed to the log, the entry
  point's reply stays owed, and the log's answer (`journal_reply/2`) forwards
  the input and settles it once the record is synced.

  Invariant: nothing else journals input, which is what lets the correction
  transaction assume none can arrive while it runs.
  """

  alias Flezha.Config.Durations
  alias Flezha.Operator.Calls
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Operator.Migration
  alias Flezha.Operator.Overwatch
  alias Flezha.Operator.Plugins
  alias Flezha.Operator.Replay
  alias Flezha.Process.CallDelivery
  alias Flezha.Process.Timer
  alias Flezha.State.InputLog
  alias Flezha.Transport.LivenessCounters

  require Logger

  @typedoc """
  What the journal answers with, and the only thing a caller may forward.

  `nil` ⇒ filtered by a plugin or absorbed as a duplicate; `:refused` ⇒ the
  durable log rejected the append. Neither is forwarded; only the confirming
  entry point tells them apart.
  """
  @type payload :: {binary(), map()} | nil | :refused

  # Metadata only the controller sets: where a stepped scope's message goes
  # (`flz.scope`) and which channel a scope's exit value enters (`flz.exit`).
  @core_set ["flz.scope", "flz.exit"]

  # The verdicts under which an operator's call, or its `Response`, was taken:
  # a duplicate absorbed was taken the first time.
  @taken [:ok, {:ok, :suspended}, {:ok, :absorbed}]

  @typedoc """
  Who is owed the outcome: nobody for a cast, or the entry point and its
  caller. The blocking `:io_in` call acts on accepted vs parked and nothing
  finer, so it is told `:ok`; the confirming `:ingest` call is told the verdict.
  A `:call` is owed its answer by the call row its token names, so only a
  verdict that ends the call is acted on (`answer_call/3`).
  """
  @type reply_to ::
          :cast
          | {:io_in | :ingest, GenServer.from()}
          | {:call, Calls.token()}
          | {:operator_call, GenServer.from() | nil, Calls.token()}
          | {:call_answer, GenServer.from() | nil, Calls.call_id()}

  @doc """
  Ingest one inbound message. Answers the payload to forward, or `:pending`
  when the durable input log holds it — the log's answer comes back to this
  process as a message for `journal_reply/2`.
  """
  @spec ingest(Overwatch.t(), binary(), map(), reply_to()) ::
          {payload() | :pending, Overwatch.t()}
  def ingest(%Overwatch{} = state, message, metadata, reply_to) do
    metadata =
      metadata
      |> Map.drop(@core_set)
      |> admitted_token(reply_to)
      |> Map.put_new("input_type", state.default_input_type)

    payload =
      case Plugins.transform_io(state, :on_input, message, metadata) do
        {message, hooked} -> {message, Calls.restore(hooked, metadata)}
        filtered -> filtered
      end

    record_io(state.liveness_ref, payload)
    journal_input(state, payload, reply_to)
  end

  # The token a call's entry point minted is the only one an input may carry.
  defp admitted_token(metadata, {:call, _token}), do: metadata
  defp admitted_token(metadata, {:operator_call, _from, _token}), do: metadata
  defp admitted_token(metadata, _reply_to), do: Calls.strip(metadata)

  @doc """
  The `:call` entry point. An outside caller's call carries an integer
  deadline: it is refused at once when nothing can answer it, else it gets a
  row with its deadline and the confirming ingest of an input carrying the
  token the row is keyed by, and every answer arrives later, from the row.

  An operator's call is delivered by the controller alone, carrying the call
  (its caller, its id, its wall-clock deadline, whether this delivery started
  the instance and, for a factory call, the factory): it gets a row keyed by
  an operator token built from those fields, counted against
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
      %{state | calls: calls}
      |> arm_call(token, wait_ms(deadline), {:call_deadline, token})
      |> ingest_call(message, metadata, token, deadline, from)
    else
      {:error, error} -> {:reply, {:error, error}, state}
      {code, details} -> {:reply, {:error, Calls.refused(callee, code, details)}, state}
    end
  end

  defp admit(calls, callee, from, metadata, deadline_ms) when is_integer(deadline_ms),
    do: Calls.admit(calls, callee, from, Map.get(metadata, "channel"), deadline_ms)

  defp admit(calls, callee, _from, _metadata, call), do: Calls.admit_operator(calls, callee, call)

  defp wait_ms(deadline_ms) when is_integer(deadline_ms), do: deadline_ms
  defp wait_ms(call), do: until(call.deadline_at)

  # An outside caller is answered from its row; an operator's delivery is
  # answered the ingest's verdict at once.
  defp ingest_call(state, message, metadata, token, deadline_ms, _from)
       when is_integer(deadline_ms),
       do: {:noreply, call(state, message, Calls.entry_metadata(metadata, token), token)}

  defp ingest_call(state, message, metadata, token, call, from) do
    metadata =
      metadata
      |> Calls.entry_metadata(token)
      |> Map.put("flz.msg_id", call.call)
      |> Map.put("flz.deadline_at", Integer.to_string(call.deadline_at))

    case confirming_ingest(state, message, metadata, {:operator_call, from, token}) do
      {:pending, state} -> {:noreply, state}
      {verdict, state} -> {:reply, verdict, forget_unless_taken(state, token, verdict)}
    end
  end

  # Why a call cannot be taken at all: a parked operator, or a single-pass
  # steps instance that has already been given its input.
  defp call_refusal(%Overwatch{init_error: reason}) when reason != nil,
    do: {"call.unavailable", %{reason: "the operator is parked"}}

  defp call_refusal(%Overwatch{} = state) do
    if single_input?(state) and (state.fed or not first_input?(state)),
      do: {"call.not_accepted", %{}}
  end

  # A factory call reaches only an instance of that factory, whatever the key
  # that named it said.
  defp factory_refusal(%Overwatch{factory_id: actual} = state, %{factory: factory})
       when is_binary(factory) and factory != actual,
       do:
         {"operator.factory_mismatch",
          %{namespace: state.namespace, factory: factory, actual: actual}}

  defp factory_refusal(_state, _call), do: nil

  defp taken(state, from, token, verdict) do
    reply_to_caller(from, verdict)
    forget_unless_taken(state, token, verdict)
  end

  # A call its callee did not take leaves no row behind.
  defp forget_unless_taken(state, _token, verdict)
       when verdict in @taken,
       do: state

  defp forget_unless_taken(%Overwatch{} = state, token, _verdict) do
    %Overwatch{
      state
      | calls: Calls.forget(state.calls, token),
        timers: Timer.cancel(state.timers, {:call, token})
    }
  end

  @doc """
  One of this operator's own calls leaves, its output's outbox hold already
  taken by the Overwatch when it is `durable?`. A call still pending (a
  re-dispatch of its output) changes nothing. A durable call the Overwatch has
  no row for is resolved before anything is sent: its `Response` retained in
  this operator's log settles it; with the deadline ahead it is sent (again:
  the callee absorbs a repeat by its message id) and its deadline armed; past
  the deadline, within `call_retention_margin_ms`, no `Response` was ever
  appended, so `call.timeout` is; past the margin one may have been appended
  and truncated since, so none is, and the call settles as
  `recovered_unanswered`.
  """
  @spec request(Overwatch.t(), binary(), map(), boolean()) :: Overwatch.t()
  def request(%Overwatch{} = state, message, metadata, durable?) do
    {id, row} = Calls.request(message, metadata, durable?)

    cond do
      Calls.pending(state.calls, id) != nil -> state
      durable? and answered?(state, id) -> settle_hold(state, row)
      true -> send_request(state, id, row)
    end
  end

  defp answered?(%Overwatch{} = state, id),
    do: InputLog.msg_offset(Overwatch.operator_class(state), "r:" <> id) != nil

  defp send_request(state, id, row) do
    left = until(row.deadline_at)
    state = %{state | calls: Calls.open(state.calls, id, row)}

    cond do
      left > 0 ->
        CallDelivery.request(state.namespace, state.id, request_of(id, row))
        arm_call(state, id, left, {:call_timeout, id})

      left + Durations.get(:call_retention_margin_ms) > 0 ->
        timeout(state, id)

      true ->
        {row, calls} = Calls.close(state.calls, id)
        Calls.recovered_unanswered(Overwatch.callee(state), row)
        settle_hold(%{state | calls: calls}, row)
    end
  end

  defp request_of(id, row), do: row |> Map.drop([:hold, :settling]) |> Map.put(:id, id)

  defp settle_hold(state, row) do
    Overwatch.confirm_hold(row.hold)
    state
  end

  @doc """
  Call `id`'s deadline passed with no outcome: `call.timeout` is its outcome.
  """
  @spec timeout(Overwatch.t(), Calls.call_id()) :: Overwatch.t()
  def timeout(%Overwatch{} = state, id) do
    case Calls.pending(state.calls, id) do
      %{settling: false} = row ->
        error = Calls.timeout(row, Overwatch.callee(state))
        respond(state, id, Calls.own_outcome(row, error), nil)

      _settling_or_gone ->
        state
    end
  end

  @doc """
  The outcome of call `id` reached this caller. The first becomes a `Response`
  input on the call's Response channel, journaled with `flz.msg_id`
  `r:<id>` when this operator journals input; once it is durable (or at once
  off the tier) the call is over, its output's hold answered and `from`
  answered `:ok`. Any later outcome is dropped and counted, `from` answered
  `{:ok, :absorbed}`, unless this caller journals its `Response`s, has none
  for the call, and its writers still held calls it had no row for: `from` is
  then answered an error, so the delivery retries once they are dispatched
  again.
  """
  @spec respond(Overwatch.t(), Calls.call_id(), Calls.outcome(), GenServer.from() | nil) ::
          Overwatch.t()
  def respond(%Overwatch{} = state, id, outcome, from) do
    case Calls.settle(state.calls, id) do
      {:ok, row, calls} ->
        metadata = %{
          "channel" => row.response,
          "flz.msg_id" => "r:" <> id,
          "flz.deadline_at" => Integer.to_string(row.deadline_at)
        }

        state = %{state | calls: calls}
        envelope = Calls.envelope(id, row, outcome)

        case confirming_ingest(state, envelope, metadata, {:call_answer, from, id}) do
          {:pending, state} -> state
          {verdict, state} -> responded(state, from, id, verdict)
        end

      :none ->
        reply_to_caller(from, unclaimed(state, id))
        state
    end
  end

  # An outcome for no row the caller can take: the call is over, or, for a
  # caller that journals its `Response`s and restarted since it made the call,
  # not relearned yet. Its writers then dispatch their held calls now and the
  # delivery retries.
  defp unclaimed(%Overwatch{} = state, id) do
    if Replay.floor_reporter(state) != nil and Calls.pending(state.calls, id) == nil and
         not answered?(state, id) and Replay.redrive_calls(state) > 0 do
      {:error, :call_not_relearned}
    else
      Calls.unclaimed(Overwatch.callee(state))
      {:ok, :absorbed}
    end
  end

  defp responded(%Overwatch{} = state, from, id, verdict)
       when verdict in @taken do
    {row, calls} = Calls.close(state.calls, id)
    if row, do: Overwatch.confirm_hold(row.hold)
    reply_to_caller(from, :ok)
    %{state | calls: calls, timers: Timer.cancel(state.timers, {:call, id})}
  end

  # The append failed: the call waits again, the delivery retries, and a
  # deadline that already passed tries its timeout again shortly.
  defp responded(%Overwatch{} = state, from, id, verdict) do
    reply_to_caller(from, verdict)
    state = %{state | calls: Calls.unsettle(state.calls, id)}

    case Calls.pending(state.calls, id) do
      %{deadline_at: at} -> arm_call(state, id, max(until(at), 1_000), {:call_timeout, id})
      nil -> state
    end
  end

  defp arm_call(%Overwatch{} = state, key, ms, message),
    do: %{state | timers: Timer.arm(state.timers, {:call, key}, max(ms, 0), message)}

  defp until(deadline_at), do: deadline_at - System.system_time(:millisecond)

  @doc """
  The operator tokens in this instance's undelivered input: the calls it took
  and will never run once it has ended.
  """
  @spec undelivered_calls(Overwatch.t()) :: [Calls.token()]
  def undelivered_calls(%Overwatch{} = state) do
    case InputLog.undelivered(Overwatch.operator_class(state)) do
      records when is_list(records) ->
        for {_offset, _message, %{"flz.reply_to" => "o:" <> _ = token}} <- records, do: token

      {:error, _reason} ->
        []
    end
  end

  @doc """
  Ingest an outside call's input, `metadata` already carrying its `token`: the
  caller keeps waiting on an input that was accepted, even by an instance that
  was asleep, and is answered at once when the input was absorbed or refused.
  """
  @spec call(Overwatch.t(), binary(), map(), Calls.token()) :: Overwatch.t()
  def call(%Overwatch{} = state, message, metadata, token) do
    case confirming_ingest(state, message, metadata, {:call, token}) do
      {:pending, state} -> state
      {verdict, state} -> answer_call(state, token, verdict)
    end
  end

  @doc """
  The one ingest-and-report path behind every entry point. Answers what the
  entry point is told, or `:pending` when the reply is owed until the log
  answers — the caller then returns `{:noreply, state}` and `journal_reply/2`
  settles it.

  A parked operator is refused by the callers before it reaches here — each owes
  its own reply for that.
  """
  @spec confirming_ingest(Overwatch.t(), binary(), map(), reply_to()) ::
          {term() | :pending, Overwatch.t()}
  # Suspended: the forward is deferred to the rehydrating bind, which delivers
  # the log's whole undelivered tail; the journal's answer reports the
  # suspension and wakes the instance (`settle/3`). An input a plugin filtered
  # journals nothing, and is answered — and wakes — at once.
  def confirming_ingest(%Overwatch{suspended: true} = state, message, metadata, reply_to) do
    case ingest(state, message, metadata, reply_to) do
      {:pending, state} ->
        {:pending, state}

      {_filtered, state} ->
        {told(reply_to, filtered_asleep(reply_to)), Migration.rehydrate(state)}
    end
  end

  def confirming_ingest(%Overwatch{} = state, message, metadata, reply_to) do
    case ingest(state, message, metadata, reply_to) do
      {:pending, state} -> {:pending, state}
      {payload, state} -> {told(reply_to, verdict(state, payload)), state}
    end
  end

  @doc """
  A stepped scope's exit value into this instance's own graph: journaled with
  no input hook, since it is the instance's own value and not an outside
  input, then forwarded like any journaled input; `reply_to` is told the
  verdict.
  """
  @spec exit_value(Overwatch.t(), binary(), map(), reply_to()) ::
          {term() | :pending, Overwatch.t()}
  def exit_value(%Overwatch{} = state, message, metadata, reply_to) do
    case journal_input(state, {message, metadata}, reply_to) do
      {:pending, state} -> {:pending, state}
      {payload, state} -> {told(reply_to, verdict(state, payload)), state}
    end
  end

  # A filtered input produces nothing, so a call on it can never be answered.
  defp filtered_asleep({:call, _token}), do: {:ok, :absorbed}
  defp filtered_asleep({:call_answer, _from, _id}), do: {:ok, :absorbed}
  defp filtered_asleep(_reply_to), do: {:ok, :suspended}

  @doc """
  The input log's answer to an append `ingest/4` handed it — or the log's exit,
  which the monitored request reports the same way: forward the journaled
  payload and settle the reply owed for it. `:ignore` for any other message.

  The log answers appends in admission order and this process handles the
  answers in arrival order, so forward order is offset order and replies settle
  in call order — across a park too, since the log unparks in arrival order.
  """
  @spec journal_reply(Overwatch.t(), term()) :: {:ok, Overwatch.t()} | :ignore
  def journal_reply(%Overwatch{} = state, msg) do
    case :gen_server.check_response(msg, state.journal_reqids, true) do
      {{:reply, reply}, {reply_to, payload, _retries}, reqids} ->
        state = %{state | journal_reqids: reqids}
        {:ok, settle(state, reply_to, journaled(state, payload, reply))}

      # The log went away under the append. Re-issue once — a restarting log's
      # registry entry can briefly name the dead pid — then refuse, so the
      # sender retries an input that was never forwarded.
      {{:error, {_reason, _log}}, {reply_to, payload, retries}, reqids} when retries > 0 ->
        {:ok, append_async(%{state | journal_reqids: reqids}, payload, reply_to, retries - 1)}

      {{:error, {_reason, _log}}, {reply_to, _payload, 0}, reqids} ->
        {:ok, settle(%{state | journal_reqids: reqids}, reply_to, :refused)}

      _no_reply_or_no_request ->
        :ignore
    end
  end

  # Durable ingest point — the one place an inbound payload is made durable
  # BEFORE it is forwarded, asynchronously, for an instance with durable input
  # (`durable-input-log`): a flow operator on the effectively-once tier or a durable
  # steps operator. The payload is appended to the instance's input log and the
  # answer stamps `flz.offset`, which the SDK threads into the effect record or
  # the iteration commit as the consumed offset. A `flz.msg_id` the log still
  # retains is absorbed (`nil` — nothing is forwarded), and an append that
  # fails answers `:refused` and is NOT forwarded either: the contract is
  # append-before-forward, so a lost frame the sender can retry beats an
  # undurable one it believes was accepted.
  #
  # Every other operator passes through untouched (cost-free default).
  @doc """
  Whether the next input is the instance's first: always for an instance
  without a durable steps operator's log, and for one whose log has journaled
  nothing. A log that holds records makes a bind a restart, not a spawn.
  """
  @spec first_input?(Overwatch.t()) :: boolean()
  def first_input?(%Overwatch{durable_steps: true} = state),
    do: match?({:ok, _floor, -1}, InputLog.tail(Overwatch.operator_class(state)))

  def first_input?(%Overwatch{}), do: true

  @doc """
  Whether the instance reads one input only: a `:once` or `:chained` steps
  operator's pass never reads a second.
  """
  @spec single_input?(Overwatch.t()) :: boolean()
  def single_input?(%Overwatch{factory_id: nil}), do: false

  def single_input?(%Overwatch{} = state),
    do: ClassRegistry.partitioning(state.factory_id).each in [:once, :chained]

  defp journal_input(state, nil, _reply_to), do: {nil, state}

  defp journal_input(%Overwatch{} = state, payload, reply_to) do
    state = if single_input?(state), do: %Overwatch{state | fed: true}, else: state

    if Replay.floor_reporter(state),
      do: {:pending, append_async(state, payload, reply_to, 1)},
      else: {payload, state}
  end

  # Hand the payload to the log without waiting. The request's label carries
  # everything its answer needs — who is owed, what to forward, how many
  # re-issues remain — so the in-flight collection IS the owed set.
  defp append_async(%Overwatch{} = state, {message, metadata} = payload, reply_to, retries) do
    label = {reply_to, payload, retries}

    reqids =
      InputLog.append_async(
        Overwatch.operator_class(state),
        message,
        metadata,
        state.journal_reqids,
        label
      )

    %{state | journal_reqids: reqids}
  end

  # What the log's answer makes of the payload: forwardable and stamped with
  # its durable offset, `nil` for a duplicate the log absorbed, `:refused` for
  # an append the log rejected.
  defp journaled(%Overwatch{}, {message, metadata}, {:ok, offset}),
    do: {message, Map.put(metadata, "flz.offset", Integer.to_string(offset))}

  defp journaled(%Overwatch{} = state, _payload, {:duplicate, offset}) do
    :telemetry.execute(
      [:flezha, :state, :inbox_duplicate],
      %{count: 1},
      %{operator_id: state.id, offset: offset}
    )

    nil
  end

  defp journaled(%Overwatch{} = state, _payload, {:error, reason}) do
    Logger.error(
      "[Overwatch] durable input append failed for #{state.id}: #{inspect(reason)} — " <>
        "input NOT forwarded (append-before-forward)"
    )

    :refused
  end

  # A journaled payload's forward and the reply it owed, once the log answered.
  # A suspended instance forwards nothing: the payload waits in the log for the
  # rehydrating bind, which is what the reply reports and what the append wakes.
  defp settle(%Overwatch{suspended: true} = state, reply_to, payload)
       when payload != :refused do
    state
    |> owe(reply_to, told(reply_to, {:ok, :suspended}))
    |> Migration.rehydrate()
  end

  defp settle(%Overwatch{} = state, reply_to, payload),
    do: owe(state, reply_to, told(reply_to, verdict(state, payload)))

  defp owe(state, {:call, token}, verdict), do: answer_call(state, token, verdict)
  defp owe(state, {:operator_call, from, token}, verdict), do: taken(state, from, token, verdict)
  defp owe(state, {:call_answer, from, id}, verdict), do: responded(state, from, id, verdict)

  defp owe(state, reply_to, reply) do
    reply(reply_to, reply)
    state
  end

  defp reply(:cast, _reply), do: :ok
  defp reply({_entry_point, from}, reply), do: GenServer.reply(from, reply)

  defp reply_to_caller(nil, _reply), do: :ok
  defp reply_to_caller(from, reply), do: GenServer.reply(from, reply)

  # An accepted input leaves the caller waiting for its answer, an asleep
  # instance's too, since the append wakes it; every other verdict ends the call.
  defp answer_call(state, _token, verdict) when verdict in [:ok, {:ok, :suspended}], do: state

  defp answer_call(%Overwatch{} = state, token, verdict) do
    {code, details} =
      case verdict do
        {:ok, :absorbed} -> {"call.absorbed", %{}}
        {:error, :refused} -> {"call.unavailable", %{reason: "its input log refused the input"}}
        {:error, :not_ready} -> {"call.unavailable", %{reason: "no runner can take the input"}}
      end

    %Overwatch{
      state
      | calls: Calls.refuse(state.calls, Overwatch.callee(state), token, code, details)
    }
  end

  defp told({:io_in, _from}, _verdict), do: :ok
  defp told(_reply_to, verdict), do: verdict

  # Forward what the journal answered with, and say what became of it. An unbound
  # stream is not a delivery failure for a durable target: the log holds the
  # payload and the channel's next bind re-forwards the tail above the Cell's
  # consumed offset. `Replay.arm_bind_reforward/1` is what makes that true —
  # the claim is only worth `:ok` once the re-forward is actually scheduled.
  # Anything else — no durable log to hold it, or no channel that can ever bind —
  # is refused, so the journal can never claim an input the system dropped.
  defp verdict(_state, nil), do: {:ok, :absorbed}
  defp verdict(_state, :refused), do: {:error, :refused}

  defp verdict(%Overwatch{} = state, payload) do
    case Overwatch.deliver(state, payload) do
      :ok -> :ok
      # Undelivered is undelivered, whatever the channel could not carry it
      # for — an unbound stream, or a send buffer that never drained under a
      # held sender. Both leave the payload where it was and both are answered
      # the same way, so the reason belongs in the channel's own reporting.
      {:error, _reason} -> undeliverable(state)
    end
  end

  defp undeliverable(%Overwatch{} = state) do
    if Replay.floor_reporter(state) && Replay.arm_bind_reforward(state) == :armed,
      do: :ok,
      else: {:error, :not_ready}
  end

  # Liveness bookkeeping for one ingress message: always an io_in, then either
  # filtered (on_input returned nil) or forwarded to the channel.
  defp record_io(ref, payload) do
    LivenessCounters.bump_operator(ref, :io_in_received)
    LivenessCounters.bump_operator(ref, if(is_nil(payload), do: :filtered_nil, else: :forwarded))
  end
end
