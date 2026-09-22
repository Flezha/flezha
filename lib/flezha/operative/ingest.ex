defmodule Flezha.Operative.Ingest do
  @moduledoc """
  The operative's input path: type a payload, run the `:on_input` chain, count
  it, make it durable, and forward it to the bound stream channel.

  Every way in — the `:io_in` cast, the blocking `:io_in` call, the confirming
  `:ingest` call, a suspended instance's deferred append and a spawn's seed —
  runs exactly `ingest/4`.

  A durable append is asynchronous: the payload is handed to the log, the entry
  point's reply stays owed, and the log's answer (`journal_reply/2`) forwards
  the input and settles it once the record is synced.

  Invariant: nothing else journals input, which is what lets the correction
  transaction assume none can arrive while it runs.
  """

  alias Flezha.Operative.Migration
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Plugins
  alias Flezha.Operative.Replay
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

  @typedoc """
  Who is owed the outcome: nobody for a cast, or the entry point and its
  caller. The blocking `:io_in` call acts on accepted vs parked and nothing
  finer, so it is told `:ok`; the confirming `:ingest` call is told the verdict.
  """
  @type reply_to :: :cast | {:io_in | :ingest, GenServer.from()}

  @doc """
  Ingest one inbound message. Answers the payload to forward, or `:pending`
  when the durable input log holds it — the log's answer comes back to this
  process as a message for `journal_reply/2`.
  """
  @spec ingest(Overwatch.t(), binary(), map(), reply_to()) ::
          {payload() | :pending, Overwatch.t()}
  def ingest(%Overwatch{} = state, message, metadata, reply_to) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = Plugins.transform_io(state, :on_input, message, metadata)
    record_io(state.liveness_ref, payload)
    journal_input(state, payload, reply_to)
  end

  @doc """
  The one ingest-and-report path behind every entry point. Answers what the
  entry point is told, or `:pending` when the reply is owed until the log
  answers — the caller then returns `{:noreply, state}` and `journal_reply/2`
  settles it.

  A parked operative is refused by the callers before it reaches here — each owes
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
      {:pending, state} -> {:pending, state}
      {_filtered, state} -> {told(reply_to, {:ok, :suspended}), Migration.rehydrate(state)}
    end
  end

  def confirming_ingest(%Overwatch{} = state, message, metadata, reply_to) do
    case ingest(state, message, metadata, reply_to) do
      {:pending, state} -> {:pending, state}
      {payload, state} -> {told(reply_to, verdict(state, payload)), state}
    end
  end

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
  # (`durable-input-log`): a stream on the effectively-once tier or a durable
  # workflow. The payload is appended to the instance's input log and the
  # answer stamps `flz.offset`, which the SDK threads into the effect record or
  # the iteration commit as the consumed offset. A `flz.msg_id` the log still
  # retains is absorbed (`nil` — nothing is forwarded), and an append that
  # fails answers `:refused` and is NOT forwarded either: the contract is
  # append-before-forward, so a lost frame the sender can retry beats an
  # undurable one it believes was accepted.
  #
  # Every other operative passes through untouched (cost-free default).
  @doc """
  Journal a suspended instance's timer wake: an empty record marked
  `flz.wake`, appended like any input, so a racing message and the deadline
  collapse into one rehydration, and forwarded by the rehydrating bind with
  the rest of the tail.
  """
  @spec journal_wake(Overwatch.t()) :: :ok
  def journal_wake(%Overwatch{} = state) do
    _ = InputLog.append(Overwatch.operative_class(state), "", %{"flz.wake" => "timer"})
    :ok
  end

  @doc """
  Whether the next input is the instance's first: always for an instance
  without a durable workflow's log, and for one whose log has journaled
  nothing. A log that holds records makes a bind a restart, not a spawn.
  """
  @spec first_input?(Overwatch.t()) :: boolean()
  def first_input?(%Overwatch{durable_workflow: true} = state),
    do: match?({:ok, _floor, -1}, InputLog.tail(Overwatch.operative_class(state)))

  def first_input?(%Overwatch{}), do: true

  defp journal_input(state, nil, _reply_to), do: {nil, state}

  defp journal_input(%Overwatch{} = state, payload, reply_to) do
    if durable_input?(state),
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
        Overwatch.operative_class(state),
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
      %{operative_id: state.id, offset: offset}
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
    reply(reply_to, told(reply_to, {:ok, :suspended}))
    Migration.rehydrate(state)
  end

  defp settle(%Overwatch{} = state, reply_to, payload) do
    reply(reply_to, told(reply_to, verdict(state, payload)))
    state
  end

  defp reply(:cast, _reply), do: :ok
  defp reply({_entry_point, from}, reply), do: GenServer.reply(from, reply)

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
    if durable_input?(state) and Replay.arm_bind_reforward(state) == :armed,
      do: :ok,
      else: {:error, :not_ready}
  end

  defp durable_input?(%Overwatch{effectively_once: eo, durable_workflow: db}), do: eo or db

  # Liveness bookkeeping for one ingress message: always an io_in, then either
  # filtered (on_input returned nil) or forwarded to the channel.
  defp record_io(ref, payload) do
    LivenessCounters.bump_operative(ref, :io_in_received)
    LivenessCounters.bump_operative(ref, if(is_nil(payload), do: :filtered_nil, else: :forwarded))
  end
end
