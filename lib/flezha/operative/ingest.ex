defmodule Flezha.Operative.Ingest do
  @moduledoc """
  The operative's input path: type a payload, run it through the `:on_input`
  plugin chain, count it, make it durable, and forward it to the bound stream
  channel.

  Every way in — the `:io_in` cast, the blocking `:io_in` call, the confirming
  `:ingest` call, a suspended instance's deferred append, and a
  procedure-requested spawn's seed — runs exactly `ingest/4`. That single path is
  what makes the append-before-forward contract hold and what lets the
  correction transaction assume no input can be journaled while it runs.

  A durable append — the effectively-once input log, the durable workflow's
  inbox — is asynchronous: the payload is handed to the log, the entry point's
  reply stays owed, and the log's answer (`journal_reply/2`), which arrives
  once the record is synced, forwards the input and settles the reply. So a
  zero-credit park lands on the source's call — the Overwatch keeps running the
  output chains whose confirmations mint the credit — and the batch sync every
  record waits on is never waited on here.

  Runs in the `Flezha.Operative.Overwatch` process. The only state it moves is
  the in-flight request collection and the suspension a delivery wakes, which
  it does by calling back into `Flezha.Operative.Lifecycle`. The `:on_input`
  chain itself is run by `Flezha.Operative.Plugins`, which owns the extension
  table.
  """

  alias Flezha.Operative.Lifecycle
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Plugins
  alias Flezha.State.InputLog
  alias Flezha.State.ProcInbox
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
  when the effectively-once journal holds it — the log's answer comes back to
  this process as a message for `journal_reply/2`.
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
  # the whole undelivered inbox tail; the journal's answer reports the
  # suspension and wakes the instance (`settle/3`). An input a plugin filtered
  # journals nothing, and is answered — and wakes — at once.
  def confirming_ingest(%Overwatch{suspended: true} = state, message, metadata, reply_to) do
    case ingest(state, message, metadata, reply_to) do
      {:pending, state} -> {:pending, state}
      {_filtered, state} -> {told(reply_to, {:ok, :suspended}), Lifecycle.rehydrate(state)}
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
  # BEFORE it is forwarded, for both durability tiers, asynchronously:
  #
  #   * effectively-once (`durable-input-log`): appended to the operative's
  #     input log; the answer stamps `flz.offset`, which the SDK threads into
  #     the effect record;
  #   * durable workflow (`workflow-durable-suspension`): appended to the
  #     instance's `inbox.wal` and stamped `flz.inbox_offset`, which the SDK
  #     threads into the iteration commit as the consumed offset. A duplicate
  #     dedup id in the undelivered window is absorbed (`nil` — nothing is
  #     forwarded), and an append that fails answers `:refused` and is NOT
  #     forwarded either: the contract is append-before-forward, so a lost frame
  #     the sender can retry beats an undurable one it believes was accepted.
  #
  # Every other operative passes through untouched (cost-free default).
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
    reqids = state.journal_reqids

    reqids =
      if state.effectively_once,
        do:
          InputLog.append_async(
            Overwatch.operative_class(state),
            message,
            metadata,
            reqids,
            label
          ),
        else: ProcInbox.append_async(state.id, message, metadata, reqids, label)

    %{state | journal_reqids: reqids}
  end

  # What the log's answer makes of the payload: forwardable and stamped with
  # its durable offset, `nil` for a duplicate the inbox absorbed, `:refused`
  # for an append the log rejected.
  defp journaled(%Overwatch{effectively_once: true}, {message, metadata}, {:ok, offset}),
    do: {message, stamp(metadata, "flz.offset", offset)}

  defp journaled(%Overwatch{}, {message, metadata}, {:ok, seq}),
    do: {message, stamp(metadata, "flz.inbox_offset", ProcInbox.offset(seq))}

  defp journaled(%Overwatch{} = state, _payload, {:duplicate, seq}) do
    :telemetry.execute(
      [:flezha, :state, :inbox_duplicate],
      %{count: 1},
      %{operative_id: state.id, offset: ProcInbox.offset(seq)}
    )

    nil
  end

  defp journaled(%Overwatch{} = state, _payload, {:error, reason}) do
    Logger.error(
      "[Overwatch] durable-inbox append failed for #{state.id}: #{inspect(reason)} — " <>
        "input NOT forwarded (append-before-forward)"
    )

    :refused
  end

  defp stamp(metadata, key, offset), do: Map.put(metadata, key, Integer.to_string(offset))

  # A journaled payload's forward and the reply it owed, once the log answered.
  # A suspended instance forwards nothing: the payload waits in the inbox for the
  # rehydrating bind, which is what the reply reports and what the append wakes.
  defp settle(%Overwatch{suspended: true} = state, reply_to, payload)
       when payload != :refused do
    reply(reply_to, told(reply_to, {:ok, :suspended}))
    Lifecycle.rehydrate(state)
  end

  defp settle(%Overwatch{} = state, reply_to, payload) do
    reply(reply_to, told(reply_to, verdict(state, payload)))
    state
  end

  defp reply(:cast, _reply), do: :ok
  defp reply({_entry_point, from}, reply), do: GenServer.reply(from, reply)

  defp told({:io_in, _from}, _verdict), do: :ok
  defp told(_reply_to, verdict), do: verdict

  # Forward what the journal answered with, and say what became of it. A durable
  # target has already journaled the payload, so an unbound stream is not a
  # delivery failure — the bind delivers it. A target with no durable log has
  # nowhere to hold it, so the sender is told to retry.
  defp verdict(_state, nil), do: {:ok, :absorbed}
  defp verdict(_state, :refused), do: {:error, :refused}

  defp verdict(%Overwatch{} = state, payload) do
    case Overwatch.deliver(state, payload) do
      :ok -> :ok
      {:error, :no_stream} -> if durable_input?(state), do: :ok, else: {:error, :not_ready}
    end
  end

  defp durable_input?(%Overwatch{effectively_once: eo, durable_workflow: db}), do: eo or db

  # Liveness bookkeeping for one ingress message: always an io_in, then either
  # filtered (on_input returned nil) or forwarded to the channel.
  defp record_io(ref, payload) do
    LivenessCounters.bump_operative(ref, :io_in_received)
    LivenessCounters.bump_operative(ref, if(is_nil(payload), do: :filtered_nil, else: :forwarded))
  end
end
