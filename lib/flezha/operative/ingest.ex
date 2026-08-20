defmodule Flezha.Operative.Ingest do
  @moduledoc """
  The operative's input path: type a payload, run it through the `:on_input`
  plugin chain, count it, make it durable, and forward it to the bound stream
  channel.

  Every way in — the `:io_in` cast, the blocking `:io_in` call, the confirming
  `:ingest` call, a suspended instance's deferred append, and a
  procedure-requested spawn's seed — runs exactly `ingest/3`. That single path is
  what makes the append-before-forward contract hold and what lets the
  correction transaction assume no input can be journaled while it runs.

  Runs in the `Flezha.Operative.Overwatch` process and changes none of its state:
  the only state a delivery can move is the suspension it wakes, which it does by
  calling back into `Flezha.Operative.Lifecycle`. The `:on_input` chain itself is
  run by `Flezha.Operative.Plugins`, which owns the extension table.
  """

  alias Flezha.Operative.Lifecycle
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Plugins
  alias Flezha.Runner.StreamChannel
  alias Flezha.State.InputLog
  alias Flezha.State.ProcInbox
  alias Flezha.Transport.LivenessCounters

  require Logger

  @typedoc """
  What `ingest/3` answers with, and the only thing a caller may forward.

  `nil` ⇒ filtered by a plugin or absorbed as a duplicate; `:refused` ⇒ the
  durable log rejected the append. Neither is forwarded; only the confirming
  entry point tells them apart.
  """
  @type payload :: {binary(), map()} | nil | :refused

  @doc """
  Ingest one inbound message. The payload it answers is the one to forward.
  """
  @spec ingest(Overwatch.t(), binary(), map()) :: payload()
  def ingest(%Overwatch{} = state, message, metadata) do
    metadata = Map.put_new(metadata, "input_type", state.default_input_type)
    payload = Plugins.transform_io(state, :on_input, message, metadata)
    record_io(state.liveness_ref, payload)
    journal_input(state, payload)
  end

  @doc """
  Same ingest, but the forward is deferred to the rehydrating bind, which
  delivers the whole undelivered inbox tail.
  """
  @spec ingest_suspended(Overwatch.t(), binary(), map()) :: Overwatch.t()
  def ingest_suspended(%Overwatch{} = state, message, metadata) do
    _ = ingest(state, message, metadata)
    Lifecycle.rehydrate(state)
  end

  @doc """
  The one ingest-and-report path behind both blocking entry points: `{:io_in,
  _, _}`, which reports coarsely, and `{:ingest, _, _}`, which reports the
  verdict.

  A parked operative is refused by the callers before it reaches here — each owes
  its own reply for that.
  """
  @spec confirming_ingest(Overwatch.t(), binary(), map()) :: {term(), Overwatch.t()}
  def confirming_ingest(%Overwatch{suspended: true} = state, message, metadata) do
    {{:ok, :suspended}, ingest_suspended(state, message, metadata)}
  end

  def confirming_ingest(%Overwatch{} = state, message, metadata) do
    verdict =
      case ingest(state, message, metadata) do
        nil -> {:ok, :absorbed}
        :refused -> {:error, :refused}
        payload -> confirm_forward(state, payload)
      end

    {verdict, state}
  end

  @doc """
  Forward an ingested payload to the bound stream channel, fire-and-forget.
  """
  @spec forward(Overwatch.t(), payload()) :: :ok
  def forward(%Overwatch{} = state, {_message, _metadata} = payload),
    do: Overwatch.deliver(state, payload)

  def forward(%Overwatch{}, _absorbed_or_refused), do: :ok

  # A durable target has already journaled the payload, so an unbound stream is
  # not a delivery failure — the bind delivers it. A target with no durable log
  # has nowhere to hold it, so the sender is told to retry.
  defp confirm_forward(%Overwatch{} = state, payload) do
    case forward_input(state, payload) do
      :ok -> :ok
      {:error, :no_stream} -> if durable_input?(state), do: :ok, else: {:error, :not_ready}
    end
  end

  defp durable_input?(%Overwatch{effectively_once: eo, durable_workflow: db}), do: eo or db

  # An unbound or absent stream is counted and emitted by `StreamChannel.io_in/3`
  # itself, so the reconciler-detected stall is observable (the probe rebinds
  # the stream).
  defp forward_input(%Overwatch{} = state, payload),
    do: StreamChannel.io_in(state.channel, state.id, payload)

  # Durable ingest point — the one place an inbound payload is made durable
  # BEFORE it is forwarded, for both durability tiers:
  #
  #   * effectively-once (`durable-input-log`): appended to the operative's
  #     input log and stamped `flz.offset`, which the SDK threads into the
  #     effect record;
  #   * durable workflow (`workflow-durable-suspension`): appended to the
  #     instance's `inbox.wal` and stamped `flz.inbox_offset`, which the SDK
  #     threads into the iteration commit as the consumed offset. A duplicate
  #     dedup id in the undelivered window is absorbed (`nil` — nothing is
  #     forwarded), and an append that fails answers `:refused` and is NOT
  #     forwarded either: the contract is append-before-forward, so a lost frame
  #     the sender can retry beats an undurable one it believes was accepted.
  #
  # Every other operative passes through untouched (cost-free default).
  @doc false
  def journal_input(_state, nil), do: nil

  def journal_input(%Overwatch{effectively_once: true} = state, {message, metadata}) do
    {:ok, offset} = InputLog.append(Overwatch.operative_class(state), message, metadata)
    {message, Map.put(metadata, "flz.offset", Integer.to_string(offset))}
  end

  def journal_input(%Overwatch{durable_workflow: true} = state, {message, metadata})
      when is_binary(message) do
    case ProcInbox.append(state.id, message, metadata) do
      {:ok, offset} ->
        {message, Map.put(metadata, "flz.inbox_offset", Integer.to_string(offset))}

      {:duplicate, offset} ->
        :telemetry.execute(
          [:flezha, :state, :inbox_duplicate],
          %{count: 1},
          %{operative_id: state.id, offset: offset}
        )

        nil

      {:error, reason} ->
        Logger.error(
          "[Overwatch] durable-inbox append failed for #{state.id}: #{inspect(reason)} — " <>
            "input NOT forwarded (append-before-forward)"
        )

        :refused
    end
  end

  def journal_input(_state, payload), do: payload

  # Liveness bookkeeping for one ingress message: always an io_in, then either
  # filtered (on_input returned nil) or forwarded to the channel.
  defp record_io(ref, payload) do
    LivenessCounters.bump_operative(ref, :io_in_received)
    LivenessCounters.bump_operative(ref, if(is_nil(payload), do: :filtered_nil, else: :forwarded))
  end
end
