defmodule Flezha.State.WalWriter.RemoteGate do
  @moduledoc """
  The `durable: remote` gate on a flow Cell's writer: what an fsync made
  durable is acknowledged only once the remote durability tier holds it.

  A writer acknowledges a synced batch in two ways: it drains the batch's
  credit to the stream that handed the records over, which also confirms the
  instance's processed prefix (its input-log floor), and it lets the outbox
  dispatch the batch's effect records: their outputs, their parks and their
  consumed offsets. Behind this gate both wait, in sync order, for a ship of
  the Cell and its input log to reach the manifest.

  A batch is a commit point when its records acknowledge input: under
  `ack: checkpoint` only a batch holding an effect record that carries a
  consumed offset (the interval's close), under every other ack any batch. A
  batch that is no commit point and waits behind none is acknowledged at once,
  so a checkpoint interval ships once. One ship is in flight per writer; the
  batches synced meanwhile wait for the next, which covers them all. A failed
  ship is retried on the tier's backoff ladder while the acknowledgements
  wait; the shipper's failure record is what the RPO alarm reads.

  A writer without the gate (`remote: nil`) acknowledges each batch the moment
  it is synced: every class that does not declare `durable: remote`.
  """

  alias Flezha.Config.Backoff
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Process.Timer
  alias Flezha.State.{DurabilityTier, Paths}
  alias Flezha.State.WalWriter.{Admission, Settlement}

  # `held`: synced batches not yet acknowledged, `{seq, credit}` oldest first.
  # `shipping`: `nil`, `{:to, seq}` while a ship covering `seq` is in flight,
  # or `:cooling` while a failed one waits for its retry. `generation` names
  # the lineage a ship was started for, so the answer to a ship a realign or a
  # rewind outdated changes nothing.
  defstruct checkpoint?: false,
            held: :queue.new(),
            shipping: nil,
            failures: 0,
            generation: 0

  @type t :: %__MODULE__{}

  @doc """
  The gate for `cell_id`'s writer, or `nil`: a flow Cell of a class that keeps
  its work `durable: remote`, on a controller with a durability backend. A
  steps Cell commits at its stage barrier, which the tier gates already, and
  the reserved identities (a task journal, an input log) acknowledge nothing.
  """
  @spec new(Paths.cell_id(), :record | :stage) :: t() | nil
  def new({operator_class, _sh, key}, :record) do
    if key not in Paths.reserved_cell_keys() and DurabilityTier.sync_class?(operator_class),
      do: %__MODULE__{checkpoint?: ClassRegistry.ack_checkpoint?(operator_class)}
  end

  def new(_cell_id, :stage), do: nil

  @doc """
  Everything appended is synced: acknowledge it, at once without the gate, or
  behind the remote ship with it.
  """
  def synced(%{remote: nil} = state),
    do:
      acknowledge(%{state | credit_pending: %{}}, state.last_persisted_seq, state.credit_pending)

  def synced(state), do: state |> hold() |> advance()

  @doc """
  A drain outside an fsync (a prefix cut's rewrite): released at once without
  the gate, held behind the earlier batches with it. Every drain of a gated
  writer reaches its stream in the order its records were written, because
  the stream counts drained bytes per writer to confirm its markers.
  """
  def drained(%{remote: nil} = state), do: Admission.flush_credit_pending(state)
  def drained(state), do: state |> hold() |> advance()

  @doc """
  A record that never occupies the buffer (a dedup drop, a gap, a shed): its
  bytes drain at once, unless the gate holds earlier drains, which they follow.
  """
  def released(%{remote: %__MODULE__{held: held} = gate} = state, opts) do
    cond do
      not :queue.is_empty(held) ->
        {{:value, {seq, credit}}, rest} = :queue.out_r(held)
        batch = {seq, Admission.add_credit(credit, opts)}
        %{state | remote: %{gate | held: :queue.in(batch, rest)}}

      state.credit_pending != %{} ->
        Admission.track_credit(state, opts)

      true ->
        release_now(state, opts)
    end
  end

  def released(state, opts), do: release_now(state, opts)

  defp release_now(state, opts) do
    Admission.release_credit(state, opts)
    state
  end

  @doc """
  The writer is going, or its lineage was cut: a realign keeps nothing, a
  rewind keeps the records through its boundary. Held batches through
  `keep_through` stay held behind a fresh ship; every other held or pending
  drain is released, freeing the stream's window without counting toward its
  markers, since the backend may never hold those records. The ship in flight
  is forgotten. Without the gate this is the plain credit flush.
  """
  def reset(state, keep_through \\ -1)
  def reset(%{remote: nil} = state, _keep_through), do: Admission.flush_credit_pending(state)

  def reset(%{remote: gate} = state, keep_through) do
    state = if state.credit_pending == %{}, do: state, else: hold(state)

    {kept, gone} =
      state.remote.held |> :queue.to_list() |> Enum.split_with(&(elem(&1, 0) <= keep_through))

    Enum.each(gone, fn {_seq, credit} ->
      Admission.drain_credit(state.cell_id, credit, :credit_released)
    end)

    fresh = %__MODULE__{
      checkpoint?: gate.checkpoint?,
      generation: gate.generation + 1,
      held: :queue.from_list(kept)
    }

    state = %{state | remote: fresh, timers: Timer.cancel(state.timers, :remote_retry)}

    if kept == [] do
      :ok = DurabilityTier.end_wait(state.cell_id)
      state
    else
      ship(state)
    end
  end

  @doc """
  The outbox's durable cursor once a reopen or a rewind makes `seq` durable
  here: `seq` without the gate, and with it no further than `acknowledged`,
  what the gate had already let through, until a ship covers the rest.
  """
  def durable_through(%{remote: nil}, seq, _acknowledged), do: seq
  def durable_through(_state, seq, acknowledged), do: min(acknowledged, seq)

  @doc "The ship started for `generation` answered."
  def shipped(%{remote: %{generation: generation} = gate} = state, generation, :ok) do
    {:to, seq} = gate.shipping
    {due, held} = Enum.split_while(:queue.to_list(gate.held), fn {s, _} -> s <= seq end)
    state = %{state | remote: %{gate | held: :queue.from_list(held), shipping: nil, failures: 0}}

    due
    |> Enum.reduce(state, fn {s, credit}, acc -> acknowledge(acc, s, credit) end)
    |> advance()
  end

  def shipped(%{remote: %{generation: generation} = gate} = state, generation, {:error, _}) do
    failures = gate.failures + 1
    delay = Backoff.arm(:durability_ship, failures, state.cell_id)

    %{
      state
      | remote: %{gate | shipping: :cooling, failures: failures},
        timers: Timer.arm(state.timers, :remote_retry, delay, :remote_retry)
    }
  end

  def shipped(state, _outdated_generation, _result), do: state

  @doc "The backoff after a failed ship elapsed: ship again."
  def retry(%{remote: %{shipping: :cooling} = gate} = state),
    do: advance(%{state | remote: %{gate | shipping: nil}})

  def retry(state), do: state

  # ---------- the held batches ----------

  defp hold(%{remote: gate} = state) do
    batch = {state.last_persisted_seq, state.credit_pending}
    %{state | credit_pending: %{}, remote: %{gate | held: :queue.in(batch, gate.held)}}
  end

  # Acknowledge the leading batches that are no commit point; at the first that
  # is, ship everything held. Nothing moves while a ship is in flight or
  # cooling off: a later batch never overtakes an earlier one.
  defp advance(%{remote: %{shipping: nil} = gate} = state) do
    case :queue.out(gate.held) do
      {:empty, _} ->
        state

      {{:value, {seq, credit}}, rest} ->
        if commit_point?(state, seq),
          do: ship(state),
          else: advance(acknowledge(%{state | remote: %{gate | held: rest}}, seq, credit))
    end
  end

  defp advance(state), do: state

  defp commit_point?(%{remote: %{checkpoint?: false}} = state, seq),
    do: seq > state.outbox_durable_seq

  defp commit_point?(state, seq) do
    acked = state.outbox_durable_seq

    state.pending_outbox
    |> :queue.to_list()
    |> Enum.any?(&(&1.seq > acked and &1.seq <= seq and is_integer(&1.consumed_offset)))
  end

  defp ship(%{remote: gate} = state) do
    {:value, {last, _credit}} = :queue.peek_r(gate.held)
    :ok = DurabilityTier.commit_async(state.cell_id, gate.generation)
    %{state | remote: %{gate | shipping: {:to, last}}}
  end

  # The batch through `seq` is acknowledged: its credit drained, its effect
  # records dispatchable. Without the gate `seq` is the last persisted seq,
  # which the durable cursor never passes.
  defp acknowledge(state, seq, credit) do
    Admission.drain_credit(state.cell_id, credit)

    Settlement.try_dispatch_outbox(%{
      state
      | outbox_durable_seq: max(state.outbox_durable_seq, seq)
    })
  end
end
