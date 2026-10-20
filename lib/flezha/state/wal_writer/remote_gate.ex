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
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Process.{DurableTasks, Timer, Watch}
  alias Flezha.State.{DurabilityTier, Paths, WriterRegistry}
  alias Flezha.State.DurabilityTier.{Artifact, Shipper}
  alias Flezha.State.WalWriter.{Admission, Boot, Settlement}

  # The credit of records a closed writer left on disk unshipped, one row per
  # held batch under its Cell id, owned by `Flezha.State.Supervisor` so it
  # outlives every writer.
  @owed :flezha_remote_gate_owed

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
  def new({relay_class, _sh, key}, :record) do
    if key not in Paths.reserved_cell_keys() and DurabilityTier.sync_class?(relay_class),
      do: %__MODULE__{checkpoint?: ClassRegistry.ack_checkpoint?(relay_class)}
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
  A drain outside an fsync (a prefix cut's rewrite): drained at once without
  the gate, held behind the earlier batches with it. Every drain of a gated
  writer reaches its stream in the order its records were written, because
  the stream counts drained bytes per writer to confirm its markers.
  """
  def drained(%{remote: nil} = state), do: Admission.flush_credit_pending(state, :credit_drained)
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
  The writer's lineage was cut, or it is going without its pending records
  synced: a realign keeps nothing, a rewind keeps the records through its
  boundary. Held batches through `keep_through` stay held behind a fresh ship;
  every other held or pending drain is released, freeing the stream's window
  without counting toward its markers, since the backend may never hold those
  records. The ship in flight is forgotten. Without the gate every pending
  drain is released.
  """
  def reset(state, keep_through \\ -1)

  def reset(%{remote: nil} = state, _keep_through),
    do: Admission.flush_credit_pending(state, :credit_released)

  def reset(state, keep_through),
    do: let_go(state, keep_through, &Admission.drain_credit(&1, &2, :credit_released))

  @doc """
  The writer is going with every record it took synced on this disk: what it
  holds for them is not released but kept owed under the Cell, for the next
  writer's boot ship to drain (`take_owed/1`), so no marker confirms a record
  before the backend holds it. The gate lets go as `reset/2` does. Without the
  gate the synced records drain at once.
  """
  def close(%{remote: nil} = state), do: drained(state)
  def close(state), do: let_go(state, -1, &:ets.insert(@owed, {&1, &2}))

  # Hand each held or pending drain after `keep_through` to `dispose`, keep
  # the rest held behind a fresh ship, and forget the ship in flight.
  defp let_go(%{remote: gate} = state, keep_through, dispose) do
    state = if state.credit_pending == %{}, do: state, else: hold(state)

    {kept, gone} =
      state.remote.held |> :queue.to_list() |> Enum.split_with(&(elem(&1, 0) <= keep_through))

    for {_seq, credit} <- gone, credit != %{}, do: dispose.(state.cell_id, credit)

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

  @doc "The credit a closed writer of `cell_id` left owed, taken: a booting writer drains it."
  def take_owed(cell_id) do
    @owed
    |> :ets.take(cell_id)
    |> Enum.reduce(%{}, fn {_, credit}, acc -> Map.merge(acc, credit, fn _, a, b -> a + b end) end)
  end

  @doc "The Cell's records left the disk: the credit owed for them is released."
  def release_owed(cell_id),
    do: Admission.drain_credit(cell_id, take_owed(cell_id), :credit_released)

  @doc """
  `relay_class` left this node, so no writer of its Cells boots here to drain
  what they left owed: one ship each, off the caller, drains it once the
  backend holds the records, or releases it when that ship fails.
  """
  def ship_owed(relay_class) do
    for cell_id <- :ets.select(@owed, [{{{relay_class, :_, :_}, :_}, [], [{:element, 1, :"$_"}]}]),
        owed = take_owed(cell_id),
        owed != %{} do
      DurableTasks.start(fn ->
        tag =
          if DurabilityTier.commit_sync(cell_id) == :ok,
            do: :credit_drained,
            else: :credit_released

        Admission.drain_credit(cell_id, owed, tag)
      end)
    end

    :ok
  end

  @doc """
  Wait until each gated writer `identity` owns drains a credit behind what it held, or dies, and
  each writer-less gated Cell past its shipped cut ships; the disk is listed before the writers.
  """
  @spec await_held(binary() | nil) :: :ok
  def await_held(identity) do
    unshipped = unshipped(identity)
    credit = %{channel: self(), session: make_ref(), wire_size: 1}

    {owed, watch} =
      for {{relay_class, _, _} = cell_id, pid} <- WriterRegistry.cell_writers(),
          DurabilityTier.sync_class?(relay_class),
          match?({:ok, _, ^identity}, ClassRegistry.owner(relay_class)),
          reduce: {%{}, Watch.new()} do
        {owed, watch} ->
          GenServer.cast(pid, {:fence, credit})
          {Map.put(owed, cell_id, pid), Watch.watch(watch, pid, cell_id)}
      end

    Flezha.Parallel.each(Enum.reject(unshipped, &is_map_key(owed, &1)), &ship_until_landed/1)
    await_drains(owed, watch, credit.session)
  end

  # A Cell with a writer running is that writer's to drain: asking the Shipper
  # about it would wait out the very ship the writer holds, past the writer's end.
  defp unshipped(identity) do
    running = MapSet.new(WriterRegistry.cell_writers(), &elem(&1, 0))

    for class <- Paths.relay_classes(),
        match?({:ok, _, ^identity}, ClassRegistry.owner(class)),
        cell_id <- Paths.cells_of(class),
        not MapSet.member?(running, cell_id),
        new(cell_id, Boot.commit_unit(cell_id)),
        {_lo, top} <- [Artifact.seq_bounds({:cell, cell_id})],
        top > Shipper.shipped_through({:cell, cell_id}),
        do: cell_id
  end

  defp ship_until_landed(cell_id, attempt \\ 1) do
    with {:error, _} <- DurabilityTier.commit_sync(cell_id) do
      Backoff.wait(:durability_ship, attempt, cell_id)
      ship_until_landed(cell_id, attempt + 1)
    end
  end

  defp await_drains(owed, _watch, _ref) when owed == %{}, do: :ok

  defp await_drains(owed, watch, ref) do
    receive do
      {_drained_or_released, ^ref, _bytes, cell_id} ->
        await_drains(Map.delete(owed, cell_id), watch, ref)

      {:DOWN, monitor, :process, _pid, _reason} ->
        case Watch.down(watch, monitor) do
          {:unknown, watch} -> await_drains(owed, watch, ref)
          {cell_ids, watch} -> await_drains(Map.drop(owed, cell_ids), watch, ref)
        end
    end
  end

  @doc "Create the owed-credit table; its owner must outlive every writer."
  def new_owed_table(), do: :ets.new(@owed, [:duplicate_bag, :public, :named_table])

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
