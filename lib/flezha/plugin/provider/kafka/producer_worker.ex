defmodule Flezha.Plugin.Provider.Kafka.ProducerWorker do
  @moduledoc """
  Per-`(plugin_id, topic, partition)` GenServer that owns a bounded outbound
  queue, runs linger-based batching, retries transient broker errors, and
  drives idempotent producer-id semantics.

  Issues real Produce/InitProducerId/CreateTopics requests through a
  `KafkaEx.Client` (Kayrock-based) shared worker pid acquired from
  `Flezha.Plugin.Provider.Kafka.WorkerRegistry`.

  Up to `:max_in_flight` Produce requests are outstanding per partition at a
  time: the worker sends them itself through the client's asynchronous API and
  reads the outcomes off its own mailbox. Metadata refreshes and topic creation
  are synchronous client calls and still run in short-lived spawned processes
  so the mailbox stays drainable.
  """

  use GenServer

  require Logger

  alias Flezha.Plugin.Provider.Kafka
  alias Flezha.Plugin.Provider.Kafka.WorkerRegistry
  alias Flezha.State.Outbox
  alias KafkaEx.Client, as: KafkaClient
  alias KafkaEx.Client.NodeSelector

  @retriable [
    :no_broker,
    :not_leader_for_partition,
    :network_exception,
    :request_timed_out,
    :not_enough_replicas,
    :not_enough_replicas_after_append,
    :replica_not_available,
    :leader_not_available,
    :broker_not_available,
    :closed,
    :correlation_mismatch
  ]

  @permanent [
    :invalid_topic_exception,
    :invalid_topic,
    :record_list_too_large,
    :message_size_too_large,
    :offset_metadata_too_large,
    :topic_authorization_failed,
    :cluster_authorization_failed
  ]

  # The batch is genuinely stored already (a retry that landed twice). Safe to ack.
  @idempotence_already_delivered [:duplicate_sequence_number]

  # The producer session is fenced/desynced and the batch was NOT stored. The
  # session must be re-initialised and the batch resent — acking it would drop data.
  @idempotence_fenced [:out_of_order_sequence_number, :invalid_producer_epoch]

  # Consecutive `fail_batch/3` occurrences (a batch that exhausted its
  # retries / delivery timeout) before we force a fresh broker connection.
  # A clean broker restart leaves the shared client alive but produce stalls
  # forever; refreshing the client is the only path back. Reset to 0 on any
  # successful batch.
  @default_stall_threshold 3

  # Error reasons that mean the shared client has NO working broker connection
  # (the classic clean-broker-restart wedge). A single batch failing this way
  # is authoritative that the client must be recreated — waiting for
  # `@default_stall_threshold` batches strands a low-traffic producer that may
  # emit only one batch per input burst. Recreate is throttled by
  # `@force_replace_throttle_ms` so a genuinely-down broker is not recreated on
  # every batch.
  @client_wedged [
    :no_broker,
    :broker_not_available,
    :network_exception,
    :leader_not_available,
    :closed,
    :correlation_mismatch
  ]
  @force_replace_throttle_ms 2_000

  def start_link(opts) when is_list(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  def child_spec(opts) do
    plugin_id = Keyword.fetch!(opts, :plugin_id)
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    %{
      id: {__MODULE__, plugin_id, topic, partition},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :transient,
      shutdown: 5_000
    }
  end

  @doc """
  Synchronous enqueue intended for tests, ad-hoc tooling, and explicit-flush
  call sites. Blocks the caller until the message is acknowledged, dropped,
  or the producer's internal `:delivery_timeout_ms` is exhausted. The
  caller's GenServer.call timeout SHOULD exceed `:delivery_timeout_ms` by a
  small buffer; pass `:timeout` explicitly to override.

  Chain plugins MUST use `cast_enqueue/3` instead — `enqueue/4` blocks the
  caller for the duration of one broker round-trip and will saturate the
  host process's mailbox under sustained input above the broker drain rate.
  """
  @spec enqueue(GenServer.server(), binary(), map(), keyword()) ::
          :ok | {:error, term()}
  def enqueue(server, msg, metadata, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 35_000)
    GenServer.call(server, {:enqueue, msg, metadata}, timeout)
  catch
    :exit, {:timeout, _} -> {:error, {:delivery_failed, :enqueue_timeout}}
    :exit, {:noproc, _} -> {:error, {:delivery_failed, :no_producer_worker}}
  end

  @doc """
  Asynchronous, non-blocking enqueue used by the chain dispatch path.
  Returns `:ok` immediately after handing the message off to the worker's
  mailbox. Admission against the bounded queue happens inside the worker;
  overflow is resolved per the configured `:queue_overflow` strategy. Under
  `:block` a cast-admitted record waits for room instead of being dropped —
  the caller is never blocked, the record simply is not admitted yet, which is
  what back-pressures the outbox flush that produced it.

  Permanent delivery failures of cast-admitted records surface through
  `[:flezha, :plugin, :kafka, :produce, :failed]` telemetry tagged with
  `caller: :async`, and — for a delivery-guaranteed record — through the
  outbox park path; there is no caller to reply to.
  """
  @spec cast_enqueue(GenServer.server(), binary(), map()) :: :ok
  def cast_enqueue(server, msg, metadata) do
    GenServer.cast(server, {:enqueue_async, msg, metadata})
  end

  @spec stats(GenServer.server()) :: map()
  def stats(server) do
    GenServer.call(server, :stats)
  end

  @doc """
  Worker-reuse policy for `ProducerSup.ensure_worker/1`: alive AND not
  permanently parked (`last_error` is `{:swap_exhausted, _}` when the
  in-process broker-swap recovery is exhausted and the queue can no longer
  drain). A wedged stats read errs on the side of reuse — only a
  definitively parked/dead worker is reaped.
  """
  @spec reusable?(pid()) :: boolean()
  def reusable?(pid) do
    Process.alive?(pid) and not parked?(pid)
  end

  defp parked?(pid) do
    match?(%{last_error: {:swap_exhausted, _}}, GenServer.call(pid, :stats, 1_000))
  catch
    :exit, _ -> false
  end

  @doc """
  Force a fresh broker connection now, bypassing the stall counter — an
  on-demand recovery for a producer wedged against a healthy-but-unreachable
  broker, without waiting for the next failed batch. (The automatic path is
  `maybe_refresh_broker/2` on batch failure.) No-op for workers that own no
  shared registry seat (test-injected `:worker_pid`).
  """
  @spec refresh_broker(GenServer.server()) :: :ok
  def refresh_broker(server), do: GenServer.cast(server, :refresh_broker)

  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    {worker_pid, registry_ref, registry_owned?} = acquire_worker(opts)

    state = %{
      worker_pid: worker_pid,
      registry_ref: registry_ref,
      registry_owned?: registry_owned?,
      connection_identity: Keyword.get(opts, :connection_identity),
      topic: Keyword.fetch!(opts, :topic),
      partition: Keyword.fetch!(opts, :partition),
      plugin_id: Keyword.fetch!(opts, :plugin_id),
      linger_ms: Keyword.get(opts, :linger_ms, 10),
      max_batch_bytes: Keyword.get(opts, :max_batch_bytes, 1_048_576),
      queue_max: Keyword.get(opts, :queue_max, 10_000),
      queue_overflow: Keyword.get(opts, :queue_overflow) || :block,
      queue_block_timeout_ms: Keyword.get(opts, :queue_block_timeout_ms, 5_000),
      retries: Keyword.get(opts, :retries, 5),
      retry_backoff_ms: Keyword.get(opts, :retry_backoff_ms, 100),
      retry_max_backoff_ms: Keyword.get(opts, :retry_max_backoff_ms, 5_000),
      delivery_timeout_ms: Keyword.get(opts, :delivery_timeout_ms, 30_000),
      dispatch_timeout_ms: Keyword.get(opts, :dispatch_timeout_ms, 10_000),
      required_acks: Keyword.get(opts, :required_acks, -1),
      request_timeout_ms: Keyword.get(opts, :request_timeout_ms, 10_000),
      idempotent: Keyword.get(opts, :idempotent, false),
      max_in_flight: Keyword.get(opts, :max_in_flight, 1),
      auto_create_topic: Keyword.get(opts, :auto_create_topic, true),
      auto_create_partitions: Keyword.get(opts, :auto_create_partitions, 1),
      auto_create_replication_factor: Keyword.get(opts, :auto_create_replication_factor, 2),
      topic_configs: Keyword.get(opts, :topic_configs, []),
      metadata_refresh_timeout_ms: Keyword.get(opts, :metadata_refresh_timeout_ms, 5_000),
      send_fn: Keyword.get(opts, :send_fn, &default_send/3),
      init_producer_id_fn: Keyword.get(opts, :init_producer_id_fn, &default_init_producer_id/2),
      create_topic_fn: Keyword.get(opts, :create_topic_fn, &default_create_topic/4),
      metadata_refresh_fn: Keyword.get(opts, :metadata_refresh_fn, &default_metadata_refresh/2),
      now_fn: Keyword.get(opts, :now_fn, fn -> System.monotonic_time(:millisecond) end),
      wall_now_fn: Keyword.get(opts, :wall_now_fn, fn -> System.system_time(:millisecond) end),
      jitter_fn: Keyword.get(opts, :jitter_fn, fn max -> :rand.uniform(max + 1) - 1 end),
      producer_id: nil,
      producer_epoch: nil,
      sequence: 0,
      queue: :queue.new(),
      queue_size: 0,
      queue_bytes: 0,
      linger_timer: nil,
      # Batches outstanding on the wire, oldest first.
      in_flight: [],
      retry_timer: nil,
      blocked: :queue.new(),
      last_error: nil,
      consecutive_failures: 0,
      dropped_total: 0,
      stall_threshold: Keyword.get(opts, :stall_threshold, @default_stall_threshold),
      last_force_replace_at: nil,
      swap_started_at: nil,
      swap_attempt: 0,
      pending_swap_telemetry: nil
    }

    {:ok, state, {:continue, :maybe_init_producer_id}}
  end

  @impl true
  def handle_continue(:maybe_init_producer_id, %{idempotent: false} = state) do
    {:noreply, emit_pending_swap_telemetry(state) |> maybe_kick_batch()}
  end

  def handle_continue(:maybe_init_producer_id, state) do
    case state.init_producer_id_fn.(state.worker_pid, state.metadata_refresh_timeout_ms) do
      {:ok, %{producer_id: pid, producer_epoch: epoch}} ->
        state = %{state | producer_id: pid, producer_epoch: epoch}
        {:noreply, emit_pending_swap_telemetry(state) |> maybe_kick_batch()}

      {:error, reason} ->
        Logger.warning(
          "#{tag(state)} " <>
            "InitProducerId failed: #{inspect(reason)} — running in non-idempotent mode"
        )

        # Without a producer session nothing keeps a retry inside a window in
        # order, so the fallback narrows it to one as well.
        state = %{state | idempotent: false, max_in_flight: 1, last_error: reason}
        {:noreply, emit_pending_swap_telemetry(state) |> maybe_kick_batch()}
    end
  end

  @impl true
  def handle_call({:enqueue, msg, metadata}, from, state) do
    record = build_record(msg, metadata, from, state.wall_now_fn.())

    case admit_record(state, record, from) do
      {:admitted, state} ->
        {:noreply, maybe_kick_batch(state)}

      {:replied, reply, state} ->
        {:reply, reply, state}

      {:blocked, state} ->
        {:noreply, state}
    end
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       queue_depth: state.queue_size,
       in_flight: in_flight_size(state),
       dropped_total: state.dropped_total,
       last_error: state.last_error,
       consecutive_failures: state.consecutive_failures
     }, state}
  end

  def handle_cast(:refresh_broker, %{registry_ref: nil} = state), do: {:noreply, state}

  def handle_cast(:refresh_broker, state) do
    Logger.info(
      "#{tag(state)} " <>
        "convergence requested broker refresh; forcing fresh connection"
    )

    {:noreply, force_replaced(state)}
  end

  @impl true
  def handle_cast({:enqueue_async, msg, metadata}, state) do
    record = build_record(msg, metadata, :async, state.wall_now_fn.())

    case admit_record(state, record, :async) do
      {:admitted, state} -> {:noreply, maybe_kick_batch(state)}
      {:blocked, state} -> {:noreply, state}
      {:dropped_async, state} -> {:noreply, state}
    end
  end

  @impl true
  def handle_info(:flush_batch, state) do
    {:noreply, maybe_send_batch(%{state | linger_timer: nil})}
  end

  # A response for a dispatch the window no longer owns (a resend already went
  # out, or the batch was rewound) is ignored — the same rule as the fresh
  # handle per dispatch it rests on.
  def handle_info({:kafka_ex_response, ref, result}, state) do
    {:noreply, with_dispatch(state, ref, &handle_batch_result(&1, &2, result))}
  end

  # The send neither returned nor crashed in time (e.g. the shared client is
  # wedged on a dead socket after a broker restart). Treat the stranded dispatch
  # as a retriable failure so the batch advances toward fail_batch / broker
  # refresh instead of stalling the worker forever.
  def handle_info({:dispatch_timeout, ref}, state) do
    {:noreply,
     with_dispatch(state, ref, &handle_batch_result(&1, &2, {:error, :dispatch_timeout}))}
  end

  def handle_info({:retry_window, batch_id}, state) do
    state = %{state | retry_timer: nil}

    if Enum.any?(state.in_flight, &(&1.id == batch_id and &1.status == :pending)) do
      {:noreply, resend_window(state)}
    else
      {:noreply, state}
    end
  end

  def handle_info({:topic_create_result, batch_id, result}, state) do
    {:noreply, with_batch(state, batch_id, &handle_topic_create_result(&1, &2, result))}
  end

  def handle_info({:metadata_refresh_done, batch_id, latency_ms, error}, state) do
    Kafka.metadata_refresh(
      latency_ms,
      Map.merge(partition_meta(state), %{trigger: :error_recovery, error: error})
    )

    {:noreply, with_batch(state, batch_id, &resume_owed/2)}
  end

  def handle_info({:kafka_worker_dead, ref, reason}, %{registry_ref: ref} = state) do
    Logger.warning(
      "#{tag(state)} " <>
        "worker pid #{inspect(state.worker_pid)} died (reason=#{inspect(reason)}); initiating swap"
    )

    in_flight_count = in_flight_size(state)

    state =
      state
      |> cancel_timers()
      |> rewind_in_flight(reason)

    state = %{
      state
      | swap_started_at: state.swap_started_at || state.now_fn.(),
        swap_attempt: 0,
        last_error: {:worker_died, reason},
        pending_swap_telemetry:
          state.pending_swap_telemetry ||
            %{
              measurements: %{
                queue_depth: state.queue_size,
                in_flight_records: in_flight_count,
                queue_bytes: state.queue_bytes
              },
              metadata:
                Map.merge(partition_meta(state), %{
                  reason: reason,
                  producer_id_before: state.producer_id,
                  old_worker_pid: state.worker_pid
                })
            }
    }

    attempt_swap(state, reason)
  end

  def handle_info({:kafka_worker_dead, _ref, _reason}, state) do
    Logger.debug(
      "#{tag(state)} " <>
        "stale {:kafka_worker_dead, ...} for old registry_ref; ignoring"
    )

    {:noreply, state}
  end

  def handle_info({:retry_swap, ref, attempt}, %{registry_ref: ref} = state) do
    state = %{state | swap_attempt: attempt}
    attempt_swap(state, swap_last_reason(state))
  end

  def handle_info({:retry_swap, _ref, _attempt}, state), do: {:noreply, state}

  def handle_info({:EXIT, _pid, _reason}, state) do
    Logger.debug(
      "#{tag(state)} " <>
        "received unhandled :EXIT; swap is driven by {:kafka_worker_dead, ...} — ignoring"
    )

    {:noreply, state}
  end

  def handle_info({:block_timeout, from}, state) do
    {dropped, blocked} = drop_blocked(state.blocked, from)

    if dropped do
      GenServer.reply(from, {:error, {:queue_full, :timeout}})
    end

    {:noreply, %{state | blocked: blocked}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  # A full queue never discards a record unless the operator chose a drop
  # strategy: the default is `:block` for every record, guaranteed or not.
  defp admit_record(state, record, from_or_async) do
    if state.queue_size < state.queue_max do
      {:admitted, push(state, record)}
    else
      overflow(state, record, from_or_async, state.queue_overflow)
    end
  end

  defp overflow(state, record, from_or_async, :drop_newest) do
    state = record_drop(state, record, :drop_newest)

    case from_or_async do
      :async -> {:dropped_async, state}
      _from -> {:replied, {:error, {:queue_full, :overflow}}, state}
    end
  end

  defp overflow(state, record, _from_or_async, :drop_oldest) do
    {state, evicted} = pop(state)
    reply_to_record(evicted, {:error, {:queue_full, :evicted}})
    {:admitted, state |> record_drop(evicted, :drop_oldest) |> push(record)}
  end

  # Room is made by a drained batch, never by evicting an admitted record. A
  # synchronous caller waits out its own timeout; a cast-admitted record simply
  # waits — the enqueueing process is not blocked either way.
  defp overflow(state, record, :async, :block),
    do: {:blocked, %{state | blocked: :queue.in({:async, record}, state.blocked)}}

  defp overflow(state, record, from, :block) do
    Process.send_after(self(), {:block_timeout, from}, state.queue_block_timeout_ms)
    {:blocked, %{state | blocked: :queue.in({from, record}, state.blocked)}}
  end

  # Every drop is counted for `stats/1` — telemetry alone is invisible to a
  # runner polling the worker. A dropped delivery-guaranteed record is
  # alarm-grade on top: its outbox cursor stays where it is, so the loss is
  # visible as an outbox that stops draining rather than as a counter nobody
  # reads.
  defp record_drop(state, record, strategy) do
    guaranteed? = record != nil and Outbox.guaranteed?(record.metadata)

    if guaranteed? do
      Logger.error(
        "#{tag(state)} " <>
          "dropped a delivery-guaranteed record under :queue_overflow = #{strategy}; " <>
          "its outbox cursor will not advance until the entry is re-driven"
      )
    end

    Kafka.dropped(
      1,
      Map.merge(partition_meta(state), %{
        queue_depth: state.queue_size,
        overflow_strategy: strategy,
        guaranteed: guaranteed?,
        reason: :overflow
      })
    )

    %{state | dropped_total: state.dropped_total + 1}
  end

  # The outbox confirmation seam: an acked batch confirms every guaranteed
  # record it carried in ONE report, and a permanently failed one parks them
  # with their own identity — a recorded, replayable loss instead of a silent
  # one, and a park carries a payload, so it stays per record. Records carrying
  # no token belong to a plain chain and settle nothing.
  defp settle_outbox(batch, :ok), do: Outbox.confirm_many(Enum.map(batch.records, & &1.metadata))

  defp settle_outbox(batch, {:error, {_kind, reason}}),
    do: Enum.each(batch.records, &Outbox.park(&1.metadata, &1.value, reason, batch.attempt + 1))

  defp reply_to_record(nil, _reply), do: :ok
  defp reply_to_record(%{from: :async}, _reply), do: :ok
  defp reply_to_record(%{from: from}, reply), do: GenServer.reply(from, reply)

  defp push(state, record) do
    %{
      state
      | queue: :queue.in(record, state.queue),
        queue_size: state.queue_size + 1,
        queue_bytes: state.queue_bytes + record.bytes
    }
  end

  defp pop(state) do
    case :queue.out(state.queue) do
      {{:value, record}, q} ->
        {%{
           state
           | queue: q,
             queue_size: state.queue_size - 1,
             queue_bytes: state.queue_bytes - record.bytes
         }, record}

      {:empty, _} ->
        {state, nil}
    end
  end

  defp drop_blocked(queue, from) do
    list = :queue.to_list(queue)
    {dropped, kept} = Enum.split_with(list, fn {f, _} -> f == from end)
    {dropped != [], :queue.from_list(kept)}
  end

  # A batch is cut against a free window slot: while the window is full — or
  # while it is draining for an ordered resend — arrivals accumulate into the
  # next one instead.
  defp maybe_kick_batch(state) do
    cond do
      not slot_free?(state) or state.queue_size == 0 ->
        state

      state.queue_bytes >= state.max_batch_bytes ->
        send(self(), :flush_batch)
        state

      state.linger_timer == nil ->
        timer =
          if state.linger_ms == 0 do
            send(self(), :flush_batch)
            :immediate
          else
            Process.send_after(self(), :flush_batch, state.linger_ms)
          end

        %{state | linger_timer: timer}

      true ->
        state
    end
  end

  defp maybe_send_batch(state) do
    if slot_free?(state) do
      case drain_batch(state, state.max_batch_bytes) do
        {[], state} ->
          state

        {records, state} ->
          batch = new_batch(state, records)
          next_seq = if state.idempotent, do: state.sequence + length(records), else: 0

          %{state | sequence: next_seq, in_flight: state.in_flight ++ [batch]}
          |> do_send(batch)
          |> maybe_kick_batch()
      end
    else
      state
    end
  end

  # ------------------------------------------------------------------
  # The window: the batches outstanding for this partition, oldest first. A
  # batch carries two identities — `id` is stable across retries and names it
  # to everything the worker sends itself, `ref` is the client's handle for the
  # current dispatch and names it to that dispatch's outcome. Clearing `ref` is
  # how a dispatch is abandoned: a late outcome then names no batch at all.
  # `status` is about outcomes, not the wire: `:sent` means one is outstanding
  # (a response, or a topic creation / metadata refresh issued for this batch),
  # `:pending` means the batch is owed a resend.
  # ------------------------------------------------------------------

  defp new_batch(state, records) do
    %{
      id: make_ref(),
      ref: nil,
      records: records,
      base_sequence: state.sequence,
      attempt: 0,
      first_attempt_at: state.now_fn.(),
      last_error: nil,
      status: :sent,
      dispatch_timer: nil
    }
  end

  # Nothing new leaves while a batch is owed a resend: the broker must see the
  # window in its original order, so k goes out again before anything newer.
  defp slot_free?(state) do
    length(state.in_flight) < state.max_in_flight and
      not Enum.any?(state.in_flight, &(&1.status == :pending))
  end

  defp oldest?(%{in_flight: [%{id: id} | _]}, %{id: id}), do: true
  defp oldest?(_state, _batch), do: false

  # A resend waits until every batch sent after the failed one has come back:
  # the wire order must be k, k+1, … again, so nothing goes out mid-window.
  defp maybe_resend(state) do
    if Enum.any?(state.in_flight, &(&1.status == :sent)),
      do: state,
      else: schedule_retry(state)
  end

  # Everything still owed goes out again oldest first, under its original
  # sequence numbers, before anything newer is cut.
  defp resend_window(state) do
    state.in_flight
    |> Enum.filter(&(&1.status == :pending))
    |> Enum.reduce(state, fn batch, acc -> do_send(acc, batch) end)
  end

  # A fresh producer session numbers from zero, so every batch still owed is
  # renumbered in window order and owes another attempt. Whatever was on the
  # wire under the old session is abandoned: its dispatch handle is dropped, so
  # a late outcome names no batch.
  defp realign_window(state) do
    {batches, sequence} =
      Enum.map_reduce(state.in_flight, state.sequence, fn batch, seq ->
        batch = %{
          cancel_dispatch_timer(batch)
          | base_sequence: seq,
            attempt: batch.attempt + 1,
            status: :pending,
            ref: nil
        }

        {batch, seq + length(batch.records)}
      end)

    %{state | in_flight: batches, sequence: if(state.idempotent, do: sequence, else: 0)}
  end

  defp in_flight_size(state) do
    Enum.reduce(state.in_flight, 0, fn batch, n -> n + length(batch.records) end)
  end

  # Runs `fun` against the batch a dispatch handle names, having first taken the
  # dispatch off it — the outcome is in hand, so its timer and handle are spent.
  defp with_dispatch(state, ref, fun) do
    case Enum.find(state.in_flight, &(&1.ref == ref)) do
      nil ->
        state

      batch ->
        batch = %{cancel_dispatch_timer(batch) | ref: nil}
        fun.(put_batch(state, batch), batch)
    end
  end

  defp with_batch(state, batch_id, fun) do
    case Enum.find(state.in_flight, &(&1.id == batch_id)) do
      nil -> state
      batch -> fun.(state, batch)
    end
  end

  defp put_batch(state, batch) do
    %{state | in_flight: Enum.map(state.in_flight, &if(&1.id == batch.id, do: batch, else: &1))}
  end

  defp drop_batch(state, batch) do
    %{state | in_flight: Enum.reject(state.in_flight, &(&1.id == batch.id))}
  end

  # The whole window returns to the head of the queue, oldest batch first, so
  # the fresh producer session re-sends it in admission order.
  defp rewind_in_flight(state, reason) do
    records = Enum.flat_map(state.in_flight, & &1.records)
    bytes_back = Enum.reduce(records, 0, fn r, n -> n + r.bytes end)

    %{
      state
      | queue: :queue.join(:queue.from_list(records), state.queue),
        queue_size: state.queue_size + length(records),
        queue_bytes: state.queue_bytes + bytes_back,
        in_flight: [],
        last_error: {:worker_died, reason}
    }
  end

  defp drain_batch(state, max_bytes), do: drain(state, max_bytes, [], 0)

  # `acc_bytes` rides along: re-summing the accumulator per record made a
  # batch of n records cost n² additions.
  defp drain(state, max_bytes, acc, acc_bytes) do
    case :queue.peek(state.queue) do
      :empty ->
        {Enum.reverse(acc), state}

      {:value, record} ->
        if acc != [] and acc_bytes + record.bytes > max_bytes do
          {Enum.reverse(acc), state}
        else
          {state, _} = pop(state)
          drain(state, max_bytes, [record | acc], acc_bytes + record.bytes)
        end
    end
  end

  # The client answers by message, so the worker sends from its own process and
  # the dispatch handle it hands back identifies the batch. A handle is fresh
  # per dispatch: a late outcome from a prior hung send no longer names a batch
  # in the window and is ignored, so a slow send can't double-apply.
  defp do_send(state, batch) do
    request = build_produce_request(state, batch.records, batch.base_sequence)
    selector = NodeSelector.topic_partition(state.topic, state.partition)

    # A crash or call timeout in the shared client (e.g. a wedged socket after a
    # broker restart) must never silently strand the worker: turn it into a
    # retriable outcome that flows through the one result path, so every batch
    # in the window resolves and nothing resolves re-entrantly mid-resend.
    outcome =
      try do
        state.send_fn.(state.worker_pid, request, selector)
      catch
        :exit, reason -> {:error, {:exit, reason}}
        kind, reason -> {:error, {:send_crashed, kind, reason}}
      end

    {ref, timer} =
      case outcome do
        {:ok, ref} ->
          {ref, Process.send_after(self(), {:dispatch_timeout, ref}, state.dispatch_timeout_ms)}

        {:error, _reason} = error ->
          ref = make_ref()
          send(self(), {:kafka_ex_response, ref, error})
          {ref, nil}
      end

    put_batch(state, %{
      cancel_dispatch_timer(batch)
      | ref: ref,
        status: :sent,
        dispatch_timer: timer
    })
  end

  defp build_produce_request(state, records, base_sequence) do
    record_batch = %Kayrock.RecordBatch{
      attributes: 0,
      producer_id: if(state.idempotent, do: state.producer_id, else: -1),
      producer_epoch: if(state.idempotent, do: state.producer_epoch, else: -1),
      base_sequence: if(state.idempotent, do: base_sequence, else: -1),
      records:
        Enum.map(records, fn r ->
          %Kayrock.RecordBatch.Record{
            attributes: 0,
            timestamp: r.timestamp,
            offset: 0,
            key: r.key,
            value: r.value,
            headers: record_headers(r)
          }
        end)
    }

    %Kayrock.Produce.V5.Request{
      transactional_id: nil,
      acks: state.required_acks,
      timeout: state.request_timeout_ms,
      topic_data: [
        %{
          topic: state.topic,
          data: [%{partition: state.partition, record_set: record_batch}]
        }
      ]
    }
  end

  defp handle_batch_result(state, batch, result) do
    case classify_result(result, state) do
      {:ok, base_offset} ->
        deliver_batch(state, batch, base_offset)

      {:already_delivered, reason} ->
        Logger.info("#{tag(state)} broker reports #{inspect(reason)} — treating as delivered")
        deliver_batch(state, batch, -1)

      {:retry, reason} ->
        retry_batch(state, batch, reason)

      {:fenced, reason} ->
        fence_batch(state, batch, reason)

      {:permanent, reason} ->
        emit_failed_per_record(state, batch, reason)
        complete_batch(state, batch, {:error, {:permanent, reason}})
    end
  end

  defp deliver_batch(state, batch, base_offset) do
    Kafka.produce_ok(
      length(batch.records),
      elapsed_ms(state, batch),
      Map.merge(partition_meta(state), %{base_offset: base_offset, attempt: batch.attempt + 1})
    )

    complete_batch(state, batch, :ok)
  end

  defp retry_batch(state, batch, reason) do
    cond do
      exhausted?(state, batch) ->
        fail_batch(state, batch, reason)

      reason == :unknown_topic_or_partition and not state.auto_create_topic ->
        fail_batch(state, batch, :unknown_topic_or_partition)

      # The recovery action is this batch's outstanding outcome, so the window
      # keeps waiting on it rather than resending around it.
      reason == :unknown_topic_or_partition ->
        {state, batch} = retrying(state, batch, reason, :sent)
        issue_create_topic(state, batch)
        state

      reason in [:not_leader_for_partition, :no_broker] ->
        {state, batch} = retrying(state, batch, reason, :sent)
        issue_metadata_refresh(state, batch)
        state

      true ->
        {state, _batch} = retrying(state, batch, reason, :pending)
        maybe_resend(state)
    end
  end

  # The producer session is fenced and nothing in the window was stored. Re-init
  # the session on the same (healthy) broker client, renumber every batch still
  # owed in window order, and resend them in that order with backoff — never ack
  # a fenced batch. Bounded by retries / delivery_timeout so a re-fencing broker
  # can't spin; on exhaustion fail_batch/3 increments consecutive_failures and
  # maybe_refresh_broker/2 forces a fresh broker client.
  defp fence_batch(state, batch, reason) do
    cond do
      # An earlier batch is still unresolved, so the broker rejected this one
      # for the sequence gap that batch left, not for a fenced session.
      not oldest?(state, batch) ->
        {state, _batch} = retrying(state, batch, reason, :pending)
        maybe_resend(state)

      exhausted?(state, batch) ->
        fail_batch(state, batch, reason)

      true ->
        emit_retry(state, batch, reason)

        %{state | last_error: reason}
        |> restart_producer_session()
        |> schedule_retry()
    end
  end

  defp exhausted?(state, batch) do
    batch.attempt + 1 > state.retries or elapsed_ms(state, batch) >= state.delivery_timeout_ms
  end

  # One more attempt is owed. `:sent` parks the batch behind a recovery action
  # that is still running; `:pending` puts it in the resend queue.
  defp retrying(state, batch, reason, status) do
    emit_retry(state, batch, reason)

    batch = %{
      cancel_dispatch_timer(batch)
      | attempt: batch.attempt + 1,
        last_error: reason,
        status: status,
        ref: nil
    }

    {put_batch(%{state | last_error: reason}, batch), batch}
  end

  defp emit_retry(state, batch, reason) do
    Kafka.retry(
      batch.attempt + 1,
      retry_backoff_ms(state, batch.attempt + 1),
      Map.put(partition_meta(state), :reason, reason)
    )
  end

  # The recovery action this batch was waiting on is done; it re-joins the
  # resend queue.
  defp resume_owed(state, batch) do
    state |> put_batch(%{batch | status: :pending}) |> maybe_resend()
  end

  defp handle_topic_create_result(state, batch, result) do
    case result do
      :ok ->
        Kafka.topic_created(
          state.auto_create_partitions,
          state.auto_create_replication_factor,
          %{
            plugin_id: state.plugin_id,
            topic: state.topic
          }
        )

        resume_owed(state, batch)

      {:error, :topic_already_exists} ->
        resume_owed(state, batch)

      {:error, reason} ->
        fail_batch(state, batch, reason)
    end
  end

  defp issue_create_topic(state, batch) do
    parent = self()
    id = batch.id
    create_fn = state.create_topic_fn
    worker = state.worker_pid
    topic = state.topic

    opts = %{
      num_partitions: state.auto_create_partitions,
      replication_factor: state.auto_create_replication_factor,
      config_entries: state.topic_configs,
      timeout_ms: state.metadata_refresh_timeout_ms
    }

    timeout = state.metadata_refresh_timeout_ms

    spawn(fn ->
      # A crash here would leave the batch — and, behind it, the whole window —
      # waiting on an outcome that never arrives.
      result =
        try do
          create_fn.(worker, topic, opts, timeout)
        catch
          :exit, reason -> {:error, {:exit, reason}}
          kind, reason -> {:error, {kind, reason}}
        end

      send(parent, {:topic_create_result, id, result})
    end)

    :ok
  end

  defp issue_metadata_refresh(state, batch) do
    parent = self()
    id = batch.id
    refresh_fn = state.metadata_refresh_fn
    worker = state.worker_pid
    started = state.now_fn.()
    timeout = state.metadata_refresh_timeout_ms
    now_fn = state.now_fn

    spawn(fn ->
      err =
        try do
          case refresh_fn.(worker, timeout) do
            :ok -> nil
            {:ok, _} -> nil
            {:error, reason} -> reason
            other -> other
          end
        catch
          :exit, reason -> reason
        end

      send(parent, {:metadata_refresh_done, id, now_fn.() - started, err})
    end)

    :ok
  end

  # One timer for the whole resend run: the oldest owed batch sets the backoff
  # and its arrival resends everything owed behind it.
  defp schedule_retry(state) do
    state = cancel_retry_timer(state)

    case Enum.find(state.in_flight, &(&1.status == :pending)) do
      nil ->
        state

      batch ->
        backoff = retry_backoff_ms(state, batch.attempt)
        %{state | retry_timer: Process.send_after(self(), {:retry_window, batch.id}, backoff)}
    end
  end

  defp retry_backoff_ms(state, attempt) do
    base = trunc(state.retry_backoff_ms * :math.pow(2, max(attempt - 1, 0)))
    jitter = state.jitter_fn.(100)
    min(base + jitter, state.retry_max_backoff_ms)
  end

  defp fail_batch(state, batch, reason) do
    emit_failed_per_record(state, batch, reason)

    state = %{state | consecutive_failures: state.consecutive_failures + 1}
    state = maybe_refresh_broker(state, reason)

    complete_batch(state, batch, {:error, {:delivery_failed, reason}})
  end

  # On a sustained stall (N consecutive give-ups) force a fresh broker
  # client. A clean broker restart never kills the shared client, so the
  # worker-DOWN swap path never fires and produce stalls forever. Refresh
  # trips the registry's worker monitor, driving the standard
  # `{:kafka_worker_dead, ...}` swap that reconnects on a new client.
  # Reset the counter so we don't hammer refresh every subsequent batch.
  defp maybe_refresh_broker(%{registry_ref: nil} = state, _reason), do: state

  defp maybe_refresh_broker(state, reason) do
    cond do
      # A no-broker-connection error means the shared client is wedged on a
      # dead/stale socket (the clean-broker-restart case). One such batch is
      # authoritative — recreate now, throttled, instead of waiting for the
      # stall counter a low-traffic producer may never reach.
      reason in @client_wedged and force_replace_due?(state) ->
        Logger.warning(
          "#{tag(state)} " <>
            "no broker connection (reason=#{inspect(reason)}); forcing fresh broker connection"
        )

        force_replaced(state)

      state.consecutive_failures >= state.stall_threshold ->
        Logger.warning(
          "#{tag(state)} " <>
            "#{state.consecutive_failures} consecutive failed batches " <>
            "(last reason=#{inspect(reason)}); forcing fresh broker connection"
        )

        force_replaced(state)

      true ->
        state
    end
  end

  defp force_replaced(state) do
    %{
      force_replace_broker(state)
      | consecutive_failures: 0,
        last_force_replace_at: state.now_fn.()
    }
  end

  defp force_replace_due?(%{last_force_replace_at: nil}), do: true

  defp force_replace_due?(state) do
    state.now_fn.() - state.last_force_replace_at >= @force_replace_throttle_ms
  end

  # Reliable recovery: re-acquire a freshly started shared client by identity
  # and rebind worker_pid directly. Avoids the monitor-DOWN swap fan-out, which
  # never reaches a stalled caller once the entry has been torn down.
  defp force_replace_broker(%{connection_identity: nil} = state), do: state

  defp force_replace_broker(state) do
    case WorkerRegistry.force_replace(state.connection_identity) do
      {:ok, new_pid} when is_pid(new_pid) ->
        restart_producer_session(%{state | worker_pid: new_pid, last_error: nil})

      _ ->
        state
    end
  end

  # (Re-)initialise the idempotent producer session: fetch a fresh
  # producer-id/epoch, reset the sequence, and renumber the window against it.
  # Used both on a producer fence and after a fresh broker client (the old id is
  # invalid there). Non-idempotent producers carry no id, but their window is
  # renumbered all the same — every batch still owed has to go out again.
  defp restart_producer_session(%{idempotent: false} = state), do: realign_window(state)

  defp restart_producer_session(state) do
    case state.init_producer_id_fn.(state.worker_pid, state.metadata_refresh_timeout_ms) do
      {:ok, %{producer_id: pid, producer_epoch: epoch}} ->
        realign_window(%{state | producer_id: pid, producer_epoch: epoch, sequence: 0})

      _ ->
        realign_window(%{state | producer_id: nil, producer_epoch: nil, sequence: 0})
    end
  end

  defp emit_failed_per_record(state, batch, reason) do
    total_ms = elapsed_ms(state, batch)

    Enum.each(batch.records, fn r ->
      Kafka.failed(
        batch.attempt + 1,
        total_ms,
        Map.merge(partition_meta(state), %{last_reason: reason, caller: caller_tag(r)})
      )
    end)
  end

  defp caller_tag(%{from: :async}), do: :async
  defp caller_tag(_), do: :sync

  defp complete_batch(state, batch, reply) do
    Enum.each(batch.records, &reply_to_record(&1, reply))
    settle_outbox(batch, reply)

    state = %{
      drop_batch(state, batch)
      | consecutive_failures: if(reply == :ok, do: 0, else: state.consecutive_failures),
        last_error:
          case reply do
            :ok -> nil
            {:error, {_, reason}} -> reason
          end
    }

    state
    |> process_blocked()
    |> maybe_resend()
    |> maybe_kick_batch()
  end

  defp process_blocked(state) do
    case :queue.out(state.blocked) do
      {:empty, _} ->
        state

      {{:value, {from, record}}, rest} ->
        if state.queue_size < state.queue_max do
          state = %{state | blocked: rest}
          state = push(state, %{record | from: from})
          process_blocked(state)
        else
          state
        end
    end
  end

  defp record_headers(%{dedup_key: dedup_key}) when is_binary(dedup_key) do
    [%Kayrock.RecordBatch.RecordHeader{key: "flezha.dedup_key", value: dedup_key}]
  end

  defp record_headers(_record), do: []

  defp elapsed_ms(state, batch), do: state.now_fn.() - batch.first_attempt_at

  # The worker's identity, in a log line and in every telemetry event.
  defp tag(state),
    do: "[Kafka.ProducerWorker #{state.plugin_id}/#{state.topic}/#{state.partition}]"

  defp partition_meta(state) do
    %{plugin_id: state.plugin_id, topic: state.topic, partition: state.partition}
  end

  defp build_record(msg, metadata, from, timestamp_ms) do
    {key, dedup_key} = record_identity(metadata)

    %{
      from: from,
      value: msg,
      key: key,
      dedup_key: dedup_key,
      metadata: metadata,
      timestamp: timestamp_ms,
      bytes: byte_size_or(0, msg) + byte_size_or(0, key) + byte_size_or(0, dedup_key)
    }
  end

  # Effectively-once tier record (`flz.epoch` present): the natural key is
  # the Kafka message key — stable partitioning across replays, upsert
  # semantics on compacted/changelog topics — and the full durable identity
  # travels as the `flezha.dedup_key` header
  # (`<flz.entity>/<outbox_seq>/<epoch>`) for consumer-side exact dedup on
  # append-only topics. Re-driven entries carry identical identity.
  defp record_identity(
         %{"flz.epoch" => epoch, "flz.entity" => entity, "flz.seq" => outbox_seq} = metadata
       ) do
    {Map.get(metadata, "flz.key"), "#{entity}/#{outbox_seq}/#{epoch}"}
  end

  # At-least-once records: no identity header, but a `.keyed()` channel's
  # natural key still partitions. Only the effectively-once tier stamps
  # `flz.epoch`, so requiring it here would ship every ordinary keyed emit
  # with a nil key — and a nil key scatters one entity's records across
  # partitions, losing exactly the ordering `.keyed()` was declared for.
  defp record_identity(metadata) do
    key =
      Map.get(metadata, "flz.key") ||
        Map.get(metadata, "flezha.dedup_key") ||
        Map.get(metadata, "kafka_msg_key", nil)

    {key, nil}
  end

  defp byte_size_or(_default, bin) when is_binary(bin), do: byte_size(bin)
  defp byte_size_or(default, _), do: default

  defp classify_result(result, state) do
    case result do
      {:ok, %{responses: responses}} ->
        case extract_partition(responses, state.topic, state.partition) do
          {:ok, base_offset} ->
            {:ok, base_offset}

          {:error, error_atom} when error_atom in @retriable ->
            {:retry, error_atom}

          {:error, error_atom} when error_atom in @permanent ->
            {:permanent, error_atom}

          {:error, :unknown_topic_or_partition} ->
            {:retry, :unknown_topic_or_partition}

          {:error, error_atom} when error_atom in @idempotence_fenced ->
            {:fenced, error_atom}

          {:error, error_atom} when error_atom in @idempotence_already_delivered ->
            {:already_delivered, error_atom}

          {:error, other} ->
            {:permanent, other}
        end

      {:error, :no_broker} ->
        {:retry, :no_broker}

      {:error, :timeout} ->
        {:retry, :request_timed_out}

      {:error, {:exit, _}} ->
        {:retry, :network_exception}

      {:error, reason} when is_atom(reason) and reason in @retriable ->
        {:retry, reason}

      {:error, reason} when is_atom(reason) and reason in @permanent ->
        {:permanent, reason}

      {:error, {:send_crashed, _kind, _reason}} ->
        {:retry, :network_exception}

      {:error, :dispatch_timeout} ->
        {:retry, :dispatch_timeout}

      {:error, reason} ->
        {:permanent, reason}
    end
  end

  defp extract_partition(responses, topic, partition) do
    case Enum.find(responses, fn r -> Map.get(r, :topic) == topic end) do
      nil ->
        {:error, :no_topic_in_response}

      %{partition_responses: parts} ->
        case Enum.find(parts, fn p -> Map.get(p, :partition) == partition end) do
          nil ->
            {:error, :no_partition_in_response}

          %{error_code: 0, base_offset: offset} ->
            {:ok, offset}

          %{error_code: code} ->
            {:error, Kayrock.ErrorCode.code_to_atom(code)}
        end
    end
  end

  defp default_send(worker_pid, request, node_selector) do
    KafkaClient.send_request_async(worker_pid, request, node_selector)
  end

  defp default_init_producer_id(worker_pid, timeout_ms) do
    request = %Kayrock.InitProducerId.V0.Request{
      transactional_id: nil,
      transaction_timeout_ms: timeout_ms
    }

    case KafkaClient.send_request(worker_pid, request, NodeSelector.first_available(), timeout_ms) do
      {:ok, %{error_code: 0, producer_id: pid, producer_epoch: epoch}} ->
        {:ok, %{producer_id: pid, producer_epoch: epoch}}

      {:ok, %{error_code: code}} ->
        {:error, Kayrock.ErrorCode.code_to_atom(code)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp default_create_topic(worker_pid, topic, opts, timeout_ms) do
    request = %Kayrock.CreateTopics.V0.Request{
      timeout_ms: opts.timeout_ms,
      topics: [
        %{
          name: topic,
          num_partitions: opts.num_partitions,
          replication_factor: opts.replication_factor,
          assignments: [],
          configs:
            Enum.map(opts.config_entries, fn
              {k, v} -> %{name: to_string(k), value: to_string(v)}
              %{config_name: cn, config_value: cv} -> %{name: to_string(cn), value: to_string(cv)}
              %{name: _, value: _} = m -> m
            end)
        }
      ]
    }

    case KafkaClient.send_request(worker_pid, request, NodeSelector.controller(), timeout_ms) do
      {:ok, %{topics: errors}} ->
        case Enum.find(errors, fn e ->
               Map.get(e, :name) == topic
             end) do
          %{error_code: 0} ->
            :ok

          %{error_code: code} ->
            atom = Kayrock.ErrorCode.code_to_atom(code)

            if atom == :topic_already_exists,
              do: {:error, :topic_already_exists},
              else: {:error, atom}

          nil ->
            :ok
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp default_metadata_refresh(worker_pid, timeout_ms) do
    GenServer.call(worker_pid, :update_metadata, timeout_ms)
  end

  defp acquire_worker(opts) do
    cond do
      pid = Keyword.get(opts, :worker_pid) ->
        {pid, nil, false}

      identity = Keyword.get(opts, :connection_identity) ->
        case WorkerRegistry.acquire(identity) do
          {:ok, pid, ref} -> {pid, ref, true}
          {:error, reason} -> exit({:registry_acquire_failed, reason})
        end

      true ->
        raise ArgumentError,
              "ProducerWorker requires either :worker_pid or :connection_identity"
    end
  end

  defp cancel_timers(state) do
    if is_reference(state.linger_timer), do: Process.cancel_timer(state.linger_timer)
    state = cancel_retry_timer(state)
    %{state | in_flight: Enum.map(state.in_flight, &cancel_dispatch_timer/1), linger_timer: nil}
  end

  defp cancel_retry_timer(%{retry_timer: t} = state) do
    if is_reference(t), do: Process.cancel_timer(t)
    %{state | retry_timer: nil}
  end

  defp cancel_dispatch_timer(%{dispatch_timer: t} = batch) do
    if is_reference(t), do: Process.cancel_timer(t)
    %{batch | dispatch_timer: nil}
  end

  defp attempt_swap(state, _reason) do
    case do_swap_or_acquire(state) do
      {:ok, new_pid, new_ref} ->
        pending =
          case state.pending_swap_telemetry do
            nil -> nil
            payload -> %{payload | metadata: Map.put(payload.metadata, :new_worker_pid, new_pid)}
          end

        state = %{
          state
          | worker_pid: new_pid,
            registry_ref: new_ref,
            producer_id: nil,
            producer_epoch: nil,
            sequence: 0,
            in_flight: [],
            swap_started_at: nil,
            swap_attempt: 0,
            pending_swap_telemetry: pending
        }

        {:noreply, state, {:continue, :maybe_init_producer_id}}

      {:error, swap_reason} ->
        now = state.now_fn.()
        elapsed = now - (state.swap_started_at || now)

        cond do
          elapsed >= state.delivery_timeout_ms ->
            fail_all_during_swap(state, swap_reason)

          true ->
            attempt = (state.swap_attempt || 0) + 1
            backoff = retry_backoff_ms(state, attempt)
            Process.send_after(self(), {:retry_swap, state.registry_ref, attempt}, backoff)
            {:noreply, %{state | swap_attempt: attempt, last_error: {:swap_failed, swap_reason}}}
        end
    end
  end

  defp do_swap_or_acquire(state) do
    case WorkerRegistry.swap(state.registry_ref) do
      {:ok, _pid, _ref} = ok ->
        ok

      {:error, :no_pending_swap} when not is_nil(state.connection_identity) ->
        WorkerRegistry.acquire(state.connection_identity)

      {:error, _reason} = err ->
        err
    end
  end

  defp swap_last_reason(%{last_error: {:worker_died, reason}}), do: reason
  defp swap_last_reason(%{last_error: {:swap_failed, reason}}), do: reason
  defp swap_last_reason(_), do: :unknown

  # On swap exhaustion we stay alive: every queued record is replied to
  # with `{:error, {:delivery_failed, :worker_swap_exhausted}}` and a
  # `[:produce, :failed]` event fires per record (per task 2.4). The
  # producer's `last_error` carries the terminal reason so callers can
  # surface it. Subsequent enqueues fail through the standard send-path
  # `delivery_timeout_ms` retry against the dead `worker_pid`, so no
  # tight restart loop and no `:swap_exhausted` crash report.
  defp fail_all_during_swap(state, swap_reason) do
    queued = :queue.to_list(state.queue)

    Enum.each(queued, fn record ->
      reply_to_record(record, {:error, {:delivery_failed, :worker_swap_exhausted}})

      Kafka.failed(
        state.swap_attempt + 1,
        state.delivery_timeout_ms,
        Map.merge(partition_meta(state), %{
          last_reason: {:worker_swap_exhausted, swap_reason},
          caller: caller_tag(record)
        })
      )
    end)

    {:noreply,
     %{
       state
       | queue: :queue.new(),
         queue_size: 0,
         queue_bytes: 0,
         in_flight: [],
         swap_started_at: nil,
         swap_attempt: 0,
         last_error: {:swap_exhausted, swap_reason},
         pending_swap_telemetry: nil
     }}
  end

  defp emit_pending_swap_telemetry(%{pending_swap_telemetry: nil} = state), do: state

  defp emit_pending_swap_telemetry(%{pending_swap_telemetry: payload} = state) do
    metadata = Map.put(payload.metadata, :producer_id_after, state.producer_id)
    Kafka.worker_swapped(payload.measurements, metadata)
    %{state | pending_swap_telemetry: nil}
  end

  @impl true
  def terminate(_reason, %{registry_owned?: true, registry_ref: ref}) when not is_nil(ref) do
    _ = WorkerRegistry.release(ref)
    :ok
  end

  def terminate(_reason, _state), do: :ok
end
