defmodule Flezha.Plugin.Provider.Kafka.ProducerWorker do
  @moduledoc """
  Per-`(plugin_id, topic, partition)` GenServer that owns a bounded outbound
  queue, runs linger-based batching, retries transient broker errors, and
  drives idempotent producer-id semantics.

  Issues real Produce/InitProducerId/CreateTopics requests through a
  `KafkaEx.New.Client` (Kayrock-based) shared worker pid acquired from
  `Flezha.Plugin.Provider.Kafka.WorkerRegistry`.

  All side-effects (broker I/O, metadata refreshes, topic creation) run in
  short-lived spawned processes so the GenServer mailbox stays drainable
  during in-flight batches and retry sleeps.
  """

  use GenServer

  require Logger

  alias Flezha.Plugin.Provider.Kafka.ProducerSup
  alias Flezha.Plugin.Provider.Kafka.Telemetry
  alias Flezha.Plugin.Provider.Kafka.WorkerRegistry
  alias KafkaEx.New.Client, as: KafkaClient
  alias KafkaEx.New.Structs.NodeSelector

  @retriable [
    :no_broker,
    :not_leader_for_partition,
    :network_exception,
    :request_timed_out,
    :not_enough_replicas,
    :not_enough_replicas_after_append,
    :replica_not_available,
    :leader_not_available,
    :broker_not_available
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

  @idempotence_already_delivered [
    :duplicate_sequence_number,
    :invalid_producer_epoch,
    :out_of_order_sequence_number
  ]

  @spec via(binary(), binary(), non_neg_integer()) :: {:via, Registry, term()}
  def via(plugin_id, topic, partition) do
    {:via, Registry, {ProducerSup.registry_name(), {plugin_id, topic, partition}}}
  end

  def start_link(opts) when is_list(opts) do
    plugin_id = Keyword.fetch!(opts, :plugin_id)
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)
    name = Keyword.get(opts, :name, via(plugin_id, topic, partition))
    GenServer.start_link(__MODULE__, opts, name: name)
  end

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
  overflow is resolved per the configured `:queue_overflow` strategy with
  the caveat that `:block` mode is not honored on this path (it is treated
  as `:drop_newest` with a one-shot deprecation warning).

  Permanent delivery failures of cast-admitted records surface only through
  `[:flezha, :plugin, :kafka, :produce, :failed]` telemetry tagged with
  `caller: :async` — there is no caller to reply to.
  """
  @spec cast_enqueue(GenServer.server(), binary(), map()) :: :ok
  def cast_enqueue(server, msg, metadata) do
    GenServer.cast(server, {:enqueue_async, msg, metadata})
  end

  @spec stats(GenServer.server()) :: map()
  def stats(server) do
    GenServer.call(server, :stats)
  end

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
      queue_overflow: Keyword.get(opts, :queue_overflow, :drop_oldest),
      queue_block_timeout_ms: Keyword.get(opts, :queue_block_timeout_ms, 5_000),
      retries: Keyword.get(opts, :retries, 5),
      retry_backoff_ms: Keyword.get(opts, :retry_backoff_ms, 100),
      retry_max_backoff_ms: Keyword.get(opts, :retry_max_backoff_ms, 5_000),
      delivery_timeout_ms: Keyword.get(opts, :delivery_timeout_ms, 30_000),
      required_acks: Keyword.get(opts, :required_acks, -1),
      request_timeout_ms: Keyword.get(opts, :request_timeout_ms, 10_000),
      idempotent: Keyword.get(opts, :idempotent, true),
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
      in_flight: nil,
      retry_timer: nil,
      blocked: :queue.new(),
      dropped_total: 0,
      last_error: nil,
      block_in_cast_warned?: false,
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
          "[Kafka.ProducerWorker #{state.plugin_id}/#{state.topic}/#{state.partition}] " <>
            "InitProducerId failed: #{inspect(reason)} — running in non-idempotent mode"
        )

        state = %{state | idempotent: false, last_error: reason}
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
       dropped_total: state.dropped_total,
       queue_depth: state.queue_size,
       in_flight: in_flight_size(state),
       last_error: state.last_error
     }, state}
  end

  @impl true
  def handle_cast({:enqueue_async, msg, metadata}, state) do
    record = build_record(msg, metadata, :async, state.wall_now_fn.())

    case admit_record(state, record, :async) do
      {:admitted, state} -> {:noreply, maybe_kick_batch(state)}
      {:dropped_async, state} -> {:noreply, state}
    end
  end

  @impl true
  def handle_info(:flush_batch, state) do
    {:noreply, maybe_send_batch(%{state | linger_timer: nil})}
  end

  def handle_info({:batch_result, batch_ref, result, attempt_no}, state) do
    case state.in_flight do
      %{ref: ^batch_ref} = batch ->
        {:noreply, handle_batch_result(state, batch, attempt_no, result)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:retry_batch, batch_ref}, state) do
    state = %{state | retry_timer: nil}

    case state.in_flight do
      %{ref: ^batch_ref} = batch ->
        {:noreply, do_send(state, batch, batch.attempt)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:topic_create_result, batch_ref, result}, state) do
    case state.in_flight do
      %{ref: ^batch_ref} = batch ->
        {:noreply, handle_topic_create_result(state, batch, result)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:metadata_refresh_done, batch_ref, latency_ms, error}, state) do
    Telemetry.metadata_refresh(latency_ms, %{
      trigger: :error_recovery,
      error: error,
      plugin_id: state.plugin_id,
      topic: state.topic,
      partition: state.partition
    })

    case state.in_flight do
      %{ref: ^batch_ref} = batch ->
        {:noreply, schedule_retry(state, batch)}

      _ ->
        {:noreply, state}
    end
  end

  def handle_info({:kafka_worker_dead, ref, reason}, %{registry_ref: ref} = state) do
    Logger.warning(
      "[Kafka.ProducerWorker #{state.plugin_id}/#{state.topic}/#{state.partition}] " <>
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
              metadata: %{
                plugin_id: state.plugin_id,
                topic: state.topic,
                partition: state.partition,
                reason: reason,
                producer_id_before: state.producer_id,
                old_worker_pid: state.worker_pid
              }
            }
    }

    attempt_swap(state, reason)
  end

  def handle_info({:kafka_worker_dead, _ref, _reason}, state) do
    Logger.debug(
      "[Kafka.ProducerWorker #{state.plugin_id}/#{state.topic}/#{state.partition}] " <>
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
      "[Kafka.ProducerWorker #{state.plugin_id}/#{state.topic}/#{state.partition}] " <>
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

  defp admit_record(state, record, from_or_async) do
    cond do
      state.queue_size < state.queue_max ->
        {:admitted, push(state, record)}

      state.queue_overflow == :drop_newest ->
        Telemetry.dropped(1, %{
          plugin_id: state.plugin_id,
          topic: state.topic,
          partition: state.partition,
          queue_depth: state.queue_size,
          overflow_strategy: :drop_newest,
          reason: :overflow
        })

        state = %{state | dropped_total: state.dropped_total + 1}

        case from_or_async do
          :async -> {:dropped_async, state}
          _from -> {:replied, {:error, {:queue_full, :overflow}}, state}
        end

      state.queue_overflow == :drop_oldest ->
        {state, evicted} = pop(state)
        reply_to_record(evicted, {:error, {:queue_full, :evicted}})

        Telemetry.dropped(1, %{
          plugin_id: state.plugin_id,
          topic: state.topic,
          partition: state.partition,
          queue_depth: state.queue_size,
          overflow_strategy: :drop_oldest,
          reason: :overflow
        })

        state = %{state | dropped_total: state.dropped_total + 1}
        {:admitted, push(state, record)}

      state.queue_overflow == :block and from_or_async == :async ->
        state = warn_block_in_cast_unsupported(state)

        Telemetry.dropped(1, %{
          plugin_id: state.plugin_id,
          topic: state.topic,
          partition: state.partition,
          queue_depth: state.queue_size,
          overflow_strategy: :block_in_cast_unsupported,
          reason: :overflow
        })

        {:dropped_async, %{state | dropped_total: state.dropped_total + 1}}

      state.queue_overflow == :block ->
        from = from_or_async
        Process.send_after(self(), {:block_timeout, from}, state.queue_block_timeout_ms)
        {:blocked, %{state | blocked: :queue.in({from, record}, state.blocked)}}
    end
  end

  defp reply_to_record(nil, _reply), do: :ok
  defp reply_to_record(%{from: :async}, _reply), do: :ok
  defp reply_to_record(%{from: from}, reply), do: GenServer.reply(from, reply)

  defp warn_block_in_cast_unsupported(%{block_in_cast_warned?: true} = state), do: state

  defp warn_block_in_cast_unsupported(state) do
    Logger.warning(
      "[Kafka.ProducerWorker #{state.plugin_id}/#{state.topic}/#{state.partition}] " <>
        ":queue_overflow = :block is not honored on the chain dispatch path " <>
        "(cast_enqueue/3). Cast-admitted records are treated as :drop_newest " <>
        "with telemetry. Use :drop_oldest or :drop_newest explicitly to silence " <>
        "this warning."
    )

    %{state | block_in_cast_warned?: true}
  end

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

  defp maybe_kick_batch(state) do
    cond do
      state.in_flight != nil ->
        state

      state.queue_bytes >= state.max_batch_bytes ->
        send(self(), :flush_batch)
        state

      state.linger_timer == nil and state.queue_size > 0 ->
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

  defp maybe_send_batch(%{in_flight: in_flight} = state) when not is_nil(in_flight), do: state

  defp maybe_send_batch(state) do
    case drain_batch(state, state.max_batch_bytes) do
      {[], state} ->
        state

      {records, state} ->
        ref = make_ref()
        base_seq = state.sequence
        next_seq = if state.idempotent, do: state.sequence + length(records), else: 0

        batch = %{
          ref: ref,
          records: records,
          base_sequence: base_seq,
          attempt: 0,
          first_attempt_at: state.now_fn.(),
          last_error: nil
        }

        do_send(%{state | in_flight: batch, sequence: next_seq}, batch, 0)
    end
  end

  defp drain_batch(state, max_bytes), do: drain(state, max_bytes, [])

  defp drain(state, max_bytes, acc) do
    case :queue.peek(state.queue) do
      :empty ->
        {Enum.reverse(acc), state}

      {:value, record} ->
        if acc != [] and byte_acc(acc) + record.bytes > max_bytes do
          {Enum.reverse(acc), state}
        else
          {state, _} = pop(state)
          drain(state, max_bytes, [record | acc])
        end
    end
  end

  defp byte_acc(records), do: Enum.reduce(records, 0, fn r, n -> n + r.bytes end)

  defp do_send(state, batch, attempt) do
    parent = self()
    ref = batch.ref
    request = build_produce_request(state, batch.records, batch.base_sequence)
    send_fn = state.send_fn
    worker = state.worker_pid
    topic = state.topic
    partition = state.partition

    spawn(fn ->
      result = send_fn.(worker, request, NodeSelector.topic_partition(topic, partition))
      send(parent, {:batch_result, ref, result, attempt})
    end)

    %{state | in_flight: %{batch | attempt: attempt}}
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
            headers: []
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

  defp handle_batch_result(state, batch, attempt, result) do
    case classify_result(result, state) do
      {:ok, base_offset} ->
        Telemetry.produce_ok(length(batch.records), elapsed_ms(state, batch), %{
          plugin_id: state.plugin_id,
          topic: state.topic,
          partition: state.partition,
          base_offset: base_offset,
          attempt: attempt + 1
        })

        complete_batch(state, batch, :ok)

      {:retry, reason} ->
        cond do
          attempt + 1 > state.retries ->
            fail_batch(state, batch, reason)

          elapsed_ms(state, batch) >= state.delivery_timeout_ms ->
            fail_batch(state, batch, reason)

          true ->
            Telemetry.retry(attempt + 1, retry_backoff_ms(state, attempt + 1), %{
              plugin_id: state.plugin_id,
              topic: state.topic,
              partition: state.partition,
              reason: reason
            })

            batch = %{batch | attempt: attempt + 1, last_error: reason}
            state = %{state | in_flight: batch, last_error: reason}

            cond do
              reason == :unknown_topic_or_partition and state.auto_create_topic ->
                issue_create_topic(state, batch)
                state

              reason == :unknown_topic_or_partition and not state.auto_create_topic ->
                fail_batch(state, batch, :unknown_topic_or_partition)

              reason in [:not_leader_for_partition, :no_broker] ->
                issue_metadata_refresh(state, batch)
                state

              true ->
                schedule_retry(state, batch)
            end
        end

      {:permanent, reason} ->
        emit_failed_per_record(state, batch, attempt + 1, reason)
        complete_batch(state, batch, {:error, {:permanent, reason}})

      {:already_delivered, reason} ->
        Logger.info(
          "[Kafka.ProducerWorker #{state.plugin_id}/#{state.topic}/#{state.partition}] " <>
            "broker reports #{inspect(reason)} — treating as delivered"
        )

        Telemetry.produce_ok(length(batch.records), elapsed_ms(state, batch), %{
          plugin_id: state.plugin_id,
          topic: state.topic,
          partition: state.partition,
          base_offset: -1,
          attempt: attempt + 1
        })

        complete_batch(state, batch, :ok)
    end
  end

  defp handle_topic_create_result(state, batch, result) do
    case result do
      :ok ->
        Telemetry.topic_created(
          state.auto_create_partitions,
          state.auto_create_replication_factor,
          %{
            plugin_id: state.plugin_id,
            topic: state.topic
          }
        )

        schedule_retry(state, batch)

      {:error, :topic_already_exists} ->
        schedule_retry(state, batch)

      {:error, reason} ->
        fail_batch(state, batch, reason)
    end
  end

  defp issue_create_topic(state, batch) do
    parent = self()
    ref = batch.ref
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
      result = create_fn.(worker, topic, opts, timeout)
      send(parent, {:topic_create_result, ref, result})
    end)

    :ok
  end

  defp issue_metadata_refresh(state, batch) do
    parent = self()
    ref = batch.ref
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

      send(parent, {:metadata_refresh_done, ref, now_fn.() - started, err})
    end)

    :ok
  end

  defp schedule_retry(state, batch) do
    backoff = retry_backoff_ms(state, batch.attempt)
    timer = Process.send_after(self(), {:retry_batch, batch.ref}, backoff)
    %{state | retry_timer: timer}
  end

  defp retry_backoff_ms(state, attempt) do
    base = trunc(state.retry_backoff_ms * :math.pow(2, max(attempt - 1, 0)))
    jitter = state.jitter_fn.(100)
    min(base + jitter, state.retry_max_backoff_ms)
  end

  defp fail_batch(state, batch, reason) do
    emit_failed_per_record(state, batch, batch.attempt + 1, reason)
    complete_batch(state, batch, {:error, {:delivery_failed, reason}})
  end

  defp emit_failed_per_record(state, batch, attempts, reason) do
    total_ms = elapsed_ms(state, batch)

    Enum.each(batch.records, fn r ->
      Telemetry.failed(attempts, total_ms, %{
        plugin_id: state.plugin_id,
        topic: state.topic,
        partition: state.partition,
        last_reason: reason,
        caller: caller_tag(r)
      })
    end)
  end

  defp caller_tag(%{from: :async}), do: :async
  defp caller_tag(_), do: :sync

  defp complete_batch(state, batch, reply) do
    Enum.each(batch.records, fn r -> reply_to_record(r, reply) end)

    state = %{
      state
      | in_flight: nil,
        last_error:
          case reply do
            :ok -> nil
            {:error, {_, reason}} -> reason
          end
    }

    state = process_blocked(state)
    maybe_kick_batch(state)
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

  defp elapsed_ms(state, batch), do: state.now_fn.() - batch.first_attempt_at

  defp in_flight_size(%{in_flight: nil}), do: 0
  defp in_flight_size(%{in_flight: %{records: r}}), do: length(r)

  defp build_record(msg, metadata, from, timestamp_ms) do
    # `flezha.dedup_key` (set by the SDK runtime for any emit issued
    # during a `state.apply(...)`-driven `Shard::run`) wins over a
    # user-supplied `kafka_msg_key`. The contract: the framework owns
    # the dedup key for stateful relays so retries land on the same
    # Kafka partition — bypass-able by users only via a different
    # metadata key (see `relay-state-persistence-controller` §11).
    key =
      Map.get(metadata, "flezha.dedup_key") ||
        Map.get(metadata, "kafka_msg_key", nil)

    %{
      from: from,
      value: msg,
      key: key,
      metadata: metadata,
      timestamp: timestamp_ms,
      bytes: byte_size_or(0, msg) + byte_size_or(0, key)
    }
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
    KafkaClient.send_request(worker_pid, request, node_selector)
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
      timeout: opts.timeout_ms,
      create_topic_requests: [
        %{
          topic: topic,
          num_partitions: opts.num_partitions,
          replication_factor: opts.replication_factor,
          replica_assignment: [],
          config_entries:
            Enum.map(opts.config_entries, fn
              {k, v} -> %{config_name: to_string(k), config_value: to_string(v)}
              %{config_name: _, config_value: _} = m -> m
            end)
        }
      ]
    }

    case KafkaClient.send_request(worker_pid, request, NodeSelector.controller(), timeout_ms) do
      {:ok, %{topic_errors: errors}} ->
        case Enum.find(errors, fn e ->
               Map.get(e, :topic) == topic or Map.get(e, :topic_name) == topic
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
    if is_reference(state.retry_timer), do: Process.cancel_timer(state.retry_timer)
    %{state | linger_timer: nil, retry_timer: nil}
  end

  defp rewind_in_flight(%{in_flight: nil} = state, _reason), do: state

  defp rewind_in_flight(%{in_flight: batch} = state, reason) do
    requeued = :queue.join(:queue.from_list(batch.records), state.queue)
    bytes_back = Enum.reduce(batch.records, 0, fn r, n -> n + r.bytes end)

    %{
      state
      | queue: requeued,
        queue_size: state.queue_size + length(batch.records),
        queue_bytes: state.queue_bytes + bytes_back,
        in_flight: nil,
        last_error: {:worker_died, reason}
    }
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
            in_flight: nil,
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

      Telemetry.failed(state.swap_attempt + 1, state.delivery_timeout_ms, %{
        plugin_id: state.plugin_id,
        topic: state.topic,
        partition: state.partition,
        last_reason: {:worker_swap_exhausted, swap_reason},
        caller: caller_tag(record)
      })
    end)

    {:noreply,
     %{
       state
       | queue: :queue.new(),
         queue_size: 0,
         queue_bytes: 0,
         in_flight: nil,
         swap_started_at: nil,
         swap_attempt: 0,
         last_error: {:swap_exhausted, swap_reason},
         pending_swap_telemetry: nil
     }}
  end

  defp emit_pending_swap_telemetry(%{pending_swap_telemetry: nil} = state), do: state

  defp emit_pending_swap_telemetry(%{pending_swap_telemetry: payload} = state) do
    metadata = Map.put(payload.metadata, :producer_id_after, state.producer_id)
    Telemetry.worker_swapped(payload.measurements, metadata)
    %{state | pending_swap_telemetry: nil}
  end

  @impl true
  def terminate(_reason, %{registry_owned?: true, registry_ref: ref}) when not is_nil(ref) do
    _ = WorkerRegistry.release(ref)
    :ok
  end

  def terminate(_reason, _state), do: :ok
end
