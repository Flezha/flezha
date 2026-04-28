defmodule FlezhaTest.Plugin.Kafka.ProducerWorkerTest do
  alias Flezha.Plugin.Provider.Kafka.ProducerWorker

  use ExUnit.Case, async: false

  @topic "p_topic"
  @partition 0

  defmodule Fakes do
    def init_producer_id_ok(producer_id \\ 7777, epoch \\ 0) do
      fn _worker, _timeout ->
        {:ok, %{producer_id: producer_id, producer_epoch: epoch}}
      end
    end

    def init_producer_id_fail(reason) do
      fn _worker, _timeout -> {:error, reason} end
    end

    def metadata_refresh_ok do
      fn _worker, _timeout -> :ok end
    end

    def create_topic_ok do
      fn _worker, _topic, _opts, _timeout -> :ok end
    end

    def create_topic_already_exists do
      fn _worker, _topic, _opts, _timeout -> {:error, :topic_already_exists} end
    end

    def script_send_fn(test_pid, script_pid) do
      fn worker, request, node_selector ->
        send(test_pid, {:send_called, request, node_selector})
        n = Agent.get_and_update(script_pid, fn [head | tail] -> {head, tail} end)
        case n do
          {:return, value} -> value
          {:return_fn, fun} -> fun.(worker, request, node_selector)
        end
      end
    end

    def ok_response(topic, partition, base_offset \\ 100) do
      {:ok,
       %Kayrock.Produce.V5.Response{
         responses: [
           %{
             topic: topic,
             partition_responses: [
               %{
                 partition: partition,
                 error_code: 0,
                 base_offset: base_offset,
                 log_append_time: -1,
                 log_start_offset: -1
               }
             ]
           }
         ],
         throttle_time_ms: 0
       }}
    end

    def err_response(topic, partition, error_code) do
      {:ok,
       %Kayrock.Produce.V5.Response{
         responses: [
           %{
             topic: topic,
             partition_responses: [
               %{
                 partition: partition,
                 error_code: error_code,
                 base_offset: -1,
                 log_append_time: -1,
                 log_start_offset: -1
               }
             ]
           }
         ],
         throttle_time_ms: 0
       }}
    end
  end

  defp start_worker(opts) do
    test_pid = self()

    {:ok, script_pid} = Agent.start_link(fn -> Keyword.get(opts, :script, []) end)

    base = [
      worker_pid: spawn(fn -> Process.sleep(:infinity) end),
      topic: @topic,
      partition: @partition,
      plugin_id: "test-#{System.unique_integer([:positive])}",
      linger_ms: Keyword.get(opts, :linger_ms, 5),
      max_batch_bytes: Keyword.get(opts, :max_batch_bytes, 1_048_576),
      queue_max: Keyword.get(opts, :queue_max, 1_000),
      queue_overflow: Keyword.get(opts, :queue_overflow, :drop_oldest),
      queue_block_timeout_ms: Keyword.get(opts, :queue_block_timeout_ms, 5_000),
      retries: Keyword.get(opts, :retries, 5),
      retry_backoff_ms: Keyword.get(opts, :retry_backoff_ms, 5),
      retry_max_backoff_ms: Keyword.get(opts, :retry_max_backoff_ms, 50),
      delivery_timeout_ms: Keyword.get(opts, :delivery_timeout_ms, 1_000),
      idempotent: Keyword.get(opts, :idempotent, true),
      auto_create_topic: Keyword.get(opts, :auto_create_topic, true),
      metadata_refresh_timeout_ms: 100,
      send_fn: Fakes.script_send_fn(test_pid, script_pid),
      init_producer_id_fn:
        Keyword.get(opts, :init_producer_id_fn, Fakes.init_producer_id_ok()),
      metadata_refresh_fn:
        Keyword.get(opts, :metadata_refresh_fn, Fakes.metadata_refresh_ok()),
      create_topic_fn: Keyword.get(opts, :create_topic_fn, Fakes.create_topic_ok()),
      jitter_fn: fn _ -> 0 end,
      now_fn: Keyword.get(opts, :now_fn, fn -> System.monotonic_time(:millisecond) end),
      name: nil
    ]

    {:ok, pid} =
      GenServer.start_link(ProducerWorker, base ++ Keyword.get(opts, :extra, []))

    {pid, script_pid}
  end

  test "successful enqueue replies :ok and emits produce.ok telemetry" do
    {pid, _script} =
      start_worker(script: [{:return, Fakes.ok_response(@topic, @partition, 42)}])

    attach(:produce_ok)
    assert :ok = ProducerWorker.enqueue(pid, "hello", %{})
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :ok], measurements, metadata}
    assert measurements.batch_size == 1
    assert metadata.topic == @topic
  end

  test "InitProducerId result is cached and used in subsequent batches" do
    pid_id = 4242
    epoch = 5

    {pid, _} =
      start_worker(
        init_producer_id_fn: Fakes.init_producer_id_ok(pid_id, epoch),
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_receive {:send_called, request, _selector}
    [%{data: [%{record_set: rb}]}] = request.topic_data
    assert rb.producer_id == pid_id
    assert rb.producer_epoch == epoch
    assert rb.base_sequence == 0
  end

  test "linger_ms batches enqueues into one request" do
    {pid, _} =
      start_worker(
        linger_ms: 50,
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    callers =
      for i <- 1..10 do
        Task.async(fn -> ProducerWorker.enqueue(pid, "msg-#{i}", %{}) end)
      end

    Task.await_many(callers, 2_000)

    assert_received {:send_called, request, _}
    refute_received {:send_called, _, _}
    [%{data: [%{record_set: rb}]}] = request.topic_data
    assert length(rb.records) == 10
  end

  test "retriable error is retried with same base_sequence under idempotence" do
    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        script: [
          {:return, Fakes.err_response(@topic, @partition, 6)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    attach(:produce_retry)
    assert :ok = ProducerWorker.enqueue(pid, "hello", %{})

    assert_received {:send_called, req1, _}
    assert_received {:send_called, req2, _}
    [%{data: [%{record_set: rb1}]}] = req1.topic_data
    [%{data: [%{record_set: rb2}]}] = req2.topic_data
    assert rb1.base_sequence == rb2.base_sequence

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :retry], _, %{reason: :not_leader_for_partition}}
  end

  test "permanent error returns {:error, {:permanent, _}}" do
    {pid, _} =
      start_worker(script: [{:return, Fakes.err_response(@topic, @partition, 18)}])

    attach(:produce_failed)
    assert {:error, {:permanent, :record_list_too_large}} = ProducerWorker.enqueue(pid, "x", %{})

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :failed], _, %{last_reason: :record_list_too_large}}
  end

  test "delivery_timeout exhaustion returns {:error, {:delivery_failed, _}}" do
    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        retry_max_backoff_ms: 5,
        delivery_timeout_ms: 30,
        retries: 100,
        script: List.duplicate({:return, {:error, :no_broker}}, 200)
      )

    assert {:error, {:delivery_failed, :no_broker}} = ProducerWorker.enqueue(pid, "x", %{})
  end

  test "drop-oldest evicts head and admits new tail under saturation" do
    blocking_send = fn _w, _r, _ns ->
      Process.sleep(150)
      Fakes.ok_response(@topic, @partition)
    end

    {pid, script_pid} =
      start_worker(
        queue_max: 2,
        queue_overflow: :drop_oldest,
        linger_ms: 1
      )

    Agent.update(script_pid, fn _ ->
      [{:return_fn, blocking_send} | List.duplicate({:return, Fakes.ok_response(@topic, @partition)}, 10)]
    end)

    attach(:dropped)

    # Saturate: kick off the in-flight slow batch
    {first_caller, _} = spawn_monitor(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    Process.sleep(20)

    # Fill queue to capacity
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-1", %{}) end)
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-2", %{}) end)
    Process.sleep(10)

    # This one will trigger drop_oldest
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-3", %{}) end)

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :dropped], %{count: 1}, %{overflow_strategy: :drop_oldest}}, 1000

    Process.exit(first_caller, :kill)
  end

  test "drop-newest rejects new and replies overflow" do
    blocking_send = fn _w, _r, _ns ->
      Process.sleep(150)
      Fakes.ok_response(@topic, @partition)
    end

    {pid, script_pid} =
      start_worker(
        queue_max: 1,
        queue_overflow: :drop_newest,
        linger_ms: 1
      )

    Agent.update(script_pid, fn _ ->
      [{:return_fn, blocking_send} | List.duplicate({:return, Fakes.ok_response(@topic, @partition)}, 10)]
    end)

    spawn(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    Process.sleep(10)

    spawn(fn -> ProducerWorker.enqueue(pid, "filler", %{}) end)
    Process.sleep(10)

    assert {:error, {:queue_full, :overflow}} = ProducerWorker.enqueue(pid, "rejected", %{})
  end

  test "auto_create_topic on :unknown_topic_or_partition issues CreateTopics and retries" do
    create_called = self()

    create_fn = fn _worker, topic, _opts, _timeout ->
      send(create_called, {:create_called, topic})
      :ok
    end

    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        create_topic_fn: create_fn,
        auto_create_topic: true,
        script: [
          {:return, Fakes.err_response(@topic, @partition, 3)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    attach(:topic_created)
    assert :ok = ProducerWorker.enqueue(pid, "hi", %{})
    assert_receive {:create_called, @topic}
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :topic, :created], _, _}
  end

  test "auto_create_topic = false drops :unknown_topic_or_partition immediately" do
    {pid, _} =
      start_worker(
        auto_create_topic: false,
        script: [{:return, Fakes.err_response(@topic, @partition, 3)}]
      )

    assert {:error, {:delivery_failed, :unknown_topic_or_partition}} =
             ProducerWorker.enqueue(pid, "hi", %{})
  end

  test ":not_leader_for_partition triggers metadata refresh before retry" do
    refreshed_pid = self()

    refresh_fn = fn _worker, _timeout ->
      send(refreshed_pid, :refresh_called)
      :ok
    end

    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        metadata_refresh_fn: refresh_fn,
        script: [
          {:return, Fakes.err_response(@topic, @partition, 6)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    attach(:metadata_refresh)
    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    assert_receive :refresh_called
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :metadata, :refresh], _, %{trigger: :error_recovery, error: nil}}
  end

  test "stats/1 reports counters and last_error" do
    {pid, _} =
      start_worker(
        script: [
          {:return, Fakes.err_response(@topic, @partition, 18)}
        ]
      )

    {:error, {:permanent, _}} = ProducerWorker.enqueue(pid, "x", %{})

    stats = ProducerWorker.stats(pid)
    assert stats.dropped_total == 0
    assert stats.queue_depth == 0
    assert stats.in_flight == 0
    assert stats.last_error == :record_list_too_large
  end

  test "non-idempotent mode skips InitProducerId and uses producer_id = -1" do
    test_pid = self()

    init_fn = fn _w, _t ->
      send(test_pid, :init_called)
      {:ok, %{producer_id: 1, producer_epoch: 1}}
    end

    {pid, _} =
      start_worker(
        idempotent: false,
        init_producer_id_fn: init_fn,
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    refute_received :init_called

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_received {:send_called, request, _}
    [%{data: [%{record_set: rb}]}] = request.topic_data
    assert rb.producer_id == -1
    assert rb.base_sequence == -1
  end

  test "InitProducerId failure falls back to non-idempotent mode" do
    {pid, _} =
      start_worker(
        idempotent: true,
        init_producer_id_fn: Fakes.init_producer_id_fail(:no_coordinator),
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_received {:send_called, request, _}
    [%{data: [%{record_set: rb}]}] = request.topic_data
    assert rb.producer_id == -1
    assert rb.base_sequence == -1
  end

  test "linger_ms = 0 disables batching — one Produce per enqueue" do
    {pid, _} =
      start_worker(
        linger_ms: 0,
        script: [
          {:return, Fakes.ok_response(@topic, @partition)},
          {:return, Fakes.ok_response(@topic, @partition)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    assert :ok = ProducerWorker.enqueue(pid, "a", %{})
    assert :ok = ProducerWorker.enqueue(pid, "b", %{})
    assert :ok = ProducerWorker.enqueue(pid, "c", %{})

    # Three separate sends.
    assert_received {:send_called, _, _}
    assert_received {:send_called, _, _}
    assert_received {:send_called, _, _}
    refute_received {:send_called, _, _}
  end

  test ":no_broker triggers metadata refresh before retry" do
    refreshed = self()

    refresh_fn = fn _w, _t ->
      send(refreshed, :refresh_called)
      :ok
    end

    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        metadata_refresh_fn: refresh_fn,
        script: [
          {:return, {:error, :no_broker}},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    attach(:metadata_refresh)
    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    assert_receive :refresh_called
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :metadata, :refresh], _,
                    %{trigger: :error_recovery}}
  end

  test "topic create failure fails the batch" do
    create_fn = fn _w, _t, _o, _to -> {:error, :invalid_replication_factor} end

    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        create_topic_fn: create_fn,
        script: [{:return, Fakes.err_response(@topic, @partition, 3)}]
      )

    assert {:error, {:delivery_failed, :invalid_replication_factor}} =
             ProducerWorker.enqueue(pid, "hi", %{})
  end

  test "duplicate_sequence_number is treated as already delivered" do
    # error_code 45 = duplicate_sequence_number
    {pid, _} =
      start_worker(
        idempotent: true,
        script: [{:return, Fakes.err_response(@topic, @partition, 45)}]
      )

    attach(:produce_ok)
    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :ok], _, _}
  end

  test "max_batch_bytes triggers immediate flush even within linger window" do
    {pid, _} =
      start_worker(
        linger_ms: 5_000,
        max_batch_bytes: 30,
        script: [
          {:return, Fakes.ok_response(@topic, @partition)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    # Enqueue messages totalling > 30 bytes — should flush mid-window without
    # waiting the full 5 seconds.
    Task.async(fn -> ProducerWorker.enqueue(pid, String.duplicate("a", 20), %{}) end)
    Task.async(fn -> ProducerWorker.enqueue(pid, String.duplicate("b", 20), %{}) end)

    assert_receive {:send_called, _, _}, 1_000
  end

  test "block mode applies back-pressure with timeout" do
    # Send_fn never returns — guarantees the in-flight batch is still pending
    # when the block-timeout fires for the queued enqueue.
    forever_send = fn _w, _r, _ns -> Process.sleep(:infinity) end

    {pid, script_pid} =
      start_worker(
        queue_max: 1,
        queue_overflow: :block,
        queue_block_timeout_ms: 50,
        linger_ms: 1
      )

    Agent.update(script_pid, fn _ ->
      List.duplicate({:return_fn, forever_send}, 4)
    end)

    # Establish in-flight (first) and a queued message (filler).
    spawn(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    Process.sleep(20)
    spawn(fn -> ProducerWorker.enqueue(pid, "filler", %{}) end)
    Process.sleep(20)

    # The queue is now full (filler) and in-flight (first) never completes;
    # this third enqueue must timeout via the block path.
    assert {:error, {:queue_full, :timeout}} =
             ProducerWorker.enqueue(pid, "blocked", %{}, timeout: 1_000)
  end

  test "stale batch_result is ignored after batch completes" do
    {pid, _} =
      start_worker(
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    # Inject a stale batch_result message — should be a no-op.
    send(pid, {:batch_result, make_ref(), Fakes.ok_response(@topic, @partition), 0})
    Process.sleep(10)

    # Worker still alive and responsive.
    assert ProducerWorker.stats(pid).queue_depth == 0
  end

  defp attach(tag) do
    event =
      case tag do
        :produce_ok -> [:flezha, :plugin, :kafka, :produce, :ok]
        :produce_retry -> [:flezha, :plugin, :kafka, :produce, :retry]
        :produce_failed -> [:flezha, :plugin, :kafka, :produce, :failed]
        :dropped -> [:flezha, :plugin, :kafka, :produce, :dropped]
        :metadata_refresh -> [:flezha, :plugin, :kafka, :metadata, :refresh]
        :topic_created -> [:flezha, :plugin, :kafka, :topic, :created]
      end

    handler_id = "test-#{System.unique_integer([:positive])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn name, measurements, metadata, _ ->
          send(test_pid, {:telemetry, name, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end
