defmodule FlezhaTest.Plugin.Kafka.ProducerWorkerTest do
  alias Flezha.Plugin.Provider.Kafka.ProducerWorker
  alias Flezha.Plugin.Provider.Kafka.WorkerRegistry
  alias FlezhaTest.Kafka.ProducerFakes, as: Fakes

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use ExUnit.Case, async: false
  use Mimic

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 3]

  import ExUnit.CaptureLog

  setup :set_mimic_global

  @topic "p_topic"
  @partition 0

  defp window_records(state) do
    Enum.reduce(state.in_flight, 0, fn batch, n -> n + length(batch.records) end)
  end

  # The worker's own accounting is the settle signal for a caller that
  # enqueues from another process.
  defp in_flight?(pid, n),
    do: wait_until(fn -> ProducerWorker.stats(pid).in_flight == n end, 1_000, 5)

  defp queued?(pid, n),
    do: wait_until(fn -> ProducerWorker.stats(pid).queue_depth == n end, 1_000, 5)

  defp dispatched?(pid),
    do: wait_until(fn -> ProducerWorker.stats(pid).in_flight >= 1 end, 1_000, 5)

  defp start_worker(opts) do
    test_pid = self()

    {:ok, script_pid} = Agent.start_link(fn -> Keyword.get(opts, :script, []) end)

    base = [
      worker_pid: spawn(&FlezhaTest.Helper.NodeTestHelper.park/0),
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
      max_in_flight: Keyword.get(opts, :max_in_flight, 1),
      auto_create_topic: Keyword.get(opts, :auto_create_topic, true),
      metadata_refresh_timeout_ms: 100,
      send_fn: Keyword.get(opts, :send_fn) || Fakes.script_send_fn(test_pid, script_pid),
      init_producer_id_fn: Keyword.get(opts, :init_producer_id_fn, Fakes.init_producer_id_ok()),
      metadata_refresh_fn: Keyword.get(opts, :metadata_refresh_fn, Fakes.metadata_refresh_ok()),
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

  describe "effectively-once tier records (§5)" do
    @tier_metadata %{
      "channel" => "out",
      "flz.entity" => "MarketOperative/s1/k1",
      "flz.seq" => "42.0",
      "flz.epoch" => "1",
      "flz.key" => "BTC/1m/1720000000"
    }

    test "flz.key is the message key and the identity header rides along" do
      {pid, _} = start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      assert :ok = ProducerWorker.enqueue(pid, "candle", @tier_metadata)
      assert_receive {:send_called, request, _selector}
      [%{data: [%{record_set: rb}]}] = request.topic_data
      [record] = rb.records

      assert record.key == "BTC/1m/1720000000"

      assert [%Kayrock.RecordBatch.RecordHeader{key: "flezha.dedup_key", value: dedup}] =
               record.headers

      assert dedup == "MarketOperative/s1/k1/42.0/1"
      refute dedup =~ "processor_"
      refute dedup =~ "compute_engine_"
    end

    test "a re-driven record carries identical key, header, and partition" do
      {pid, _} =
        start_worker(
          script: [
            {:return, Fakes.ok_response(@topic, @partition)},
            {:return, Fakes.ok_response(@topic, @partition)}
          ]
        )

      assert :ok = ProducerWorker.enqueue(pid, "candle", @tier_metadata)
      assert_receive {:send_called, req1, _}
      # Restart re-drive re-publishes the same durable identity.
      assert :ok = ProducerWorker.enqueue(pid, "candle", @tier_metadata)
      assert_receive {:send_called, req2, _}

      [%{data: [%{partition: p1, record_set: rb1}]}] = req1.topic_data
      [%{data: [%{partition: p2, record_set: rb2}]}] = req2.topic_data
      [r1] = rb1.records
      [r2] = rb2.records

      assert p1 == p2
      assert r1.key == r2.key
      assert r1.headers == r2.headers
      assert r1.value == r2.value
    end

    test "at-least-once records keep the caller key and carry no headers" do
      {pid, _} = start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      assert :ok = ProducerWorker.enqueue(pid, "v", %{"kafka_msg_key" => "user-key"})
      assert_receive {:send_called, request, _}
      [%{data: [%{record_set: rb}]}] = request.topic_data
      [record] = rb.records

      assert record.key == "user-key"
      assert record.headers == []
    end

    test "a keyed channel outside the tier still partitions by its natural key" do
      {pid, _} = start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      # What an ordinary `.keyed()` hybrid channel stamps: entity + seq + key,
      # but no `flz.epoch` — only the effectively-once tier stamps that. A nil
      # key here scatters one entity's records across partitions, losing the
      # ordering `.keyed()` was declared for.
      assert :ok =
               ProducerWorker.enqueue(pid, "candle", %{
                 "channel" => "ohlc_out",
                 "flz.entity" => "MarketOperative/ohlc/BTC-USD",
                 "flz.seq" => "42.0",
                 "flz.key" => "BTC/USD"
               })

      assert_receive {:send_called, request, _}
      [%{data: [%{record_set: rb}]}] = request.topic_data
      [record] = rb.records

      assert record.key == "BTC/USD"
      assert record.headers == [], "no durable identity outside the tier, so no dedup header"
    end
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

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :retry], _,
                    %{reason: :not_leader_for_partition}}
  end

  test "permanent error returns {:error, {:permanent, _}}" do
    {pid, _} =
      start_worker(script: [{:return, Fakes.err_response(@topic, @partition, 18)}])

    attach(:produce_failed)
    assert {:error, {:permanent, :record_list_too_large}} = ProducerWorker.enqueue(pid, "x", %{})

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :failed], _,
                    %{last_reason: :record_list_too_large}}
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
    {pid, script_pid} =
      start_worker(
        queue_max: 2,
        queue_overflow: :drop_oldest,
        linger_ms: 1
      )

    Agent.update(script_pid, fn _ ->
      [
        {:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}
        | List.duplicate({:return, Fakes.ok_response(@topic, @partition)}, 10)
      ]
    end)

    attach(:dropped)

    # Saturate: kick off the in-flight slow batch
    {first_caller, _} = spawn_monitor(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    assert in_flight?(pid, 1)

    # Fill queue to capacity
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-1", %{}) end)
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-2", %{}) end)
    assert queued?(pid, 2)

    # This one will trigger drop_oldest
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-3", %{}) end)

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :dropped], %{count: 1},
                    %{overflow_strategy: :drop_oldest}},
                   1000

    Process.exit(first_caller, :kill)
  end

  test "drop-newest rejects new and replies overflow" do
    {pid, script_pid} =
      start_worker(
        queue_max: 1,
        queue_overflow: :drop_newest,
        linger_ms: 1
      )

    Agent.update(script_pid, fn _ ->
      [
        {:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}
        | List.duplicate({:return, Fakes.ok_response(@topic, @partition)}, 10)
      ]
    end)

    spawn(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    assert in_flight?(pid, 1)

    spawn(fn -> ProducerWorker.enqueue(pid, "filler", %{}) end)
    assert queued?(pid, 1)

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

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :metadata, :refresh], _,
                    %{trigger: :error_recovery, error: nil}}
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

  test "duplicate_sequence_number is treated as already delivered (no re-init, no resend)" do
    # error_code 46 = duplicate_sequence_number — the batch is genuinely stored.
    test_pid = self()

    {pid, _} =
      start_worker(
        idempotent: true,
        init_producer_id_fn: fn _w, _t ->
          send(test_pid, :init_called)
          {:ok, %{producer_id: 7777, producer_epoch: 0}}
        end,
        script: [{:return, Fakes.err_response(@topic, @partition, 46)}]
      )

    # The boot-time InitProducerId fires once; drain it before asserting no re-init.
    assert_receive :init_called

    attach(:produce_ok)
    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :ok], _, _}

    # Exactly one send (the original) — a duplicate must not re-init or resend.
    assert_received {:send_called, _, _}
    refute_received {:send_called, _, _}
    refute_received :init_called
  end

  for {label, code} <- [out_of_order_sequence_number: 45, invalid_producer_epoch: 47] do
    test "#{label} fence re-inits the producer session and resends without acking the fence" do
      test_pid = self()
      {:ok, epoch_counter} = Agent.start_link(fn -> 0 end)

      # Boot init returns epoch 0; the post-fence re-init returns epoch 1 so we
      # can prove the resend rode a fresh session.
      init_fn = fn _w, _t ->
        epoch = Agent.get_and_update(epoch_counter, fn n -> {n, n + 1} end)
        send(test_pid, {:init_called, epoch})
        {:ok, %{producer_id: 7777, producer_epoch: epoch}}
      end

      {pid, _} =
        start_worker(
          idempotent: true,
          retry_backoff_ms: 1,
          init_producer_id_fn: init_fn,
          script: [
            {:return, Fakes.err_response(@topic, @partition, unquote(code))},
            {:return, Fakes.ok_response(@topic, @partition)}
          ]
        )

      assert_receive {:init_called, 0}

      attach(:produce_ok)
      assert :ok = ProducerWorker.enqueue(pid, "hello", %{})

      # The fence drove a second InitProducerId (session re-init).
      assert_receive {:init_called, 1}

      # Two sends: the fenced original, then the resend on the fresh session
      # with sequence realigned to 0.
      assert_received {:send_called, req1, _}
      assert_received {:send_called, req2, _}
      [%{data: [%{record_set: rb1}]}] = req1.topic_data
      [%{data: [%{record_set: rb2}]}] = req2.topic_data
      assert rb1.producer_epoch == 0
      assert rb2.producer_epoch == 1
      assert rb2.base_sequence == 0

      # Delivered on the resend — never a fake ack on the fence itself.
      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :ok], _, _}
    end
  end

  test "non-idempotent producer (the default) treats a transient hiccup as a plain retry" do
    test_pid = self()

    {pid, _} =
      start_worker(
        idempotent: false,
        retry_backoff_ms: 1,
        init_producer_id_fn: fn _w, _t ->
          send(test_pid, :init_called)
          {:ok, %{producer_id: 1, producer_epoch: 1}}
        end,
        script: [
          # 6 = not_leader_for_partition (transient), then ok.
          {:return, Fakes.err_response(@topic, @partition, 6)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    refute_received :init_called

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    assert_received {:send_called, req1, _}
    [%{data: [%{record_set: rb1}]}] = req1.topic_data
    assert rb1.producer_id == -1
    assert rb1.base_sequence == -1
  end

  test "a fence the worker cannot recover surfaces :produce_stalled health after exhaustion" do
    {pid, _} =
      start_worker(
        idempotent: true,
        retries: 1,
        retry_backoff_ms: 1,
        delivery_timeout_ms: 1_000,
        # Every send fences — each batch exhausts its retry and fails.
        script: List.duplicate({:return, Fakes.err_response(@topic, @partition, 45)}, 30)
      )

    attach(:produce_failed)

    # Each synchronous enqueue is its own batch; three failed batches drive
    # consecutive_failures past the health threshold.
    for _ <- 1..3 do
      assert {:error, {:delivery_failed, :out_of_order_sequence_number}} =
               ProducerWorker.enqueue(pid, "x", %{})
    end

    assert_received {:telemetry, [:flezha, :plugin, :kafka, :produce, :failed], _, _}

    # The producer now reports :produce_stalled on its health surface; the
    # in-process maybe_refresh_broker/2 path drives recovery on batch failure.
    stats = ProducerWorker.stats(pid)
    assert stats.consecutive_failures >= 3

    assert {:unhealthy, :produce_stalled} =
             Flezha.Plugin.Provider.Kafka.Producer.health(%{producer_pid: pid})
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
    # The dispatch is never answered — the batch is guaranteed to still be in
    # flight when the block-timeout fires for the queued enqueue.
    {pid, script_pid} =
      start_worker(
        queue_max: 1,
        queue_overflow: :block,
        queue_block_timeout_ms: 50,
        linger_ms: 1
      )

    Agent.update(script_pid, fn _ ->
      List.duplicate({:return, :never}, 4)
    end)

    # Establish in-flight (first) and a queued message (filler).
    spawn(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    assert in_flight?(pid, 1)
    spawn(fn -> ProducerWorker.enqueue(pid, "filler", %{}) end)
    assert queued?(pid, 1)

    # The queue is now full (filler) and in-flight (first) never completes;
    # this third enqueue must timeout via the block path.
    assert {:error, {:queue_full, :timeout}} =
             ProducerWorker.enqueue(pid, "blocked", %{}, timeout: 1_000)
  end

  test "an outcome for an unknown dispatch is ignored after its batch completes" do
    {pid, _} =
      start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    # Inject an outcome for a dispatch the window never issued — a no-op.
    send(pid, {:kafka_ex_response, make_ref(), Fakes.ok_response(@topic, @partition)})

    # Worker still alive and responsive (the call queues behind the message).
    assert ProducerWorker.stats(pid).queue_depth == 0
  end

  describe "cast_enqueue/3" do
    test "admission below queue_max returns :ok and queue depth increases" do
      {pid, script_pid} =
        start_worker(
          queue_max: 100,
          linger_ms: 1
        )

      Agent.update(script_pid, fn _ ->
        List.duplicate({:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}, 5)
      end)

      assert :ok = ProducerWorker.cast_enqueue(pid, "first", %{})
      # The linger fires and the first record goes out alone.
      assert in_flight?(pid, 1)

      assert :ok = ProducerWorker.cast_enqueue(pid, "msg-1", %{})
      assert :ok = ProducerWorker.cast_enqueue(pid, "msg-2", %{})
      assert :ok = ProducerWorker.cast_enqueue(pid, "msg-3", %{})
      assert queued?(pid, 3)

      stats = ProducerWorker.stats(pid)
      assert stats.queue_depth == 3
      assert stats.in_flight == 1
    end

    test "sustained casts above drain rate trigger drop-oldest telemetry; mailbox stays bounded" do
      {pid, script_pid} =
        start_worker(
          queue_max: 5,
          queue_overflow: :drop_oldest,
          linger_ms: 1
        )

      Agent.update(script_pid, fn _ ->
        [{:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}]
      end)

      attach(:dropped)

      :ok = ProducerWorker.cast_enqueue(pid, "first", %{})
      assert in_flight?(pid, 1)

      for i <- 1..200 do
        :ok = ProducerWorker.cast_enqueue(pid, "msg-#{i}", %{})
      end

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :dropped], %{count: 1},
                      %{overflow_strategy: :drop_oldest}},
                     500

      # Every cast has been handled once a call queued behind them answers.
      _ = ProducerWorker.stats(pid)
      {:message_queue_len, qlen} = Process.info(pid, :message_queue_len)
      assert qlen <= 5

      # One in flight, five queued, the other 195 evicted — and readable.
      stats = ProducerWorker.stats(pid)
      assert stats.queue_depth == 5
      assert stats.dropped_total == 195
    end

    test "drop-newest under cast emits dropped telemetry without replying" do
      {pid, script_pid} =
        start_worker(
          queue_max: 1,
          queue_overflow: :drop_newest,
          linger_ms: 1
        )

      Agent.update(script_pid, fn _ ->
        [{:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}]
      end)

      attach(:dropped)

      :ok = ProducerWorker.cast_enqueue(pid, "first", %{})
      assert in_flight?(pid, 1)
      :ok = ProducerWorker.cast_enqueue(pid, "filler", %{})
      assert queued?(pid, 1)
      :ok = ProducerWorker.cast_enqueue(pid, "rejected", %{})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :dropped], %{count: 1},
                      %{overflow_strategy: :drop_newest}}

      assert Process.alive?(pid)
      assert ProducerWorker.stats(pid).dropped_total == 1
    end

    test ":block on the cast path waits for room instead of dropping" do
      cast_path_waits_for_room(:block)
    end

    test "with no configured overflow policy a plain record waits instead of being dropped" do
      cast_path_waits_for_room(nil)
    end

    test "permanent broker error fires :failed with caller: :async; no GenServer.reply" do
      {pid, _} =
        start_worker(script: [{:return, Fakes.err_response(@topic, @partition, 18)}])

      attach(:produce_failed)

      assert :ok = ProducerWorker.cast_enqueue(pid, "x", %{})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :failed], _,
                      %{last_reason: :record_list_too_large, caller: :async}},
                     1_000

      assert Process.alive?(pid)
      stats = ProducerWorker.stats(pid)
      assert stats.queue_depth == 0
      assert stats.last_error == :record_list_too_large
    end

    test "delivery_timeout exhaustion on cast-admitted record fires :failed with caller: :async" do
      {pid, _} =
        start_worker(
          retry_backoff_ms: 1,
          retry_max_backoff_ms: 5,
          delivery_timeout_ms: 30,
          retries: 100,
          script: List.duplicate({:return, {:error, :no_broker}}, 200)
        )

      attach(:produce_failed)

      assert :ok = ProducerWorker.cast_enqueue(pid, "x", %{})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :failed], _,
                      %{last_reason: :no_broker, caller: :async}},
                     1_000

      assert Process.alive?(pid)
    end

    test "mixed sync + async records: sync gets reply, async gets telemetry" do
      {pid, _} =
        start_worker(
          linger_ms: 50,
          script: [{:return, Fakes.err_response(@topic, @partition, 18)}]
        )

      attach(:produce_failed)

      sync_caller =
        Task.async(fn ->
          ProducerWorker.enqueue(pid, "sync-msg", %{})
        end)

      # The sync record is queued (the linger is still open) when the async
      # one joins it: both ride one batch, one failure.
      assert queued?(pid, 1)
      :ok = ProducerWorker.cast_enqueue(pid, "async-msg", %{})

      assert {:error, {:permanent, :record_list_too_large}} = Task.await(sync_caller, 1_000)

      events = collect_failed_events(2, 500)
      callers = Enum.map(events, fn {_, _, _, md} -> md.caller end) |> Enum.sort()
      assert callers == [:async, :sync]
    end
  end

  defp collect_failed_events(0, _timeout), do: []

  defp collect_failed_events(n, timeout) do
    receive do
      {:telemetry, [:flezha, :plugin, :kafka, :produce, :failed], _, _} = ev ->
        [ev | collect_failed_events(n - 1, timeout)]
    after
      timeout -> []
    end
  end

  # Four plain records through a `queue_max: 1` worker whose first batch is
  # slow: the cast never blocks its caller, and nothing is evicted — the
  # records queue behind the in-flight batch and are produced once it drains.
  defp cast_path_waits_for_room(queue_overflow) do
    {pid, script_pid} =
      start_worker(queue_max: 1, queue_overflow: queue_overflow, linger_ms: 1)

    Agent.update(script_pid, fn _ ->
      [
        {:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}
        | List.duplicate({:return, Fakes.ok_response(@topic, @partition)}, 10)
      ]
    end)

    attach(:dropped)

    for payload <- ["first", "filler", "waits-1", "waits-2"] do
      :ok = ProducerWorker.cast_enqueue(pid, payload, %{})
    end

    # Every cast has been admitted (or evicted) once a call queued behind them
    # answers; a drop would have emitted before that.
    _ = ProducerWorker.stats(pid)
    refute_received {:telemetry, [:flezha, :plugin, :kafka, :produce, :dropped], _, _}

    sent =
      for _ <- 1..4, into: MapSet.new() do
        assert_receive {:send_called, request, _ns}, 2_000
        [%{data: [%{record_set: rb}]}] = request.topic_data
        Enum.map(rb.records, & &1.value)
      end
      |> Enum.concat()
      |> MapSet.new()

    assert MapSet.equal?(sent, MapSet.new(["first", "filler", "waits-1", "waits-2"]))
    assert ProducerWorker.stats(pid).dropped_total == 0
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
        :worker_swapped -> [:flezha, :plugin, :kafka, :worker, :swapped]
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

  describe "sustained produce stall forces a fresh broker connection (unified-convergence-loop)" do
    test "resumes producing after K give-ups without the shared worker dying" do
      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      identity = %{hosts: [{"k", 9092}]}
      fresh = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      Mimic.stub(WorkerRegistry, :force_replace, fn ^identity -> {:ok, fresh} end)

      # Two full batch give-ups (each exhausts retries fast), then the broker
      # is reachable again and the third batch succeeds — recovery rebinds onto
      # a freshly replaced client (no reliance on the swap fan-out).
      {pid, _} =
        start_worker(
          retry_backoff_ms: 1,
          retry_max_backoff_ms: 3,
          delivery_timeout_ms: 30,
          retries: 1,
          linger_ms: 1,
          script:
            List.duplicate({:return, {:error, :no_broker}}, 4) ++
              List.duplicate({:return, Fakes.ok_response(@topic, @partition, 500)}, 4),
          extra: [stall_threshold: 2, connection_identity: identity]
        )

      seed_registry_ref(pid, registry_ref)

      assert {:error, {:delivery_failed, :no_broker}} = ProducerWorker.enqueue(pid, "m1", %{})

      # A no-broker error is authoritative: the FIRST such batch forces a fresh
      # client immediately (throttled), resetting the counter — recovery no
      # longer waits for the stall threshold a low-traffic producer may never
      # reach.
      assert :sys.get_state(pid).consecutive_failures == 0

      assert {:error, {:delivery_failed, :no_broker}} = ProducerWorker.enqueue(pid, "m2", %{})

      # The throttle window blocks a second immediate refresh, so this give-up
      # only increments the counter (still below the stall threshold) — the
      # worker keeps draining on the same pid.
      assert :sys.get_state(pid).consecutive_failures == 1
      assert Process.alive?(pid)

      attach(:produce_ok)
      assert :ok = ProducerWorker.enqueue(pid, "m3", %{})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :ok], _, _}
      assert :sys.get_state(pid).consecutive_failures == 0
    end

    test "refresh_broker/1 cast forces a fresh connection on demand" do
      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      identity = %{hosts: [{"k", 9092}]}
      test_pid = self()
      fresh = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      Mimic.stub(WorkerRegistry, :force_replace, fn ^identity ->
        send(test_pid, :refresh_called)
        {:ok, fresh}
      end)

      {pid, _} =
        start_worker(
          script: [{:return, Fakes.ok_response(@topic, @partition)}],
          extra: [connection_identity: identity]
        )

      seed_registry_ref(pid, registry_ref)
      :sys.replace_state(pid, fn s -> %{s | consecutive_failures: 5} end)

      ProducerWorker.refresh_broker(pid)

      assert_receive :refresh_called, 500
      assert :sys.get_state(pid).consecutive_failures == 0
    end
  end

  describe "worker swap on {:kafka_worker_dead, ...} (task 2.7)" do
    defp seed_registry_ref(pid, ref) do
      :sys.replace_state(pid, fn state -> %{state | registry_ref: ref} end)
    end

    test "swap preserves queue and re-issues InitProducerId on the new worker (2.7.1 / 2.7.3)" do
      call_count = :counters.new(1, [])

      init_producer_id_fn = fn _w, _t ->
        :counters.add(call_count, 1, 1)

        case :counters.get(call_count, 1) do
          1 -> {:ok, %{producer_id: 4242, producer_epoch: 5}}
          _ -> {:ok, %{producer_id: 9999, producer_epoch: 7}}
        end
      end

      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: init_producer_id_fn,
          queue_max: 100,
          linger_ms: 5
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 10) end)

      :ok = ProducerWorker.cast_enqueue(pid, "a", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "b", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "c", %{})
      assert dispatched?(pid)

      pre = :sys.get_state(pid)
      assert pre.producer_id == 4242
      assert pre.producer_epoch == 5

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      seed_registry_ref(pid, registry_ref)

      new_worker_pid = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      Mimic.expect(WorkerRegistry, :swap, fn ^registry_ref ->
        {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
      end)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      post = :sys.get_state(pid)
      assert post.worker_pid == new_worker_pid
      assert post.producer_id == 9999
      assert post.producer_epoch == 7
      assert post.pending_swap_telemetry == nil

      # All three records still in-system (queue + in_flight).
      assert post.queue_size + window_records(post) == 3
    end

    test "re-admitted in_flight records retain from/metadata/timestamp/bytes/key (2.7.2)" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 1
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "payload-1", %{"k" => "v"})
      :ok = ProducerWorker.cast_enqueue(pid, "payload-2", %{"flezha.dedup_key" => "abc"})
      assert dispatched?(pid)

      pre = :sys.get_state(pid)
      in_flight_records = Enum.flat_map(pre.in_flight, & &1.records)
      assert length(in_flight_records) >= 1

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      seed_registry_ref(pid, registry_ref)
      new_worker_pid = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      Mimic.expect(WorkerRegistry, :swap, fn ^registry_ref ->
        {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
      end)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      post = :sys.get_state(pid)

      # After the swap, the rewound records may still be in queue OR have
      # been kicked into the new in_flight batch — assert they remain in
      # the producer's bookkeeping with their original fields intact.
      in_post = :queue.to_list(post.queue) ++ Enum.flat_map(post.in_flight, & &1.records)

      preserved = Enum.take(in_post, length(in_flight_records))

      assert Enum.map(preserved, & &1.value) == Enum.map(in_flight_records, & &1.value)
      assert Enum.map(preserved, & &1.metadata) == Enum.map(in_flight_records, & &1.metadata)
      assert Enum.map(preserved, & &1.timestamp) == Enum.map(in_flight_records, & &1.timestamp)
      assert Enum.map(preserved, & &1.bytes) == Enum.map(in_flight_records, & &1.bytes)
      assert Enum.map(preserved, & &1.key) == Enum.map(in_flight_records, & &1.key)
    end

    test "telemetry [:worker, :swapped] fires once with measurements + metadata (2.7.4)" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(1234, 0),
          linger_ms: 1
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "y", %{})
      assert dispatched?(pid)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      seed_registry_ref(pid, registry_ref)
      new_worker_pid = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      Mimic.expect(WorkerRegistry, :swap, fn ^registry_ref ->
        {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
      end)

      attach(:worker_swapped)
      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :worker, :swapped], measurements,
                      metadata},
                     500

      assert Map.has_key?(measurements, :queue_depth)
      assert Map.has_key?(measurements, :in_flight_records)
      assert Map.has_key?(measurements, :queue_bytes)
      assert metadata.topic == @topic
      assert metadata.partition == @partition
      _ = :sys.get_state(pid)
      assert metadata.reason == :killed
      assert metadata.producer_id_before == 1234
      assert metadata.new_worker_pid == new_worker_pid
      assert is_pid(metadata.old_worker_pid)
      assert is_integer(metadata.producer_id_after) or is_nil(metadata.producer_id_after)

      refute_received {:telemetry, [:flezha, :plugin, :kafka, :worker, :swapped], _, _}
    end

    test "no :produce :failed events fire for queued records during the swap window (2.7.5)" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 5
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "y", %{})
      assert dispatched?(pid)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      seed_registry_ref(pid, registry_ref)
      new_worker_pid = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      Mimic.expect(WorkerRegistry, :swap, fn ^registry_ref ->
        {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
      end)

      attach(:produce_failed)
      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      # The swap is handled ahead of this call; a failure would have fired inside it.
      _ = :sys.get_state(pid)
      refute_received {:telemetry, [:flezha, :plugin, :kafka, :produce, :failed], _, _}
    end

    test "defensive :EXIT from unrelated pid does not crash the producer (2.7.6)" do
      {pid, _script} =
        start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      assert Process.alive?(pid)

      # NOTE: send from a non-parent pid — gen_server.erl turns
      # `{:EXIT, parent_pid, :normal}` into a real terminate call.
      other = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)
      send(pid, {:EXIT, other, :killed})
      _ = :sys.get_state(pid)

      assert Process.alive?(pid),
             ":EXIT from unrelated pid must not crash the producer"
    end

    test "stale {:kafka_worker_dead, old_ref, _} is dropped without altering queue (2.7.7)" do
      {pid, _script} =
        start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      seed_registry_ref(pid, registry_ref)

      pre = :sys.get_state(pid)

      send(pid, {:kafka_worker_dead, {{[{"k", 9092}], 0, 0}, make_ref()}, :killed})

      post = :sys.get_state(pid)
      assert post.queue == pre.queue
      assert post.registry_ref == registry_ref
      assert Process.alive?(pid)
    end

    test "swap failure schedules retry within delivery_timeout_ms (2.4 happy retry)" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 1,
          retry_backoff_ms: 1,
          delivery_timeout_ms: 10_000
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      assert dispatched?(pid)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      seed_registry_ref(pid, registry_ref)
      new_worker_pid = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      counter = :counters.new(1, [])

      Mimic.stub(WorkerRegistry, :swap, fn ^registry_ref ->
        :counters.add(counter, 1, 1)

        if :counters.get(counter, 1) < 3 do
          {:error, :no_broker}
        else
          {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
        end
      end)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      # Two refused swaps back off before the third succeeds.
      assert wait_until(fn -> :sys.get_state(pid).worker_pid == new_worker_pid end, 2_000, 5)

      post = :sys.get_state(pid)
      assert post.worker_pid == new_worker_pid
      assert post.swap_attempt == 0
      assert post.swap_started_at == nil
    end

    test "swap exhaustion empties the queue, marks :swap_exhausted, keeps the producer alive (2.4 exhaustion)" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 1,
          retry_backoff_ms: 1,
          retry_max_backoff_ms: 3,
          delivery_timeout_ms: 50
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "y", %{})
      assert dispatched?(pid)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      seed_registry_ref(pid, registry_ref)

      # Backdate swap_started_at so the first swap-attempt is already past
      # delivery_timeout_ms; the first {:error, _} should exhaust immediately.
      now = System.monotonic_time(:millisecond)

      :sys.replace_state(pid, fn state ->
        %{state | swap_started_at: now - 1_000}
      end)

      Mimic.stub(WorkerRegistry, :swap, fn ^registry_ref -> {:error, :no_broker} end)

      attach(:produce_failed)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      # The exhaustion bookkeeping empties the queue and the window.
      assert wait_until(
               fn ->
                 state = :sys.get_state(pid)
                 state.queue_size == 0 and state.in_flight == []
               end,
               2_000,
               5
             )

      assert Process.alive?(pid),
             "producer must stay alive after exhaustion (no supervisor crash loop)"

      state = :sys.get_state(pid)
      assert state.queue_size == 0
      assert state.in_flight == []
      assert state.swap_attempt == 0
      assert state.swap_started_at == nil
      assert state.last_error == {:swap_exhausted, :no_broker}

      # Every queued record SHOULD have produced a :failed telemetry event.
      events = collect_failed_events(2, 200)
      assert length(events) == 2
    end

    test "swap falls back to acquire/2 when registry returns :no_pending_swap" do
      identity = %{hosts: [{"k", 9092}]}

      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 1,
          extra: [connection_identity: identity]
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      assert dispatched?(pid)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      seed_registry_ref(pid, registry_ref)
      new_worker_pid = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      Mimic.expect(WorkerRegistry, :swap, fn ^registry_ref -> {:error, :no_pending_swap} end)

      Mimic.expect(WorkerRegistry, :acquire, fn ^identity ->
        {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
      end)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      post = :sys.get_state(pid)
      assert post.worker_pid == new_worker_pid
    end
  end

  # ---------- outbox delivery confirmation ----------

  describe "delivery-guaranteed output" do
    setup do
      rc = "kw_" <> Integer.to_string(System.unique_integer([:positive]))
      entity_id = {rc, "proc", "k1"}
      :ok = Flezha.State.TestHelpers.stub_outbox_owner(entity_id)
      %{rc: rc, entity_id: entity_id}
    end

    test "an acked batch confirms its outbox range", %{entity_id: entity_id} do
      {pid, _} = start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      :ok =
        ProducerWorker.cast_enqueue(
          pid,
          "v",
          Flezha.State.TestHelpers.guaranteed_metadata(entity_id, 7)
        )

      assert_receive {:send_called, _request, _ns}, 1_000
      assert :ok = Flezha.State.TestHelpers.assert_confirmed([{:cursor, 7}], 1_000)
    end

    test "a permanently failed batch parks the record instead of dropping it", %{
      rc: rc,
      entity_id: entity_id
    } do
      original = Application.get_env(:flezha, :dlq, [])
      dir = Path.join(System.tmp_dir!(), "dlq_#{System.unique_integer([:positive])}")
      Application.put_env(:flezha, :dlq, Keyword.put(original, :dir, dir))

      on_exit(fn ->
        Application.put_env(:flezha, :dlq, original)
        File.rm_rf(dir)
      end)

      {pid, _} = start_worker(script: [{:return, Fakes.err_response(@topic, @partition, 18)}])

      # The park logs at :error by design. The capture has to span the enqueue as
      # well as the receive: the worker parks asynchronously, so a capture opened
      # only around the receive can miss the line it is meant to assert.
      log =
        capture_log(fn ->
          :ok =
            ProducerWorker.cast_enqueue(
              pid,
              "poison",
              Flezha.State.TestHelpers.guaranteed_metadata(entity_id, 3)
            )

          # Parked, then the hold is answered so the cursor may pass the parked range.
          assert :ok = Flezha.State.TestHelpers.assert_confirmed([{:cursor, 3}], 1_000)
        end)

      assert log =~ "parked undeliverable output"
      assert log =~ "record_list_too_large"

      assert [entry] = Flezha.Dlq.Store.list(rc)
      assert entry["entity_key"] == "k1"
      assert entry["channel"] == "out"
      assert entry["error"] =~ "record_list_too_large"
    end

    test "with no configured overflow policy a guaranteed record waits instead of evicting", %{
      entity_id: entity_id
    } do
      {pid, script_pid} =
        start_worker(queue_max: 1, queue_overflow: nil, linger_ms: 1)

      Agent.update(script_pid, fn _ ->
        [
          {:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}
          | List.duplicate({:return, Fakes.ok_response(@topic, @partition)}, 10)
        ]
      end)

      attach(:dropped)

      for seq <- 0..2 do
        :ok =
          ProducerWorker.cast_enqueue(
            pid,
            "v#{seq}",
            Flezha.State.TestHelpers.guaranteed_metadata(entity_id, seq)
          )
      end

      # Every record is delivered — under `:drop_oldest` the middle one would
      # have been evicted while its cursor claimed it delivered.
      assert :ok =
               Flezha.State.TestHelpers.assert_confirmed(
                 Enum.map(0..2, &{:cursor, &1}),
                 2_000
               )

      refute_received {:telemetry, [:flezha, :plugin, :kafka, :produce, :dropped], _, _}
    end

    test "an explicitly configured drop of a guaranteed record is alarm-grade", %{
      entity_id: entity_id
    } do
      {pid, script_pid} =
        start_worker(queue_max: 1, queue_overflow: :drop_oldest, linger_ms: 1)

      Agent.update(script_pid, fn _ ->
        [
          {:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}
          | List.duplicate({:return, Fakes.ok_response(@topic, @partition)}, 10)
        ]
      end)

      attach(:dropped)

      log =
        capture_log(fn ->
          for seq <- 0..2 do
            :ok =
              ProducerWorker.cast_enqueue(
                pid,
                "v#{seq}",
                Flezha.State.TestHelpers.guaranteed_metadata(entity_id, seq)
              )
          end

          assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :dropped], %{count: 1},
                          %{overflow_strategy: :drop_oldest, guaranteed: true}},
                         1_000
        end)

      assert log =~ "dropped a delivery-guaranteed record"
    end
  end

  describe "in-flight window" do
    # One record per dispatch: the next cast only goes in once the previous
    # batch is on the wire, so the window fills a batch at a time.
    defp fill_window(pid, n) do
      for i <- 1..n do
        :ok = ProducerWorker.cast_enqueue(pid, "m#{i}", %{})
        assert_receive {:dispatched, ref, request}, 1_000
        {ref, base_sequence(request)}
      end
    end

    defp base_sequence(request) do
      [%{data: [%{record_set: record_set} | _]} | _] = request.topic_data
      record_set.base_sequence
    end

    defp producer_id(request) do
      [%{data: [%{record_set: record_set} | _]} | _] = request.topic_data
      record_set.producer_id
    end

    defp start_relay_worker(opts) do
      {pid, _script} =
        start_worker(
          opts ++ [send_fn: Fakes.relay_send_fn(self()), linger_ms: 0, max_in_flight: 5]
        )

      pid
    end

    test "five batches are outstanding at once with consecutive sequences" do
      attach(:produce_ok)
      pid = start_relay_worker([])

      window = fill_window(pid, 5)
      assert Enum.map(window, &elem(&1, 1)) == [0, 1, 2, 3, 4]
      assert window_records(:sys.get_state(pid)) == 5

      for {ref, _seq} <- window do
        Fakes.answer(pid, ref, Fakes.ok_response(@topic, @partition))
      end

      for _ <- 1..5 do
        assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :ok], m, _}, 1_000
        assert m.batch_size == 1
      end

      assert ProducerWorker.stats(pid).in_flight == 0
    end

    test "without idempotence exactly one batch is outstanding" do
      pid = start_relay_worker(idempotent: false, max_in_flight: 1)

      :ok = ProducerWorker.cast_enqueue(pid, "a", %{})
      assert_receive {:dispatched, ref, request}, 1_000
      assert base_sequence(request) == -1

      :ok = ProducerWorker.cast_enqueue(pid, "b", %{})
      _ = :sys.get_state(pid)
      refute_received {:dispatched, _, _}

      Fakes.answer(pid, ref, Fakes.ok_response(@topic, @partition))
      assert_receive {:dispatched, _ref, _request}, 1_000
    end

    test "a head failure resends the whole tail behind it, in order" do
      pid = start_relay_worker([])

      [{ref1, 0}, {ref2, 1}, {ref3, 2}] = fill_window(pid, 3)

      Fakes.answer(pid, ref1, Fakes.err_response(@topic, @partition, 7))
      # k+1 and k+2 are still outstanding, so nothing goes back on the wire yet.
      _ = :sys.get_state(pid)
      refute_received {:dispatched, _, _}

      Fakes.answer(pid, ref2, Fakes.err_response(@topic, @partition, 45))
      Fakes.answer(pid, ref3, Fakes.err_response(@topic, @partition, 45))

      assert_receive {:dispatched, _, resent_1}, 1_000
      assert_receive {:dispatched, _, resent_2}, 1_000
      assert_receive {:dispatched, _, resent_3}, 1_000
      assert Enum.map([resent_1, resent_2, resent_3], &base_sequence/1) == [0, 1, 2]
    end

    test "a tail failure does not resend the head" do
      pid = start_relay_worker([])

      [{ref1, 0}, {ref2, 1}] = fill_window(pid, 2)

      Fakes.answer(pid, ref1, Fakes.ok_response(@topic, @partition))
      Fakes.answer(pid, ref2, Fakes.err_response(@topic, @partition, 7))

      assert_receive {:dispatched, _, resent}, 1_000
      assert base_sequence(resent) == 1
      _ = :sys.get_state(pid)
      refute_received {:dispatched, _, _}
    end

    test "a timed-out head waits for the tail's outcome, then both go out in order" do
      pid = start_relay_worker([])

      [{ref1, 0}, {ref2, 1}] = fill_window(pid, 2)

      send(pid, {:dispatch_timeout, ref1})
      _ = :sys.get_state(pid)
      refute_received {:dispatched, _, _}

      Fakes.answer(pid, ref2, Fakes.err_response(@topic, @partition, 45))

      assert_receive {:dispatched, _, resent_1}, 1_000
      assert_receive {:dispatched, _, resent_2}, 1_000
      assert Enum.map([resent_1, resent_2], &base_sequence/1) == [0, 1]
    end

    test "a fence on the oldest batch renumbers the whole window and resends it in order" do
      ids = :counters.new(1, [])

      init_producer_id_fn = fn _w, _t ->
        :counters.add(ids, 1, 1)
        {:ok, %{producer_id: 1000 + :counters.get(ids, 1), producer_epoch: 0}}
      end

      pid = start_relay_worker(init_producer_id_fn: init_producer_id_fn)

      [{ref1, 0}, {_ref2, 1}, {_ref3, 2}] = fill_window(pid, 3)

      Fakes.answer(pid, ref1, Fakes.err_response(@topic, @partition, 45))

      resent =
        for _ <- 1..3 do
          assert_receive {:dispatched, _, request}, 1_000
          request
        end

      assert Enum.map(resent, &base_sequence/1) == [0, 1, 2]
      assert Enum.map(resent, &producer_id/1) == [1002, 1002, 1002]
    end

    test "worker death rewinds the whole window to the head of the queue, oldest first" do
      attach(:worker_swapped)
      pid = start_relay_worker([])

      _window = fill_window(pid, 3)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      :sys.replace_state(pid, fn state -> %{state | registry_ref: registry_ref} end)
      new_worker_pid = spawn(&FlezhaTest.Helper.NodeTestHelper.park/0)

      Mimic.expect(WorkerRegistry, :swap, fn ^registry_ref ->
        {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
      end)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :worker, :swapped], m, _}, 1_000
      assert m.in_flight_records == 3

      # The rewound records are back at the head of the queue in admission
      # order, and leave again under the new session.
      assert_receive {:dispatched, _, resent}, 1_000
      assert record_values(resent) == ["m1", "m2", "m3"]
      assert :sys.get_state(pid).queue_size == 0
    end

    test "a completed batch confirms each of its records exactly once" do
      rc = "kw_" <> Integer.to_string(System.unique_integer([:positive]))
      entity_id = {rc, "proc", "k1"}
      :ok = Flezha.State.TestHelpers.stub_outbox_owner(entity_id)

      pid = start_relay_worker([])

      for seq <- 1..3 do
        :ok =
          ProducerWorker.cast_enqueue(
            pid,
            "v#{seq}",
            Flezha.State.TestHelpers.guaranteed_metadata(entity_id, seq)
          )

        assert_receive {:dispatched, ref, _request}, 1_000
        Fakes.answer(pid, ref, Fakes.ok_response(@topic, @partition))
        assert :ok = Flezha.State.TestHelpers.assert_confirmed([{:cursor, seq}], 1_000)
      end
    end

    defp record_values(request) do
      [%{data: [%{record_set: record_set} | _]} | _] = request.topic_data
      Enum.map(record_set.records, & &1.value)
    end
  end
end
