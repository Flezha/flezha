defmodule FlezhaTest.Plugin.Kafka.ProducerWorkerTimestampTest do
  @moduledoc """
  Regression: Kafka record `timestamp` must be a wall-clock Unix-epoch
  millisecond, not BEAM monotonic time. Monotonic time is a large negative
  integer that decodes to ~1951 once Kafka stores it as CreateTime.
  """

  alias Flezha.Plugin.Provider.Kafka.ProducerWorker
  alias FlezhaTest.Kafka.ProducerFakes, as: Fakes

  # async: false: Mimic global mode; Mimic stubs; registered singletons.

  use ExUnit.Case, async: false
  use Mimic

  setup :set_mimic_global

  @topic "ts_topic"
  @partition 0

  defp start_worker do
    test_pid = self()

    {:ok, script_pid} =
      Agent.start_link(fn -> [{:return, Fakes.ok_response(@topic, @partition, 0)}] end)

    {:ok, pid} =
      GenServer.start_link(ProducerWorker,
        worker_pid: spawn(&FlezhaTest.Helper.NodeTestHelper.park/0),
        topic: @topic,
        partition: @partition,
        plugin_id: "ts-test-#{System.unique_integer([:positive])}",
        linger_ms: 5,
        max_batch_bytes: 1_048_576,
        queue_max: 100,
        queue_overflow: :drop_oldest,
        queue_block_timeout_ms: 1_000,
        retries: 0,
        retry_backoff_ms: 5,
        retry_max_backoff_ms: 50,
        delivery_timeout_ms: 1_000,
        idempotent: true,
        auto_create_topic: false,
        metadata_refresh_timeout_ms: 100,
        send_fn: Fakes.script_send_fn(test_pid, script_pid),
        init_producer_id_fn: Fakes.init_producer_id_ok(1, 0),
        metadata_refresh_fn: Fakes.metadata_refresh_ok(),
        create_topic_fn: Fakes.create_topic_ok(),
        jitter_fn: fn _ -> 0 end,
        name: nil
      )

    pid
  end

  test "record timestamp on the wire is a wall-clock Unix-epoch millisecond" do
    pid = start_worker()
    before_ms = System.system_time(:millisecond)

    assert :ok = ProducerWorker.enqueue(pid, "hello", %{})

    assert_receive {:send_called, request, _selector}, 1_000
    [%{data: [%{record_set: rb}]}] = request.topic_data
    [record] = rb.records

    after_ms = System.system_time(:millisecond)

    assert record.timestamp >= before_ms - 60_000,
           "record timestamp #{record.timestamp} is far below current wall clock " <>
             "#{before_ms} — looks like BEAM monotonic time, not Unix-epoch ms"

    assert record.timestamp <= after_ms + 60_000,
           "record timestamp #{record.timestamp} is far above current wall clock " <>
             "#{after_ms} — not a sane Unix-epoch ms"
  end
end
