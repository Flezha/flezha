defmodule FlezhaTest.Plugin.Kafka.ProducerWorkerTimestampTest do
  @moduledoc """
  Regression: Kafka record `timestamp` must be a wall-clock Unix-epoch
  millisecond, not BEAM monotonic time. Monotonic time is a large negative
  integer that decodes to ~1951 once Kafka stores it as CreateTime.
  """

  alias Flezha.Plugin.Provider.Kafka.ProducerWorker

  use ExUnit.Case, async: false
  use Mimic

  setup :set_mimic_global

  @topic "ts_topic"
  @partition 0

  defmodule Fakes do
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

    def init_producer_id_ok do
      fn _worker, _timeout ->
        {:ok, %{producer_id: 1, producer_epoch: 0}}
      end
    end

    def metadata_refresh_ok, do: fn _worker, _timeout -> :ok end
    def create_topic_ok, do: fn _worker, _topic, _opts, _timeout -> :ok end

    def ok_response(topic, partition) do
      {:ok,
       %Kayrock.Produce.V5.Response{
         responses: [
           %{
             topic: topic,
             partition_responses: [
               %{
                 partition: partition,
                 error_code: 0,
                 base_offset: 0,
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

  defp start_worker do
    test_pid = self()

    {:ok, script_pid} =
      Agent.start_link(fn -> [{:return, Fakes.ok_response(@topic, @partition)}] end)

    {:ok, pid} =
      GenServer.start_link(ProducerWorker,
        worker_pid: spawn(fn -> Process.sleep(:infinity) end),
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
        init_producer_id_fn: Fakes.init_producer_id_ok(),
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
