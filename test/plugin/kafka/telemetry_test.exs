defmodule FlezhaTest.Plugin.Kafka.TelemetryTest do
  alias Flezha.Plugin.Provider.Kafka.Telemetry

  use ExUnit.Case, async: true

  defp listen(events) do
    handler = "test-#{System.unique_integer([:positive])}"
    me = self()

    :ok =
      :telemetry.attach_many(
        handler,
        events,
        fn name, m, md, _ -> send(me, {:tel, name, m, md}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  test "produce_ok emits with batch_size + latency_ms" do
    listen([[:flezha, :plugin, :kafka, :produce, :ok]])
    Telemetry.produce_ok(7, 12, %{topic: "t", partition: 0, plugin_id: "p"})

    assert_receive {:tel, [:flezha, :plugin, :kafka, :produce, :ok],
                    %{batch_size: 7, latency_ms: 12}, %{topic: "t", partition: 0}}
  end

  test "retry emits attempt + backoff_ms with reason metadata" do
    listen([[:flezha, :plugin, :kafka, :produce, :retry]])

    Telemetry.retry(3, 200, %{
      topic: "t",
      partition: 0,
      plugin_id: "p",
      reason: :not_leader_for_partition
    })

    assert_receive {:tel, _, %{attempt: 3, backoff_ms: 200}, %{reason: :not_leader_for_partition}}
  end

  test "dropped emits count + queue/strategy metadata" do
    listen([[:flezha, :plugin, :kafka, :produce, :dropped]])

    Telemetry.dropped(1, %{
      topic: "t",
      partition: 0,
      plugin_id: "p",
      queue_depth: 100,
      overflow_strategy: :drop_oldest
    })

    assert_receive {:tel, _, %{count: 1}, %{overflow_strategy: :drop_oldest}}
  end

  test "failed emits attempts + total_ms + last_reason" do
    listen([[:flezha, :plugin, :kafka, :produce, :failed]])

    Telemetry.failed(5, 30_000, %{
      topic: "t",
      partition: 0,
      plugin_id: "p",
      last_reason: :no_broker
    })

    assert_receive {:tel, _, %{attempts: 5, total_ms: 30_000}, %{last_reason: :no_broker}}
  end

  test "metadata_refresh emits latency + trigger metadata" do
    listen([[:flezha, :plugin, :kafka, :metadata, :refresh]])

    Telemetry.metadata_refresh(42, %{
      trigger: :error_recovery,
      error: :not_leader_for_partition,
      plugin_id: "p",
      topic: "t",
      partition: 0
    })

    assert_receive {:tel, _, %{latency_ms: 42},
                    %{trigger: :error_recovery, error: :not_leader_for_partition}}
  end

  test "topic_created accepts keyword metadata and converts to map" do
    listen([[:flezha, :plugin, :kafka, :topic, :created]])

    Telemetry.topic_created(2, 3, plugin_id: "p", topic: "t")

    assert_receive {:tel, _, %{partitions: 2, replication_factor: 3},
                    %{plugin_id: "p", topic: "t"}}
  end
end
