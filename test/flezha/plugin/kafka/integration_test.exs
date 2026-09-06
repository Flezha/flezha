defmodule FlezhaTest.Plugin.Kafka.IntegrationTest do
  @moduledoc """
  End-to-end integration tests against a real Kafka in
  `docker/dev/docker-compose.yml`. Tagged `:kafka_integration` so the
  default `mix test` skips them — run with:

      KAFKA_INTEGRATION=1 mix test --include kafka_integration test/plugin/kafka/integration_test.exs

  Pre-reqs:
    1. `docker compose -f docker/dev/docker-compose.yml up -d kafka`
    2. Kafka is reachable at `kafka:9092` from the test host (the dev
       compose stack maps it to localhost:9092 too; either works as long
       as `/etc/hosts` resolves `kafka` or you set BROKER_HOST).

  Failure mode under chaos: the test pauses Kafka mid-stream via
  `docker compose pause kafka`, holds for ~10 s, unpauses, and asserts
  every produced message survives. The producer SHALL retry transparently
  during the pause and succeed after unpause.
  """

  alias Flezha.Plugin.Provider.Kafka
  alias Flezha.Plugin.Provider.Kafka.ProducerWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ProviderPrepare

  # async: false: a shared Kafka broker in Docker.

  use ExUnit.Case, async: false

  @moduletag :kafka_integration

  @broker_host System.get_env("KAFKA_BROKER_HOST", "kafka")
  @broker_port String.to_integer(System.get_env("KAFKA_BROKER_PORT", "9092"))
  @compose_file System.get_env("KAFKA_COMPOSE_FILE", "docker/dev/docker-compose.yml")
  @compose_service System.get_env("KAFKA_COMPOSE_SERVICE", "kafka")

  # Run only when explicitly included: `mix test --include kafka_integration`
  # AND the env var is set, so accidental local invocation does not destroy
  # someone's running Kafka container.
  setup_all do
    if System.get_env("KAFKA_INTEGRATION") != "1" do
      raise "Refusing to run kafka_integration tests without KAFKA_INTEGRATION=1"
    end

    :ok
  end

  test "100 messages survive a 10-second broker pause" do
    plugin_id = "integ-#{System.unique_integer([:positive])}"
    topic = "flezha_integ_#{System.unique_integer([:positive])}"

    {:ok, _opts, pid} =
      ProviderPrepare.prepare_and_activate(Kafka.Producer, %{
        hosts: [{@broker_host, @broker_port}],
        topic: topic,
        partition: 0,
        plugin_id: plugin_id,
        linger_ms: 5,
        retries: 100,
        retry_backoff_ms: 100,
        retry_max_backoff_ms: 1_000,
        delivery_timeout_ms: 60_000,
        idempotent: true,
        auto_create_topic: true,
        auto_create_partitions: 1,
        auto_create_replication_factor: 1
      })

    # Send 50 messages, pause Kafka, send 50 more during the pause,
    # then unpause and verify all 100 succeed.
    first_half = produce_in_parallel(pid, 1..50)

    {pause_out, 0} =
      System.cmd("docker", ["compose", "-f", @compose_file, "pause", @compose_service])

    IO.puts(pause_out)

    # `docker compose pause` returns once the container is frozen.
    second_half = produce_in_parallel(pid, 51..100)

    # The paused broker answers nothing: the worker has to have met that —
    # a failed attempt on the wire — before the broker comes back.
    assert NodeTestHelper.wait_until(
             fn ->
               stats = ProducerWorker.stats(pid)
               stats.consecutive_failures >= 1 or stats.last_error != nil
             end,
             30_000
           ),
           "the worker never observed the paused broker"

    {unpause_out, 0} =
      System.cmd("docker", ["compose", "-f", @compose_file, "unpause", @compose_service])

    IO.puts(unpause_out)

    results = Task.await_many(first_half ++ second_half, 60_000)

    failures = Enum.reject(results, &(&1 == :ok))

    assert failures == [],
           "expected all 100 messages to deliver, got failures: #{inspect(failures)}"

    stats = ProducerWorker.stats(pid)
    assert stats.dropped_total == 0
    assert stats.queue_depth == 0

    WorkerSup.stop_worker(WorkerSup.family(Kafka.Producer), {plugin_id, topic, 0})
  end

  defp produce_in_parallel(pid, range) do
    for i <- range do
      Task.async(fn ->
        ProducerWorker.enqueue(pid, "msg-#{i}", %{"kafka_msg_key" => "k-#{i}"}, timeout: 65_000)
      end)
    end
  end
end
