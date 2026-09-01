defmodule FlezhaTest.Plugin.KafkaTest do
  @moduledoc """
  Facade-level tests for `Flezha.Plugin.Provider.Kafka.Producer`.

  Exercises the full path: `init_provider/1` → ensures a `ProducerWorker`
  is running, `on_input/2` / `on_output/2` → enqueues and gets back
  `{:ok, ctx}` once the broker round-trip completes.

  Per-component coverage (idempotence, retry, batching, drop semantics,
  auto-create, metadata refresh, telemetry, registry) lives in the more
  focused files under `test/plugin/kafka/`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Kafka
  alias Flezha.Plugin.Provider.Kafka.{ProducerSup, WorkerRegistry}
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: Mimic global mode; Mimic stubs; the primary Logger.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  # Some cases start a real ProducerWorker with no broker reachable, so KafkaEx
  # logs an :nxdomain connect error + crash report by design; capture so a green
  # run stays quiet (logs still print on failure).
  @moduletag :capture_log

  @topic "flezha_test_topic"
  @broker {"kafka", 9092}

  setup :set_mimic_global_until_exit

  setup do
    test_pid = self()

    fake_worker = fn ->
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)
    end

    stub(WorkerRegistry, :acquire, fn _identity ->
      pid = fake_worker.()
      send(test_pid, {:registry_acquired, pid})
      {:ok, pid, make_ref()}
    end)

    stub(WorkerRegistry, :release, fn _ref -> :ok end)

    :ok
  end

  defp build_opts(plugin_id, partition, extras \\ %{}) do
    Map.merge(
      %{
        hosts: [@broker],
        topic: @topic,
        partition: partition,
        plugin_id: plugin_id,
        idempotent: false
      },
      extras
    )
  end

  test "init_provider starts a producer worker and returns its pid" do
    plugin_id = "init-test-#{System.unique_integer([:positive])}"

    {:ok, opts, producer_pid} =
      Kafka.Producer.init_provider(build_opts(plugin_id, 0))

    assert is_pid(producer_pid)
    assert Process.alive?(producer_pid)
    assert opts[:producer_pid] == producer_pid
    assert opts[:plugin_id] == plugin_id

    [{registered_pid, _}] =
      Registry.lookup(ProducerSup.registry_name(), {plugin_id, @topic, 0})

    assert registered_pid == producer_pid

    ProducerSup.stop_worker(plugin_id, @topic, 0)
  end

  test "stats/1 returns producer counters keyed on plugin_id" do
    plugin_id = "stats-test-#{System.unique_integer([:positive])}"

    {:ok, _opts, _pid} = Kafka.Producer.init_provider(build_opts(plugin_id, 3))

    assert {:ok, stats} = Kafka.Producer.stats(plugin_id)
    assert stats.queue_depth == 0
    assert stats.topic == @topic
    assert stats.partition == 3

    ProducerSup.stop_worker(plugin_id, @topic, 3)

    assert {:error, :no_producer} = Kafka.Producer.stats(plugin_id)
  end

  test "channel filter passes through when metadata does not match" do
    plugin_id = "group-test-#{System.unique_integer([:positive])}"

    {:ok, opts, _pid} =
      Kafka.Producer.init_provider(build_opts(plugin_id, 0, %{channel: "2"}))

    ctx = %Ctx.IO{
      operative_id: "p",
      namespace: "n",
      message: "trade_payload",
      metadata: %{"channel" => "4"}
    }

    assert {:ok, ^ctx} = Kafka.Producer.on_input(ctx, opts)

    ProducerSup.stop_worker(plugin_id, @topic, 0)
  end

  test "channel filter passes through when metadata is missing" do
    plugin_id = "group-test-#{System.unique_integer([:positive])}"

    {:ok, opts, _pid} =
      Kafka.Producer.init_provider(build_opts(plugin_id, 0, %{channel: "2"}))

    ctx = %Ctx.IO{
      operative_id: "p",
      namespace: "n",
      message: "untagged",
      metadata: %{}
    }

    assert {:ok, ^ctx} = Kafka.Producer.on_input(ctx, opts)

    ProducerSup.stop_worker(plugin_id, @topic, 0)
  end

  test "a window wider than one without idempotence is refused at preparation" do
    plugin_id = "no-idem-#{System.unique_integer([:positive])}"

    assert {:error, {:max_in_flight_requires_idempotent, 3}} =
             Kafka.Producer.init_provider(
               build_opts(plugin_id, 0, %{idempotent: false, max_in_flight: 3})
             )

    assert Registry.lookup(ProducerSup.registry_name(), {plugin_id, @topic, 0}) == []
  end

  describe "build/1 conversion" do
    test "parses integer params from binary CLI input" do
      assert {:ok, %Kafka.Producer{queue_max: 20000, retries: 7, linger_ms: 25}} =
               Kafka.Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "queue_max" => "20000",
                 "retries" => "7",
                 "linger_ms" => "25"
               })
    end

    test "parses max_in_flight" do
      assert {:ok, %Kafka.Producer{max_in_flight: 5}} =
               Kafka.Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "max_in_flight" => "5"
               })
    end

    test "parses boolean params" do
      assert {:ok, %Kafka.Producer{idempotent: false, auto_create_topic: true}} =
               Kafka.Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "idempotent" => "false",
                 "auto_create_topic" => "true"
               })
    end

    test "parses queue_overflow atom" do
      assert {:ok, %Kafka.Producer{queue_overflow: :drop_newest}} =
               Kafka.Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "queue_overflow" => "drop_newest"
               })
    end

    test "queue_overflow accepts drop_nothing and refuses the retired spelling" do
      assert {:ok, %Kafka.Producer{queue_overflow: :drop_nothing}} =
               Kafka.Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "queue_overflow" => "drop_nothing"
               })

      assert {:error, "Invalid value for queue_overflow: block"} =
               Kafka.Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 # overflow-naming-record:begin
                 "queue_overflow" => "block"
                 # overflow-naming-record:end
               })
    end

    test "default required_acks is -1 (acks=all) — BREAKING change from previous default of 1" do
      assert {:ok, %Kafka.Producer{required_acks: -1}} =
               Kafka.Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x"
               })
    end
  end

  describe "host parsing" do
    test "parses comma-separated string hosts" do
      plugin_id = "host-test-#{System.unique_integer([:positive])}"

      {:ok, opts, _pid} =
        Kafka.Producer.init_provider(%{
          hosts: "kafka1:9092,kafka2:9093",
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      assert opts[:hosts] == [{"kafka1", 9092}, {"kafka2", 9093}]
      ProducerSup.stop_worker(plugin_id, @topic, 0)
    end

    test "passes through list-form hosts" do
      plugin_id = "host-test-#{System.unique_integer([:positive])}"

      {:ok, opts, _pid} =
        Kafka.Producer.init_provider(%{
          hosts: [{"k1", 9092}, {"k2", 9093}],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      assert opts[:hosts] == [{"k1", 9092}, {"k2", 9093}]
      ProducerSup.stop_worker(plugin_id, @topic, 0)
    end
  end

  describe "on_input / on_output enqueue path" do
    test "on_input forwards to producer worker and gets back :ok ctx" do
      # Drive the producer via the facade and rely on the worker to ack.
      # The worker_pid the registry hands out is a fake that never replies
      # to send_request — but we never enqueue because we only test the
      # channel passthrough branch in the facade-level tests above.
      # Here we exercise the produce attempt with an in-memory ProducerWorker
      # primed for success via send_fn.
      plugin_id = "io-test-#{System.unique_integer([:positive])}"

      {:ok, opts, producer_pid} =
        Kafka.Producer.init_provider(%{
          hosts: [@broker],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      # Replace the producer worker's send_fn with one that returns OK.
      :sys.replace_state(producer_pid, fn state ->
        send_fn = fn _w, _r, _ns ->
          {:ok,
           %Kayrock.Produce.V5.Response{
             responses: [
               %{
                 topic: @topic,
                 entity_responses: [
                   %{
                     partition: 0,
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

        %{state | send_fn: send_fn}
      end)

      ctx = %Ctx.IO{operative_id: "p", namespace: "n", message: "hello", metadata: %{}}
      assert {:ok, ^ctx} = Kafka.Producer.on_input(ctx, opts)
      assert {:ok, ^ctx} = Kafka.Producer.on_output(ctx, opts)

      ProducerSup.stop_worker(plugin_id, @topic, 0)
    end

    test "on_output returns {:ok, ctx} immediately even when broker would block 30 s" do
      plugin_id = "nonblock-test-#{System.unique_integer([:positive])}"

      {:ok, opts, producer_pid} =
        Kafka.Producer.init_provider(%{
          hosts: [@broker],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      # Send never answers — a broker round-trip that outlives the test — so a
      # chain that waited on it would block until the GenServer.call timeout.
      # With cast_enqueue the chain returns immediately.
      :sys.replace_state(producer_pid, fn state ->
        %{state | send_fn: fn _w, _r, _ns -> NodeTestHelper.park() end}
      end)

      ctx = %Ctx.IO{operative_id: "p", namespace: "n", message: "x", metadata: %{}}

      {elapsed_us, result} =
        :timer.tc(fn -> Kafka.Producer.on_output(ctx, opts) end)

      assert {:ok, ^ctx} = result
      assert elapsed_us < 1_000, "expected <1ms, got #{elapsed_us}us"

      ProducerSup.stop_worker(plugin_id, @topic, 0)
    end

    test "on_output surfaces permanent delivery failure via :failed telemetry, not return value" do
      plugin_id = "telemetry-test-#{System.unique_integer([:positive])}"

      {:ok, opts, producer_pid} =
        Kafka.Producer.init_provider(%{
          hosts: [@broker],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false,
          retries: 1,
          retry_backoff_ms: 1,
          retry_max_backoff_ms: 5,
          delivery_timeout_ms: 30,
          metadata_refresh_timeout_ms: 50
        })

      :sys.replace_state(producer_pid, fn state ->
        %{
          state
          | send_fn: fn _w, _r, _ns -> {:error, :network_exception} end,
            jitter_fn: fn _ -> 0 end
        }
      end)

      handler_id = "kafka-test-failed-#{System.unique_integer([:positive])}"
      test_pid = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:flezha, :plugin, :kafka, :produce, :failed],
          fn name, measurements, metadata, _ ->
            send(test_pid, {:telemetry, name, measurements, metadata})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      ctx = %Ctx.IO{operative_id: "p", namespace: "n", message: "x", metadata: %{}}
      assert {:ok, ^ctx} = Kafka.Producer.on_output(ctx, opts)

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :produce, :failed], _,
                      %{last_reason: :network_exception, caller: :async}},
                     2_000

      ProducerSup.stop_worker(plugin_id, @topic, 0)
    end
  end

  describe "registry release coupling" do
    test "stopping the producer worker releases the WorkerRegistry handle" do
      released = self()

      stub(WorkerRegistry, :acquire, fn _identity ->
        pid =
          spawn(fn ->
            receive do
              :stop -> :ok
            end
          end)

        {:ok, pid, {:fake_ref, System.unique_integer([:positive])}}
      end)

      stub(WorkerRegistry, :release, fn ref ->
        send(released, {:released, ref})
        :ok
      end)

      plugin_id = "release-test-#{System.unique_integer([:positive])}"

      {:ok, _opts, _pid} =
        Kafka.Producer.init_provider(%{
          hosts: [@broker],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      ProducerSup.stop_worker(plugin_id, @topic, 0)

      assert_receive {:released, {:fake_ref, _}}, 1_000
    end
  end
end

defmodule Flezha.Plugin.Provider.KafkaSaslRedactionTest do
  @moduledoc """
  finish-platform-authn-authz §7: Kafka SASL credentials ride a closure into
  the client start (mirroring the Postgres provider) — the live client
  authenticates with the real secret while the returned/persisted opts carry
  only a redacted placeholder.
  """
  # async: false: Mimic global mode; Mimic stubs; OS environment variables.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Plugin.Provider.Kafka.Producer
  alias Flezha.Plugin.Provider.Kafka.ProducerSup
  alias FlezhaTest.Helper.NodeTestHelper

  setup :set_mimic_global_until_exit

  @secret "sup3r-s3cret-sasl-pw"

  defp fake_client_stubs(test_pid) do
    stub(KafkaEx, :start_link_worker, fn :no_name, worker_opts ->
      client = spawn(&NodeTestHelper.park/0)
      send(test_pid, {:client_started, client, worker_opts})
      {:ok, client}
    end)

    stub(KafkaEx, :stop_worker, fn pid ->
      Process.exit(pid, :kill)
      :ok
    end)
  end

  test "the SASL secret reaches the client but not the returned opts" do
    fake_client_stubs(self())
    suffix = System.unique_integer([:positive])

    opts = %{
      hosts: "sasl-test-host-#{suffix}:9092",
      topic: "sasl_topic_#{suffix}",
      plugin_id: "sasl-test-#{suffix}",
      auth: %{"mechanism" => "plain", "username" => "svc", "password" => @secret}
    }

    {:ok, returned, producer_pid} = Producer.init_provider(opts)

    on_exit(fn ->
      _ = ProducerSup.stop_worker("sasl-test-#{suffix}", "sasl_topic_#{suffix}", 0)
      _ = producer_pid
    end)

    # The live client authenticated with the real secret...
    assert_receive {:client_started, _client, worker_opts}, 2_000
    assert %KafkaEx.Auth.Config{username: "svc", password: @secret} = worker_opts[:auth]

    # ...while the returned (and therefore persisted) opts are redacted.
    assert returned.auth["password"] == "**redacted**"
    refute inspect(returned, limit: :infinity) =~ @secret
  end

  test "sasl_password_env resolves the secret from the environment at client start" do
    fake_client_stubs(self())
    suffix = System.unique_integer([:positive])
    env_var = "FLZ_TEST_SASL_PW_#{suffix}"
    System.put_env(env_var, @secret)
    on_exit(fn -> System.delete_env(env_var) end)

    opts = %{
      hosts: "sasl-env-host-#{suffix}:9092",
      topic: "sasl_topic_#{suffix}",
      plugin_id: "sasl-env-#{suffix}",
      auth: %{"mechanism" => "plain", "username" => "svc", "password" => "placeholder"},
      sasl_password_env: env_var
    }

    {:ok, returned, _pid} = Producer.init_provider(opts)

    on_exit(fn ->
      _ = ProducerSup.stop_worker("sasl-env-#{suffix}", "sasl_topic_#{suffix}", 0)
    end)

    assert_receive {:client_started, _client, worker_opts}, 2_000
    assert %KafkaEx.Auth.Config{password: @secret} = worker_opts[:auth]

    refute inspect(returned, limit: :infinity) =~ @secret
  end
end
