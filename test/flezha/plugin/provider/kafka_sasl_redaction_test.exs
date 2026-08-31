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
