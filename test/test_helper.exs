ExUnit.start()

Mimic.copy(KafkaEx)

defmodule TestHelper do
  use Aflecha.Constant
  use ExUnit.Case

  def test_pipeline(pipeline_id) do
    pipeline =
      AflechaTest.Fixtures.PipelineFixture.get(
        pipeline_id,
        @factory_increment_state
      )
      |> AflechaTest.Fixtures.PipelineFixture.start()

    on_exit(fn ->
      AflechaTest.Fixtures.PipelineFixture.stop(pipeline.id)
    end)

    pipeline
  end

  def node_name() do
    :"test_agent_node1@127.0.0.1"
  end

  def start_peer_node(name) do
    args = ~c"-loader inet -hosts 127.0.0.1 -setcookie \"#{:erlang.get_cookie()}\""

    # Start the peer node.
    {:ok, pid, node_name} =
      :peer.start_link(%{
        host: ~c"127.0.0.1",
        name: name,
        args: [args]
      })

    # Add current nodes code path to the peer node.
    :erpc.call(node_name, :code, :add_paths, [:code.get_path()])

    # Adjust the environment for the peer node.
    :ok = :erpc.call(node_name, :application, :set_env, [:mix, :env, :test])
    :erpc.call(node_name, :application, :set_env, [:aflecha, :env, :test])

    # Set the same aflecha application configuration for the peer node.
    Enum.each(Application.get_all_env(:aflecha), fn {key, value} ->
      :erpc.call(node_name, Application, :put_env, [:aflecha, key, value])
    end)

    # Set the same kafka_ex application configuration for the peer node.
    Enum.each(Application.get_all_env(:kafka_ex), fn {key, value} ->
      :erpc.call(node_name, Application, :put_env, [:kafka_ex, key, value])
    end)

    # Set the same logger application configuration for the peer node.
    Enum.each(Application.get_all_env(:logger), fn {key, value} ->
      :erpc.call(node_name, Application, :put_env, [:logger, key, value])
    end)

    # Disable the testnet for the peer node.
    :erpc.call(node_name, Application, :put_env, [:aflecha, :testnet_enabled, false])

    # Overwrite the socket files path for the peer node.
    :erpc.call(node_name, Application, :put_env, [
      :aflecha,
      :socket_files_path,
      "app_data/sockets/#{node_name}/"
    ])

    # Start the aflecha application on the peer node.
    :erpc.call(node_name, Application, :ensure_all_started, [:aflecha], 10000)

    {peer_pid, node_name}
  end

  def stop_peer_node(peer_pid) do
    if Process.alive?(peer_pid) do
      :peer.stop(peer_pid)
    end
  end
end
