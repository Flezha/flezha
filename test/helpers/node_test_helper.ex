defmodule AflechaTest.Helper.NodeTestHelper do
  use Aflecha.Constant
  use ExUnit.Case

  # TODO: remove or refactor.
  # def test_pipeline(pipeline_id) do
  #   pipeline =
  #     AflechaTest.Fixtures.PipelineFixture.get(
  #       pipeline_id,
  #       @factory_increment_state
  #     )
  #     |> AflechaTest.Fixtures.PipelineFixture.start()

  #   on_exit(fn ->
  #     AflechaTest.Fixtures.PipelineFixture.stop(pipeline.id)
  #   end)

  #   pipeline
  # end

  def node_name() do
    :"test_agent_node1@127.0.0.1"
  end

  def start_peer_node(name) do
    args = ~c"-loader inet -hosts 127.0.0.1 -setcookie \"#{:erlang.get_cookie()}\""

    # Start the peer node.
    {:ok, peer_pid, node_name} =
      :peer.start_link(%{
        host: ~c"127.0.0.1",
        name: name,
        args: [args]
      })

    # Add current nodes code path to the peer node.
    :erpc.call(node_name, :code, :add_paths, [:code.get_path()])

    # Adjust the environment for the peer node.
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

  def stop_peer_node(peer_pid, peer_node) do
    if Process.alive?(peer_pid) do
      # Gracefully stop the peer node to allow migration.
      :erpc.call(peer_node, :application, :stop, [:aflecha])

      :peer.stop(peer_pid)
    end
  end

  def start_operator_remote(node, group) do
    self = self()
    :erpc.call(node, Kernel, :spawn, [__MODULE__, :start_operator_recv, [group, self]])

    receive do
      {:remote_operator_connected, ^group, operator_id} -> operator_id
    end
  end

  def stop_operator_remote(node, group, operator_id) do
    self = self()

    :erpc.call(node, Kernel, :spawn, [__MODULE__, :stop_operator_recv, [group, operator_id, self]])

    receive do
      {:remote_operator_disconnected, ^group, ^operator_id} -> :ok
    end
  end

  def start_operator_recv(group, recv_pid) do
    operator_id = start_operator(group)

    Process.send(recv_pid, {:remote_operator_connected, group, operator_id}, [])

    receive do
      :any -> :ok
    end
  end

  def stop_operator_recv(group, operator_id, recv_pid) do
    stop_operator(group, operator_id)

    Process.send(recv_pid, {:remote_operator_disconnected, group, operator_id}, [])

    receive do
      :any -> :ok
    end
  end

  def start_operator(group_id \\ "test_temp_group") do
    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --manifest-path=#{current_path}/test/apps/rust_operator/Cargo.toml --target-dir=./target -- " <>
        group_id

    Aflecha.Service.EventManager.add_handler(:operator_connected_event)
    Aflecha.Service.PortPilot.open_port(command)

    operator_id =
      receive do
        {:operator_connected_event, {^group_id, operator_id}} -> operator_id
      after
        2000 -> raise "temporary test operator not connected"
      end

    # Remove the handler to duplicate events.
    Aflecha.Service.EventManager.rem_handler(:operator_connected_event)

    operator_id
  end

  def stop_operator(group_id, operator_id) do
    Aflecha.Service.EventManager.add_handler(:operator_disconnected_event)
    Aflecha.Operator.FleetManager.disconnect_operator(group_id, operator_id)

    receive do
      {:operator_disconnected_event, {^group_id, ^operator_id}} -> :ok
    after
      2000 -> raise "temporary test operator not disconnected"
    end

    # Remove the handler to duplicate events.
    Aflecha.Service.EventManager.rem_handler(:operator_disconnected_event)

    :ok
  end
end
