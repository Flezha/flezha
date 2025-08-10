defmodule FlezhaTest.Helper.NodeTestHelper do
  use Flezha.Constant
  use ExUnit.Case

  # TODO: remove or refactor.
  # def test_computation(computation_id) do
  #   computation =
  #     FlezhaTest.Fixtures.ComputationFixture.get(
  #       computation_id,
  #       @factory_increment_state
  #     )
  #     |> FlezhaTest.Fixtures.ComputationFixture.start()

  #   on_exit(fn ->
  #     FlezhaTest.Fixtures.ComputationFixture.stop(computation.id)
  #   end)

  #   computation
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
    :erpc.call(node_name, :application, :set_env, [:flezha, :env, :test])

    # Set the same flezha application configuration for the peer node.
    Enum.each(Application.get_all_env(:flezha), fn {key, value} ->
      :erpc.call(node_name, Application, :put_env, [:flezha, key, value])
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
    :erpc.call(node_name, Application, :put_env, [:flezha, :testnet_enabled, false])

    # Overwrite the socket files path for the peer node.
    :erpc.call(node_name, Application, :put_env, [
      :flezha,
      :socket_files_path,
      "app_data/sockets/#{node_name}/"
    ])

    # Start the flezha application on the peer node.
    :erpc.call(node_name, Application, :ensure_all_started, [:flezha], 10000)

    {peer_pid, node_name}
  end

  def stop_peer_node(peer_pid, peer_node) do
    if Process.alive?(peer_pid) do
      # Gracefully stop the peer node to allow migration.
      :erpc.call(peer_node, :application, :stop, [:flezha])

      :peer.stop(peer_pid)
    end
  end

  def start_operator_remote(node, namespace) do
    self = self()
    :erpc.call(node, Kernel, :spawn, [__MODULE__, :start_operator_recv, [namespace, self]])

    receive do
      {:remote_operator_connected, ^namespace, operator_id} -> operator_id
    end
  end

  def stop_operator_remote(node, namespace, operator_id) do
    self = self()

    :erpc.call(node, Kernel, :spawn, [
      __MODULE__,
      :stop_operator_recv,
      [namespace, operator_id, self]
    ])

    receive do
      {:remote_operator_disconnected, ^namespace, ^operator_id} -> :ok
    end
  end

  def start_operator_recv(namespace, recv_pid) do
    operator_id = start_operator(namespace)

    Process.send(recv_pid, {:remote_operator_connected, namespace, operator_id}, [])

    receive do
      :any -> :ok
    end
  end

  def stop_operator_recv(namespace, operator_id, recv_pid) do
    stop_operator(namespace, operator_id)

    Process.send(recv_pid, {:remote_operator_disconnected, namespace, operator_id}, [])

    receive do
      :any -> :ok
    end
  end

  def start_operator(namespace \\ "test_temp_namespace") do
    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --manifest-path=#{current_path}/test/apps/rust_operator/Cargo.toml --target-dir=./target -- " <>
        namespace

    Flezha.Service.EventManager.add_handler(:operator_connected_event)
    Flezha.Service.PortPilot.open_port(command)

    operator_id =
      receive do
        {:operator_connected_event, {^namespace, operator_id}} -> operator_id
      after
        2000 -> raise "temporary test operator not connected"
      end

    # Remove the handler to duplicate events.
    Flezha.Service.EventManager.rem_handler(:operator_connected_event)

    operator_id
  end

  def stop_operator(namespace, operator_id) do
    Flezha.Service.EventManager.add_handler(:operator_disconnected_event)
    Flezha.Operator.FleetManager.disconnect_operator(namespace, operator_id)

    receive do
      {:operator_disconnected_event, {^namespace, ^operator_id}} -> :ok
    after
      2000 -> raise "temporary test operator not disconnected"
    end

    # Remove the handler to duplicate events.
    Flezha.Service.EventManager.rem_handler(:operator_disconnected_event)

    :ok
  end
end
