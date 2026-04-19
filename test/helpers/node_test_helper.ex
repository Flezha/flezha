defmodule FlezhaTest.Helper.NodeTestHelper do
  use Flezha.Constant
  use ExUnit.Case

  # TODO: remove or refactor.
  # def test_processor(processor_id) do
  #   processor =
  #     FlezhaTest.Fixtures.ProcessorFixture.get(
  #       processor_id,
  #       @factory_increment_state
  #     )
  #     |> FlezhaTest.Fixtures.ProcessorFixture.start()

  #   on_exit(fn ->
  #     FlezhaTest.Fixtures.ProcessorFixture.stop(processor.id)
  #   end)

  #   processor
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

    # Peer nodes only need UDS transport (avoids QUIC port conflicts).
    :erpc.call(node_name, Application, :put_env, [:flezha, :transports, [:uds]])

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

  def start_compute_engine_remote(node, namespace) do
    self = self()
    :erpc.call(node, Kernel, :spawn, [__MODULE__, :start_compute_engine_recv, [namespace, self]])

    receive do
      {:remote_compute_engine_connected, ^namespace, compute_engine_id} -> compute_engine_id
    end
  end

  def stop_compute_engine_remote(node, namespace, compute_engine_id) do
    self = self()

    :erpc.call(node, Kernel, :spawn, [
      __MODULE__,
      :stop_compute_engine_recv,
      [namespace, compute_engine_id, self]
    ])

    receive do
      {:remote_compute_engine_disconnected, ^namespace, ^compute_engine_id} -> :ok
    end
  end

  def start_compute_engine_recv(namespace, recv_pid) do
    compute_engine_id = start_compute_engine(namespace)

    Process.send(recv_pid, {:remote_compute_engine_connected, namespace, compute_engine_id}, [])

    receive do
      :any -> :ok
    end
  end

  def stop_compute_engine_recv(namespace, compute_engine_id, recv_pid) do
    stop_compute_engine(namespace, compute_engine_id)

    Process.send(
      recv_pid,
      {:remote_compute_engine_disconnected, namespace, compute_engine_id},
      []
    )

    receive do
      :any -> :ok
    end
  end

  def start_compute_engine(namespace \\ "test_temp_namespace", opts \\ []) do
    {:ok, current_path} = :file.get_cwd()
    transport = Keyword.get(opts, :transport, :uds)
    runtime = Keyword.get(opts, :runtime, :rust)

    command = build_command(runtime, current_path, namespace)

    Flezha.Service.EventManager.add_handler(:compute_engine_connected_event)
    Flezha.Service.PortPilot.open_port(command, transport: transport)

    compute_engine_id =
      receive do
        {:compute_engine_connected_event, {^namespace, compute_engine_id}} -> compute_engine_id
      after
        2000 -> raise "temporary test compute_engine not connected"
      end

    # Remove the handler to duplicate events.
    Flezha.Service.EventManager.rem_handler(:compute_engine_connected_event)

    compute_engine_id
  end

  # Build the command line used to launch the compute engine for a given runtime.
  # Keep `:rust` as the default so every existing test keeps working unchanged.
  defp build_command(:rust, current_path, namespace) do
    "cargo run --manifest-path=#{current_path}/test/apps/rust_compute_engine/Cargo.toml --target-dir=./target -- " <>
      namespace
  end

  defp build_command(:java, current_path, namespace) do
    jar_path =
      Path.join([
        current_path,
        "test/apps/java_compute_engine/target/java_compute_engine.jar"
      ])

    if not File.exists?(jar_path) do
      raise "Java compute engine jar not found at #{jar_path}. " <>
              "Build it first with `mvn -f test/apps/java_compute_engine/pom.xml package` " <>
              "or run the full pipeline via `mix test.java_sdk`."
    end

    "java -jar #{jar_path} #{namespace}"
  end

  def stop_compute_engine(namespace, compute_engine_id) do
    Flezha.Service.EventManager.add_handler(:compute_engine_disconnected_event)
    Flezha.ComputeEngine.FleetManager.disconnect_compute_engine(namespace, compute_engine_id)

    receive do
      {:compute_engine_disconnected_event, {^namespace, ^compute_engine_id}} -> :ok
    after
      2000 -> raise "temporary test compute_engine not disconnected"
    end

    # Remove the handler to duplicate events.
    Flezha.Service.EventManager.rem_handler(:compute_engine_disconnected_event)

    :ok
  end
end
