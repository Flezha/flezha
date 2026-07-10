defmodule FlezhaTest.Helper.NodeTestHelper do
  use Flezha.Constant
  use ExUnit.Case

  # TODO: remove or refactor.
  # def test_operative(operative_id) do
  #   operative =
  #     FlezhaTest.Fixtures.OperativeFixture.get(
  #       operative_id,
  #       @factory_increment_state
  #     )
  #     |> FlezhaTest.Fixtures.OperativeFixture.start()

  #   on_exit(fn ->
  #     FlezhaTest.Fixtures.OperativeFixture.stop(operative.id)
  #   end)

  #   operative
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

    # Mirror `test_helper.exs`'s `:logger.set_module_level/2` cap on the
    # peer so the noisy `ProcessHub.Service.LoggerService.error/3` lines
    # from cross-node redistribution don't leak into the suite output.
    :erpc.call(node_name, :logger, :set_module_level, [
      ProcessHub.Service.LoggerService,
      :none
    ])

    {peer_pid, node_name}
  end

  def stop_peer_node(peer_pid, peer_node) do
    if Process.alive?(peer_pid) do
      # Gracefully stop the peer node to allow migration.
      :erpc.call(peer_node, :application, :stop, [:flezha])

      :peer.stop(peer_pid)
    end
  end

  def start_executor_remote(node, namespace) do
    self = self()
    :erpc.call(node, Kernel, :spawn, [__MODULE__, :start_executor_recv, [namespace, self]])

    receive do
      {:remote_executor_connected, ^namespace, executor_id} -> executor_id
    end
  end

  def stop_executor_remote(node, namespace, executor_id) do
    self = self()

    :erpc.call(node, Kernel, :spawn, [
      __MODULE__,
      :stop_executor_recv,
      [namespace, executor_id, self]
    ])

    receive do
      {:remote_executor_disconnected, ^namespace, ^executor_id} -> :ok
    end
  end

  def start_executor_recv(namespace, recv_pid) do
    executor_id = start_executor(namespace)

    Process.send(recv_pid, {:remote_executor_connected, namespace, executor_id}, [])

    receive do
      :any -> :ok
    end
  end

  def stop_executor_recv(namespace, executor_id, recv_pid) do
    stop_executor(namespace, executor_id)

    Process.send(
      recv_pid,
      {:remote_executor_disconnected, namespace, executor_id},
      []
    )

    receive do
      :any -> :ok
    end
  end

  def start_executor(namespace \\ "test_temp_namespace", opts \\ []) do
    {_port, executor_id} = start_executor_port(namespace, opts)
    executor_id
  end

  @doc """
  Like `start_executor/2` but returns `{port, executor_id}`. The port is needed
  by crash tests that must reach the executor's OS pid
  (`:erlang.port_info(port, :os_pid)`) to `kill -KILL` it.
  """
  def start_executor_port(namespace \\ "test_temp_namespace", opts \\ []) do
    {:ok, current_path} = :file.get_cwd()
    transport = Keyword.get(opts, :transport, :uds)
    runtime = Keyword.get(opts, :runtime, :rust)

    command = build_command(runtime, current_path, namespace)

    # `cargo run` does a freshness check on every invocation (1–3 s warm,
    # tens of seconds cold) before the executor binary even starts, so 2 s
    # was racy on a cold target dir and tanked QUIC's setup_all.
    connect_timeout = Keyword.get(opts, :connect_timeout, 30_000)

    Flezha.Service.EventManager.add_handler(:executor_connected_event)

    {port, _command} =
      Flezha.Service.PortPilot.open_port(command,
        transport: transport,
        env: Keyword.get(opts, :env, [])
      )

    executor_id =
      receive do
        {:executor_connected_event, {^namespace, executor_id}} -> executor_id
      after
        connect_timeout -> raise "temporary test executor not connected"
      end

    # Remove the handler to duplicate events.
    Flezha.Service.EventManager.rem_handler(:executor_connected_event)

    {port, executor_id}
  end

  # Build the command line used to launch the compute engine for a given runtime.
  # Keep `:rust` as the default so every existing test keeps working unchanged.
  defp build_command(:rust, current_path, namespace) do
    "cargo run --quiet --manifest-path=#{current_path}/test/apps/rust_executor/Cargo.toml --target-dir=./target -- " <>
      namespace
  end

  defp build_command(:java, current_path, namespace) do
    # The test executor fat jar is *committed* under
    # test/apps/java_executor/dist/ — refresh it locally with
    # `scripts/build-dist.sh` whenever the SDK or test-app sources
    # change, then `git add` and commit. CI doesn't build it.
    jar_path =
      Path.join([
        current_path,
        "test/apps/java_executor/dist/java_executor.jar"
      ])

    if not File.exists?(jar_path) do
      raise "Java compute engine jar not found at #{jar_path}. " <>
              "Refresh it with `scripts/build-dist.sh` and commit."
    end

    # Cap slf4j-simple at ERROR — disconnect/reconnect WARN noise from
    # the integration matrix tests is intentional but drowns the
    # ExUnit output. Override per-run with
    # `-Dorg.slf4j.simpleLogger.defaultLogLevel=info` when debugging
    # the Java side.
    "java -Dorg.slf4j.simpleLogger.defaultLogLevel=error " <>
      "-jar #{jar_path} #{namespace}"
  end

  def stop_executor(namespace, executor_id, opts \\ []) do
    # QUIC session teardown runs more cleanup than UDS, so allow a wider
    # default window; callers can tighten it via `timeout: N` when testing
    # UDS-only paths that should disconnect quickly.
    timeout = Keyword.get(opts, :timeout, 5_000)

    Flezha.Service.EventManager.add_handler(:executor_disconnected_event)
    Flezha.Executor.FleetManager.disconnect_executor(namespace, executor_id)

    receive do
      {:executor_disconnected_event, {^namespace, ^executor_id}} -> :ok
    after
      timeout -> raise "temporary test executor not disconnected"
    end

    # Remove the handler to duplicate events.
    Flezha.Service.EventManager.rem_handler(:executor_disconnected_event)

    :ok
  end

  @doc """
  Poll `fun` every 50 ms until it returns a truthy value (returned) or
  `timeout_ms` elapses (returns the last falsy result). The shared home for
  the wait loop every system/e2e test used to carry privately.
  """
  def wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) >= deadline do
          falsy
        else
          Process.sleep(50)
          do_wait_until(fun, deadline)
        end

      truthy ->
        truthy
    end
  end

  @doc "Close a spawned executor/mock port if it is still open. Idempotent."
  def ensure_port_dead(port) do
    if is_port(port) and Enum.member?(:erlang.ports(), port) do
      try do
        Port.close(port)
      catch
        _, _ -> :ok
      end
    end

    :ok
  end
end
