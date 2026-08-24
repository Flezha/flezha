defmodule FlezhaTest.Helper.NodeTestHelper do
  use Flezha.Constant
  use ExUnit.Case

  alias Flezha.Service.PortPilot

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

  # A peer's `Application.start/2` blocks in `validate_boot_ready/0`, which
  # itself waits up to 30 s for `BootLoader`. The budget has to clear that
  # inner ceiling so a busy machine reports the peer's real start result
  # rather than a spurious `{:erpc, :timeout}`, and stay under ExUnit's 60 s
  # per-test timeout so a genuinely wedged boot still names the peer instead
  # of consuming the whole test.
  @peer_boot_timeout_ms 45_000

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

    # The peer copies the host's whole `:flezha` env, but it must not re-run
    # the host's boot hooks. `:post_hooks` blocks `Application.start/2` until
    # both `:external_processes` runners have connected, which is what makes a
    # busy machine miss the boot budget; the runners themselves stay, because
    # the peer only joins the `default` hub cluster once one connects, and
    # callers already wait for that. `:pre_hooks` would additionally wipe
    # `:process_hub_registry_root`, a path the peer shares with the live host.
    for key <- [:pre_hooks, :post_hooks] do
      :erpc.call(node_name, Application, :put_env, [:flezha, key, []])
    end

    # A test that fails mid-way must not leak the peer BEAM; `stop_peer_node`
    # already checks liveness, so an explicit stop makes this a no-op.
    on_exit(fn ->
      if Process.alive?(peer_pid), do: catch_exit(:peer.stop(peer_pid))
    end)

    # Start the flezha application on the peer node.
    :erpc.call(node_name, Application, :ensure_all_started, [:flezha], @peer_boot_timeout_ms)

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

  def start_runner_remote(node, namespace) do
    self = self()
    :erpc.call(node, Kernel, :spawn, [__MODULE__, :start_runner_recv, [namespace, self]])

    receive do
      {:remote_runner_connected, ^namespace, runner_id} -> runner_id
    end
  end

  def stop_runner_remote(node, namespace, runner_id) do
    self = self()

    :erpc.call(node, Kernel, :spawn, [
      __MODULE__,
      :stop_runner_recv,
      [namespace, runner_id, self]
    ])

    receive do
      {:remote_runner_disconnected, ^namespace, ^runner_id} -> :ok
    end
  end

  def start_runner_recv(namespace, recv_pid) do
    runner_id = start_runner(namespace)

    Process.send(recv_pid, {:remote_runner_connected, namespace, runner_id}, [])

    receive do
      :any -> :ok
    end
  end

  def stop_runner_recv(namespace, runner_id, recv_pid) do
    stop_runner(namespace, runner_id)

    Process.send(
      recv_pid,
      {:remote_runner_disconnected, namespace, runner_id},
      []
    )

    receive do
      :any -> :ok
    end
  end

  def start_runner(namespace \\ "test_temp_namespace", opts \\ []) do
    {_port, runner_id} = start_runner_port(namespace, opts)
    runner_id
  end

  @doc """
  Like `start_runner/2` but returns `{port, runner_id}`. The port is needed
  by crash tests that must reach the runner's OS pid
  (`:erlang.port_info(port, :os_pid)`) to `kill -KILL` it.
  """
  def start_runner_port(namespace \\ "test_temp_namespace", opts \\ []) do
    transport = Keyword.get(opts, :transport, :uds)
    runtime = Keyword.get(opts, :runtime, spawn_runtime())
    command = PortPilot.launch_command(runtime, :test, namespace)
    connect_timeout = Keyword.get(opts, :connect_timeout, manifest(runtime)["connect_timeout_ms"])

    Flezha.Service.EventManager.add_handler(:runner_connected_event)

    {port, _command} =
      PortPilot.open_port(command,
        transport: transport,
        env: Keyword.get(opts, :env, []),
        port_opts: Keyword.get(opts, :port_opts, [])
      )

    runner_id =
      receive do
        {:runner_connected_event, {^namespace, runner_id}} -> runner_id
      after
        connect_timeout -> raise "temporary test runner not connected"
      end

    # Remove the handler to duplicate events.
    Flezha.Service.EventManager.rem_handler(:runner_connected_event)

    {port, runner_id}
  end

  # ---------- SDK conformance: the runtime axis ----------
  #
  # The capability vocabulary: what a `test/system` module declares through
  # `FlezhaTest.SystemCase` and what a manifest carries or defers. One entry per
  # row of the runner matrix in `docs/architecture/runner.md`.
  @capabilities [
    :runner_lifecycle,
    :graph_builder,
    :liveness,
    :pool_wedge,
    :retry_ladder,
    :state_persistence,
    :effectively_once,
    :entity_timers,
    :correction_window,
    :keyed_channels,
    :workflow,
    :dynamic_entity,
    :entry_idle,
    :partitioned,
    :schema_evolution
  ]

  # The surface every SDK carries; a manifest cannot defer these.
  #
  # `pool_wedge` is required because a host that cannot be wedged on demand
  # cannot prove it recovers: its fixtures are what exercise the abandon path,
  # the worker replacement and the classifier's verdict on that SDK rather than
  # on the reference alone.
  @required_capabilities [
    :runner_lifecycle,
    :graph_builder,
    :liveness,
    :retry_ladder,
    :partitioned,
    :pool_wedge
  ]

  def capabilities, do: @capabilities
  def required_capabilities, do: @required_capabilities

  # The conformance suite's runtime reaches `start_runner_port/2` through the
  # test process, not the environment: `FLEZHA_E2E_RUNTIME` selects which SDK
  # `test/system` runs against, and `FlezhaTest.SystemCase` publishes it per
  # test. Everything else — controller and integration tests that merely need
  # *a* runner, written against the reference fixtures — keeps the reference,
  # so pointing a run at a partial SDK cannot turn them red for the wrong
  # reason.
  @runtime_key {__MODULE__, :spawn_runtime}

  # The SDK whose manifest every other is checked against, and the one a
  # scenario falls back to when it is not the runner under test.
  @reference_runtime :rust

  @doc "The reference SDK — the manifest every other manifest is measured by."
  def reference_runtime, do: @reference_runtime

  @doc "Scope `runtime` to the calling test process (`FlezhaTest.SystemCase`)."
  def put_spawn_runtime(runtime), do: Process.put(@runtime_key, runtime)

  @doc "The runtime `start_runner_port/2` spawns when the caller names none."
  def spawn_runtime, do: Process.get(@runtime_key, @reference_runtime)

  @doc """
  The runtime this run exercises: `FLEZHA_E2E_RUNTIME` (default `rust`). An
  unknown value raises naming the manifests that exist.
  """
  def runtime do
    case System.get_env("FLEZHA_E2E_RUNTIME", Atom.to_string(@reference_runtime)) do
      "" ->
        @reference_runtime

      name ->
        Enum.find(PortPilot.runtimes(), &(Atom.to_string(&1) == name)) ||
          raise ArgumentError,
                "FLEZHA_E2E_RUNTIME=#{name} names no SDK test app; the manifests under " <>
                  "test/apps are: " <>
                  Enum.map_join(PortPilot.runtimes(), ", ", &PortPilot.manifest_path/1)
    end
  end

  def manifest(runtime \\ runtime()), do: PortPilot.manifest(runtime)
  def manifest_path(runtime \\ runtime()), do: PortPilot.manifest_path(runtime)

  @doc "The capabilities `runtime`'s manifest defers, as vocabulary atoms."
  def deferred(runtime \\ runtime()), do: capability_atoms(runtime, manifest(runtime)["deferred"])

  @doc "The transports `runtime`'s standalone runner supports."
  def transports(runtime \\ runtime()) do
    Enum.map(manifest(runtime)["transports"], &String.to_existing_atom/1)
  end

  @doc "`runtime`'s fixture ids by capability (string keys, as declared)."
  def fixtures(runtime \\ runtime()), do: manifest(runtime)["fixtures"]

  defp capability_atoms(runtime, names) do
    Enum.map(names, fn name ->
      Enum.find(@capabilities, &(Atom.to_string(&1) == name)) ||
        raise ArgumentError,
              "#{manifest_path(runtime)}: #{inspect(name)} is not a capability; the " <>
                "vocabulary is #{inspect(@capabilities)}"
    end)
  end

  def stop_runner(namespace, runner_id, opts \\ []) do
    # QUIC session teardown runs more cleanup than UDS, so allow a wider
    # default window; callers can tighten it via `timeout: N` when testing
    # UDS-only paths that should disconnect quickly.
    timeout = Keyword.get(opts, :timeout, 5_000)

    Flezha.Service.EventManager.add_handler(:runner_disconnected_event)
    Flezha.Runner.FleetManager.disconnect_runner(namespace, runner_id)

    receive do
      {:runner_disconnected_event, {^namespace, ^runner_id}} -> :ok
    after
      timeout -> raise "temporary test runner not disconnected"
    end

    # Remove the handler to duplicate events.
    Flezha.Service.EventManager.rem_handler(:runner_disconnected_event)

    :ok
  end

  @doc """
  Set the primary `Logger` level for one test, returning a zero-arity restore
  closure for the caller's `on_exit` (same caller-owned-cleanup convention as
  `Flezha.State.TestHelpers.override_env/2`). Needed whenever a test asserts
  on log output below the test config's `:error` gate.
  """
  @spec override_log_level(Logger.level()) :: (-> :ok)
  def override_log_level(level) do
    previous = Logger.level()
    Logger.configure(level: level)
    fn -> Logger.configure(level: previous) end
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

  @doc """
  Stop `pid` if it is still alive, tolerating the race that makes
  `FlezhaTest.Helper.NodeTestHelper.ensure_stopped(pid)` flaky: the process can exit
  — or its supervisor can take it down — between the check and the stop, and
  `GenServer.stop/3` then exits the *caller* with that reason. A teardown has
  nothing to assert, so it swallows the exit. Idempotent.
  """
  def ensure_stopped(pid, reason \\ :normal, timeout \\ :infinity) do
    if is_pid(pid) and Process.alive?(pid) do
      try do
        GenServer.stop(pid, reason, timeout)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc "Close a spawned runner/mock port if it is still open. Idempotent."
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
