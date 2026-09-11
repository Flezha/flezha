defmodule FlezhaTest.Helper.NodeTestHelper do
  @moduledoc """
  The suite's one polling primitive (`wait_until/3`) and the machinery a test
  needs around a live fleet: spawning runner ports and peer BEAMs, resolving
  the SDK runtime under test, and tearing both down.
  """

  use Flezha.Constant

  alias Flezha.Config
  alias Flezha.Config.Environment
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.StreamChannel
  alias Flezha.Service.EventManager
  alias Flezha.Service.PortPilot

  @doc """
  The name this run's node is distributed under: `<base>@<host>` from the two
  keys the release names its node with (`rel/env.sh.eex`), the base derived
  per run in `:test` (`config/runtime.exs`).
  """
  def node_name() do
    base = Application.fetch_env!(:flezha, :node_base_name)
    host = Application.fetch_env!(:flezha, :node_host_name)
    :"#{base}@#{host}"
  end

  # A peer's `Application.start/2` blocks in `Boot.Loader.await_sequence/0`,
  # which itself waits up to 30 s. The budget has to clear that
  # inner ceiling so a busy machine reports the peer's real start result
  # rather than a spurious `{:erpc, :timeout}`, and stay under ExUnit's 60 s
  # per-test timeout so a genuinely wedged boot still names the peer instead
  # of consuming the whole test.
  @peer_boot_timeout_ms 45_000

  # The `:flezha` env keys naming a resource one controller instance owns
  # exclusively. `start_peer_node/2` gives a peer its own of each, and
  # `owned_resources/0` is what a co-location scenario asserts is disjoint —
  # so a resource added here is covered without touching the scenario.
  @owned_resources [:socket_files_path, :state_root, :process_hub_registry_root]

  @doc "The `:flezha` env keys naming a resource one controller owns alone."
  @spec owned_resources() :: [atom()]
  def owned_resources(), do: @owned_resources

  @doc """
  A peer BEAM carrying this node's code paths and nothing else started on it —
  the cheapest second OS process on this host, for a scenario about what two
  processes may hold at once rather than about two controllers.
  `start_peer_node/2` is this plus a controller. `index` ends the node name;
  minted once per peer, it is what `start_peer_node/2` scopes resources by.
  """
  def start_bare_peer(name, index \\ System.unique_integer([:positive])) do
    args = ~c"-loader inet -hosts 127.0.0.1 -setcookie \"#{:erlang.get_cookie()}\""

    {:ok, peer_pid, node_name} =
      :peer.start_link(%{
        host: ~c"127.0.0.1",
        name: peer_name(name, index),
        args: [args]
      })

    :erpc.call(node_name, :code, :add_paths, [:code.get_path()])

    # A test that fails mid-way must not leak the peer BEAM; `stop_peer_node`
    # already checks liveness, so an explicit stop makes this a no-op.
    ExUnit.Callbacks.on_exit(fn -> stop_bare_peer(peer_pid) end)

    {peer_pid, node_name}
  end

  defp stop_bare_peer(peer_pid) do
    if Process.alive?(peer_pid), do: tolerate(fn -> :peer.stop(peer_pid) end)
  end

  def start_peer_node(name, opts \\ []) do
    index = System.unique_integer([:positive])
    {peer_pid, node_name} = start_bare_peer(name, index)

    # Adjust the environment for the peer node.
    :erpc.call(node_name, :application, :set_env, [:flezha, :env, :test])

    # The peer runs on the host's configuration for each of these.
    for app <- [:flezha, :kafka_ex, :logger], do: copy_env(node_name, app)

    # Disable the testnet for the peer node.
    :erpc.call(node_name, Application, :put_env, [:flezha, :testnet_enabled, false])

    # A peer BEAM is a second controller on this host, so it gets its own of
    # every resource one instance owns exclusively: two controllers over one
    # entity WAL tree are two unsynchronised writers, and `<hub registry
    # root>/<hub_id>` is not keyed by node either, so one root is one
    # `registry.dets`. A caller may hand it this node's value instead — a
    # cross-controller state transfer needs the peer to hold this node's files,
    # and a co-location scenario drives the misconfiguration deliberately.
    for {key, value} <- peer_resources(index, opts) do
      :erpc.call(node_name, Application, :put_env, [:flezha, key, value])
    end

    # Peer nodes only need UDS transport (avoids QUIC port conflicts).
    :erpc.call(node_name, Application, :put_env, [:flezha, :transports, [:uds]])

    # The peer copies the host's whole `:flezha` env, but it must not re-run
    # the host's boot hooks. `:post_hooks` blocks `Application.start/2` until
    # both `:external_processes` runners have connected, which is what makes a
    # busy machine miss the boot budget; the runners themselves stay, because
    # the peer only joins the `default` hub cluster once one connects, and
    # callers already wait for that.
    for key <- [:pre_hooks, :post_hooks] do
      :erpc.call(node_name, Application, :put_env, [:flezha, key, []])
    end

    # Extra `:flezha` env for this peer, applied before its boot: the way a
    # caller gives the peer a controller role the suite's own config turns off,
    # such as the convergence Loop every real controller runs.
    for {key, value} <- Keyword.get(opts, :app_env, []) do
      :erpc.call(node_name, Application, :put_env, [:flezha, key, value])
    end

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

  defp copy_env(node_name, app) do
    Enum.each(Application.get_all_env(app), fn {key, value} ->
      :erpc.call(node_name, Application, :put_env, [app, key, value])
    end)
  end

  # A peer's resources come from the same derivation the run's own do
  # (`Flezha.Config.Environment.per_run/2`), shifted by the peer's index — so a
  # resource is added to the peer by adding it to the table, not here. A caller
  # may still hand the peer this node's value instead: a cross-controller state
  # transfer needs the peer to hold this node's files, and a co-location
  # scenario drives the misconfiguration deliberately.
  defp peer_resources(index, opts) do
    derived = Environment.per_run(Config.get(:run_token), index)

    ExUnit.Callbacks.on_exit(fn ->
      Enum.each([derived.state_root, derived.socket_files_path], &File.rm_rf/1)
    end)

    for key <- @owned_resources,
        do: {key, Keyword.get_lazy(opts, key, fn -> Map.fetch!(derived, key) end)}
  end

  # `System.unique_integer/1` restarts low in every BEAM, so the run token is
  # what keeps two suites on one host from minting the same peer name.
  defp peer_name(name, index), do: :"#{name}_#{Config.get(:run_token)}_#{index}"

  @doc """
  Claim a fresh `<prefix>_<n>` directory under `System.tmp_dir!/0`.

  `System.unique_integer/1` restarts low in every BEAM, so run N+1 re-mints run
  N's names, and `mkdir_p` would silently share the tree the previous run left.
  `File.mkdir/1` is the claim: it fails on a name somebody already holds.
  """
  @spec claim_tmp_dir(binary()) :: binary()
  def claim_tmp_dir(prefix) do
    dir = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")

    case File.mkdir(dir) do
      :ok -> dir
      {:error, :eexist} -> claim_tmp_dir(prefix)
      {:error, reason} -> raise File.Error, reason: reason, action: "make directory", path: dir
    end
  end

  def stop_peer_node(peer_pid, peer_node) do
    if Process.alive?(peer_pid) do
      # Gracefully stop the peer node to allow migration.
      tolerate(fn -> :erpc.call(peer_node, :application, :stop, [:flezha]) end)
      tolerate(fn -> :peer.stop(peer_pid) end)
    end
  end

  # A peer BEAM is linked to the test process, so by the time a teardown runs
  # the node may already be on its way down: the control process outlives the
  # distribution connection by a moment, and every way of asking a gone node to
  # stop reports something different. A node that cannot be reached is a node
  # that needs no stopping.
  defp tolerate(fun) do
    fun.()
  catch
    _, _ -> :ok
  end

  def start_runner_remote(node, namespace), do: on_peer(node, :start_runner_recv, [namespace])

  def stop_runner_remote(node, namespace, runner_id),
    do: on_peer(node, :stop_runner_recv, [namespace, runner_id])

  # The step runs in a process on the peer that outlives its answer, because a
  # runner's port closes with the process that opened it. A step that ends
  # without answering, its runner's connect budget spent or the peer gone,
  # ends this wait with the peer's own reason.
  defp on_peer(node, step, args) do
    tag = make_ref()
    {_pid, monitor} = Node.spawn_monitor(node, __MODULE__, step, args ++ [{self(), tag}])

    receive do
      {^tag, answer} ->
        Process.demonitor(monitor, [:flush])
        answer

      {:DOWN, ^monitor, :process, _pid, reason} ->
        raise "#{step} on #{node} gave no answer: #{Exception.format_exit(reason)}"
    end
  end

  def start_runner_recv(namespace, reply_to), do: reply(reply_to, start_runner(namespace))

  def stop_runner_recv(namespace, runner_id, reply_to),
    do: reply(reply_to, stop_runner(namespace, runner_id))

  defp reply({pid, tag}, answer) do
    send(pid, {tag, answer})

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

    EventManager.add_handler(:runner_connected_event)

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
    EventManager.rem_handler(:runner_connected_event)

    {port, runner_id}
  end

  # ---------- SDK conformance: the runtime axis ----------
  #
  # The capability vocabulary: what a `test/system` module declares through
  # `FlezhaTest.SystemCase` and what a manifest carries or defers. One entry per
  # row of the runner matrix in `docs/architecture/runner.md`.
  @capabilities [
    :runner_lifecycle,
    :connect_discovery,
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

  # The surface every SDK carries; a manifest cannot defer these, and every
  # capability no manifest defers belongs here — the gate refuses drift in both
  # directions. The list is written, never derived: an SDK that genuinely cannot
  # carry one defers it by name in its own manifest and leaves this list in the
  # same change, so a mandate is never dropped as a side effect of a manifest
  # edit.
  #
  # `pool_wedge` is required because a host that cannot be wedged on demand
  # cannot prove it recovers: its fixtures are what exercise the abandon path,
  # the worker replacement and the classifier's verdict on that SDK rather than
  # on the reference alone.
  @required_capabilities [
    :runner_lifecycle,
    :connect_discovery,
    :graph_builder,
    :liveness,
    :retry_ladder,
    :partitioned,
    :pool_wedge,
    :entry_idle,
    :state_persistence,
    :keyed_channels,
    :dynamic_entity,
    :schema_evolution,
    :entity_timers,
    :effectively_once,
    :correction_window,
    :workflow
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

  @doc """
  Scope `runtime` to the calling process (`FlezhaTest.SystemCase` publishes it
  in both `setup_all` and `setup`, which ExUnit runs in different processes).
  """
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

  @doc """
  A runner's stdout until `enough?` says the answer has arrived, or the budget
  runs out. `seen` carries what an earlier read already took, so two reads of
  one port never lose the first one's tail.

  What a runner prints is the only observable effect of some fixtures, so this
  is how a scenario watches its own application code rather than the
  controller's answer to it.
  """
  @spec read_port(port(), (String.t() -> boolean()), non_neg_integer(), String.t()) :: String.t()
  def read_port(port, enough?, budget_ms \\ 30_000, seen \\ "") do
    read_port_until(port, enough?, System.monotonic_time(:millisecond) + budget_ms, seen)
  end

  defp read_port_until(port, enough?, deadline, seen) do
    remaining = deadline - System.monotonic_time(:millisecond)

    cond do
      enough?.(seen) ->
        seen

      remaining <= 0 ->
        seen

      true ->
        receive do
          {^port, {:data, data}} -> read_port_until(port, enough?, deadline, seen <> data)
        after
          remaining -> seen
        end
    end
  end

  def stop_runner(namespace, runner_id, opts \\ []) do
    # QUIC session teardown runs more cleanup than UDS, so allow a wider
    # default window; callers can tighten it via `timeout: N` when testing
    # UDS-only paths that should disconnect quickly.
    timeout = Keyword.get(opts, :timeout, 5_000)

    EventManager.add_handler(:runner_disconnected_event)
    FleetManager.disconnect_runner(namespace, runner_id)

    receive do
      {:runner_disconnected_event, {^namespace, ^runner_id}} -> :ok
    after
      timeout -> raise "temporary test runner not disconnected"
    end

    # Remove the handler to duplicate events.
    EventManager.rem_handler(:runner_disconnected_event)

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
  Poll `fun` every `interval_ms` until it returns a truthy value (returned) or
  `timeout_ms` elapses (returns the last falsy result). The one polling loop in
  the suite: a state read is the settle signal of last resort, and its interval
  is the suite's one deliberate sleep.
  """
  def wait_until(fun, timeout_ms, interval_ms \\ 50) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline, interval_ms)
  end

  defp do_wait_until(fun, deadline, interval_ms) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) >= deadline do
          falsy
        else
          Process.sleep(interval_ms)
          do_wait_until(fun, deadline, interval_ms)
        end

      truthy ->
        truthy
    end
  end

  @doc """
  A real, never-bound primary `StreamChannel` under a fresh registered name, and
  the pid serving it — the state every operative's data stream passes through
  between the runner's control-channel handshake and the stream the SDK opens
  after it. Stopped on exit.

  A channel in this state answers every `io_in/3` with `{:error, :no_stream}`,
  which is the window `input-delivery-guarantee` is about. It stays unbound
  until the caller binds a stream to it.
  """
  def unbound_stream_channel(runner_id \\ "test_runner") do
    name = :"unbound_chan_#{System.unique_integer([:positive])}"
    {:ok, pid} = StreamChannel.start_link({runner_id, name, :primary})
    ExUnit.Callbacks.on_exit(fn -> ensure_stopped(pid) end)
    {name, pid}
  end

  @doc """
  Block until told to `:unpark` — the body of a placeholder process that has to
  stay alive for the test's duration (`spawn(&park/0)`, `spawn_link(&park/0)`).
  """
  def park(), do: receive(do: (:unpark -> :ok))

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
