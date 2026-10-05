defmodule Mix.Tasks.Bench.Fleet do
  @moduledoc """
  Runner-fleet lifecycle shared by the bench tasks.

  `mix bench` and `mix bench.roundtrip` both need the same three things: spawn
  N SDK runner processes into a scratch namespace, wait for each to hand-shake,
  and tear the namespace down afterwards. This module owns that; the tasks own
  the workload.

  The runtime (`:rust` / `:java` / `:elixir`) is read from the `:bench_runtime` process key,
  which the calling task sets once before any fleet call. A runner is launched
  from its test app's manifest (`test/apps/<runtime>_runner/conformance.json`,
  `bench` profile) — the same line the e2e harness renders for its `test` profile.
  """

  alias ElixirRunner.BenchFactories
  alias Flezha.AuthZ
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Config
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.EventManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PortPilot
  alias Flezha.State.Paths
  alias Mix.Tasks.Bench.Report

  # Wide enough that no bench dimension meets a quota. Only the counts a bench can
  # drive are raised; the byte and rate ceilings keep their defaults.
  @bench_quotas %{
    max_relays: 1_000_000,
    max_cells_per_relay: 1_000_000,
    max_runners: 1_024
  }

  @doc """
  Resolve a `--runtime` value against the SDK test-app manifests.

  `nil` is the default fleet. `capability` is the one this bench cannot run
  without — a runtime whose manifest defers it is refused, so a new SDK's
  refusals arrive with its manifest rather than with a clause here.
  `needed_by` names what asked for the capability, for the refusal text.
  """
  @spec parse_runtime(String.t() | nil, atom() | nil, String.t() | nil) :: atom()
  def parse_runtime(name, capability \\ nil, needed_by \\ nil)

  def parse_runtime(nil, _capability, _needed_by), do: :rust

  def parse_runtime(name, capability, needed_by) do
    runtimes = PortPilot.runtimes()

    runtime =
      Enum.find(runtimes, &(Atom.to_string(&1) == name)) ||
        Mix.raise(
          "unknown --runtime #{inspect(name)}; expected one of: " <>
            Enum.map_join(runtimes, ", ", &Atom.to_string/1)
        )

    deferred = PortPilot.manifest(runtime)["deferred"] || []

    if capability && Atom.to_string(capability) in deferred do
      asked_for = needed_by || "#{capability} fixture"

      Mix.raise(
        "--runtime #{runtime} carries no #{asked_for}: its SDK defers " <>
          "#{capability} (#{PortPilot.manifest_path(runtime)}), so there is no " <>
          "fixture to measure"
      )
    end

    runtime
  end

  @doc "True when `namespace` already has at least one connected runner."
  def runners_available?(namespace) do
    case FleetManager.active_runners(namespace) do
      engines when is_list(engines) and length(engines) > 0 -> true
      _ -> false
    end
  end

  @doc """
  Spawn `count` runners into `namespace`, blocking until each connects.

  `extra_env` is a list of `{"VAR", value}` tuples handed to the runner
  process — how `--pool-sizes` threads `FLEZHA_WORKER_POOL_SIZE` through.
  """
  def setup(namespace, count, extra_env) do
    runtime = runtime()
    transport = Config.get(:transport)

    ensure_namespace(namespace)

    if runtime in [:rust, :java] and transport == :native do
      raise "runtime=#{runtime} cannot run the :native transport (that mode is the " <>
              "in-process Elixir SDK). Use --runtime elixir, or -t uds|quic."
    end

    cond do
      runtime == :elixir and transport == :native ->
        # In-process SDK: ONE runner (the runner slot is process-wide). This is
        # the same-runtime uds-vs-native transport measurement.
        IO.puts("Starting the in-process Elixir SDK runner (native transport)...")
        start_native_sdk(namespace)

      true ->
        IO.puts("Setting up test environment with compute engines...")
        runner_ids = start_runners(namespace, count, extra_env ++ runner_credential(namespace))
        IO.puts("Started #{length(runner_ids)} compute engines for performance testing")
        Process.put(:bench_runners, runner_ids)
    end
  end

  # The bench runs under `MIX_ENV=prod`, where `authz_enabled` defaults to true,
  # so on a box whose `flz auth bootstrap` has been claimed a runner connect is
  # refused twice over: `:namespace_unknown` until the catalog carries this
  # namespace, and `:namespace_not_proven` until the connect presents a runner
  # credential for it. The harness establishes both preconditions rather than
  # opting out of enforcement: a bench that turned authz off would measure a
  # configuration that ships nowhere, and would never exercise the admission
  # path at all.
  #
  # Both are gated on the same predicate `Quotas.check_allocation/3` reads, so a
  # run that will not be enforced writes no catalog row and issues no credential.
  #
  # Quotas are wide enough that the bench never measures a ceiling: the defaults
  # cap a namespace at 100 relays, which `-n 100` sits exactly on.
  defp ensure_namespace(namespace) do
    if AuthZ.enabled?() do
      case NamespaceStore.create(namespace, quotas: @bench_quotas, created_by_principal: "bench") do
        {:ok, _entry} ->
          IO.puts("Registered namespace #{namespace} in the authz catalog")

        {:error, :already_exists} ->
          :ok

        {:error, reason} ->
          Mix.raise("could not register bench namespace #{namespace}: #{inspect(reason)}")
      end
    end
  end

  @doc """
  The credential a spawned runner proves `namespace` with, as the environment
  entry every SDK reads it from, or `[]` when nothing will be enforced.

  One is issued per run because the raw value cannot be recovered from the store
  once written, and the entries earlier runs left behind are retired first: a
  bench namespace is scratch, so every runner entry in it is one of ours.

  The in-process runner is asked for no credential (co-residence is its proof),
  so only the branch that spawns runner processes calls this.
  """
  @spec runner_credential(binary()) :: [{binary(), binary()}]
  def runner_credential(namespace) do
    if AuthZ.enabled?() do
      for %{role: :runner, revoked_at: nil, token_hash: hash} <- TokenStore.list(namespace),
          do: TokenStore.revoke(hash)

      case TokenStore.issue("runner@#{namespace}", namespace, :runner, :never,
             created_by_principal: "bench",
             label: "bench-runner"
           ) do
        {:ok, secret, _entry} ->
          IO.puts("Issued a runner credential for namespace #{namespace}")
          [{"FLEZHA_NAMESPACE_SECRET", secret}]

        {:error, reason} ->
          Mix.raise(
            "could not issue a bench runner credential for #{namespace}: #{inspect(reason)}"
          )
      end
    else
      []
    end
  end

  @doc """
  Run the runtime's own bench build before measuring with it.

  An artifact that EXISTS is not an artifact that is CURRENT, and nothing else
  in this path can tell the difference: a runner built against a two-day-old
  kernel connects, serves, and produces a plausible number. Every manifest's
  build command is incremental, and each one — cargo, maven, mix, npm — knows
  its own dependency graph, which no mtime heuristic here could. So the cheap
  correct move is to run it every time.

  `FLEZHA_BENCH_SKIP_BUILD=1` opts out for someone iterating on the harness
  rather than on a number.

  Returns `:built`, `:skipped` or `:none` so the run can report which — an
  artifact that exists tells the reader nothing about whether it is current.
  """
  @spec ensure_built(atom()) :: :built | :skipped | :none
  def ensure_built(runtime) do
    command = PortPilot.manifest(runtime)["build"]["bench"]

    cond do
      Config.get(:bench_skip_build) ->
        IO.puts("Skipping the #{runtime} bench build (FLEZHA_BENCH_SKIP_BUILD)")
        :skipped

      is_nil(command) ->
        :none

      true ->
        IO.puts("Building the #{runtime} bench runner: #{command}")

        case System.cmd("sh", ["-c", command], stderr_to_stdout: true) do
          {_output, 0} ->
            :built

          {output, status} ->
            raise "the #{runtime} bench build failed (exit #{status}); a bench against a " <>
                    "stale artifact is not a measurement.\n\n#{command}\n\n#{output}"
        end
    end
  end

  @doc "The runtime this bench run is measuring, set once by the calling task."
  @spec runtime() :: atom()
  def runtime(), do: Process.get(:bench_runtime, :rust)

  # Start the Elixir SDK in-process, advertising the bench factory trio, and
  # block until it connects over the native transport.
  defp start_native_sdk(namespace) do
    handle =
      Enum.reduce(
        BenchFactories.all(),
        Flezha.SDK.create_runner(namespace, transport: :native),
        fn m, acc ->
          Flezha.SDK.register_factory(acc, m)
        end
      )
      |> Flezha.SDK.start()

    case handle do
      %Flezha.SDK{} = h ->
        Process.put(:bench_native_sdk, h)
        wait_for_connect(namespace)

      other ->
        raise "in-process Elixir SDK failed to start: #{inspect(other)}"
    end
  end

  defp wait_for_connect(namespace, tries \\ 300) do
    cond do
      tries <= 0 ->
        raise "in-process Elixir SDK never connected"

      FleetManager.active_runners(namespace) != [] ->
        :ok

      true ->
        Process.sleep(50)
        wait_for_connect(namespace, tries - 1)
    end
  end

  @doc """
  Load every module of the controller and the libraries its start path runs
  through. A release boots with its code loaded; under `mix` the VM loads
  lazily, so without this the first relay of a run pays tens of
  milliseconds of code loading that no deployed controller pays.
  """
  def preload_code do
    modules =
      for app <- [:flezha, :process_hub],
          module <- Application.spec(app, :modules) || [],
          do: module

    :code.ensure_modules_loaded(modules)
    :ok
  end

  @doc "Stop every relay in `namespace`, disconnect its runners, drop the namespace."
  def cleanup(namespace) do
    IO.puts("Cleaning up test environment...")

    RelayManager.stop_all_relays(namespace)

    case Process.get(:bench_runners) do
      engines when is_list(engines) ->
        # A fleet manager still tearing relays down may not answer in
        # time; the namespace stop below retires the runners regardless.
        Enum.each(engines, fn engine_id ->
          try do
            FleetManager.disconnect_runner(namespace, engine_id)
          catch
            :exit, _ -> :ok
          end
        end)

      _ ->
        :ok
    end

    case Process.get(:bench_native_sdk) do
      %Flezha.SDK{} = h -> Flezha.SDK.close(h)
      _ -> :ok
    end

    FleetManager.stop_namespace(namespace)
    Process.delete(:bench_runners)
    Process.delete(:bench_ports)
    Process.delete(:bench_native_sdk)

    IO.puts("Test environment cleanup completed")
  end

  @doc """
  Remove `namespace`'s durable state and hub registry before the controller
  boots, so nothing a previous run left behind is restored into this one.

  Runs `app.config` first: until the runtime config is loaded the state root
  is the default, not the `FLEZHA_STATE_DIR` the run will boot with.
  """
  def reset_namespace(namespace) do
    Mix.Task.run("app.config")

    for dir <- [Paths.namespace_dir(namespace), Layout.file(:process_hub, "#{namespace}_hub")] do
      File.rm_rf!(dir)
    end
  end

  @doc "Spawn `count` runner processes and return their connected runner ids."
  def start_runners(namespace, count, extra_env) do
    runtime = runtime()
    build = ensure_built(runtime)

    Report.record_artifact(
      Report.artifact(runtime, PortPilot.resolve_artifact(runtime, :bench), build)
    )

    connect_timeout = PortPilot.manifest(runtime)["connect_timeout_ms"]

    EventManager.add_handler(:runner_connected_event)

    # Every runner is spawned before the first hand-shake is awaited: the
    # processes boot side by side, and the wait is one connect, not N.
    command = PortPilot.launch_command(runtime, :bench, namespace)

    ports =
      for _ <- 1..count do
        {port, _command} = PortPilot.open_port(command, env: extra_env)
        port
      end

    # Kept so a bench can SIGKILL one runner (`Port.info(port, :os_pid)`).
    Process.put(:bench_ports, ports)

    runner_ids =
      for i <- 1..count do
        receive do
          {:runner_connected_event, {^namespace, id}} -> id
        after
          connect_timeout -> raise "Performance test compute engine #{i} failed to connect"
        end
      end

    EventManager.rem_handler(:runner_connected_event)
    runner_ids
  end
end
