defmodule Mix.Tasks.Bench.Fleet do
  @moduledoc """
  Runner-fleet lifecycle shared by the bench tasks.

  `mix bench` and `mix bench.steps` both need the same three things: spawn
  N SDK runner processes into a scratch namespace, wait for each to hand-shake,
  and tear the namespace down afterwards. This module owns that; the tasks own
  the workload.

  The runtime (any SDK `parse_runtime/3` resolves) is read from the
  `:bench_runtime` process key, which the calling task sets once before any
  fleet call. A runner is launched
  from its test app's manifest (`test/apps/<runtime>_runner/conformance.json`,
  `bench` profile) — the same line the e2e harness renders for its `test` profile.
  """

  alias Flezha.AuthZ
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Config
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Runner.FleetManager
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
    ensure_namespace(namespace)
    IO.puts("Setting up test environment with compute engines...")
    runner_ids = start_runners(namespace, count, extra_env ++ runner_credential(namespace))
    IO.puts("Started #{length(runner_ids)} compute engines for performance testing")
    Process.put(:bench_runners, runner_ids)
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
  # The catalog row is gated on the predicate `Quotas.check_allocation/3` reads,
  # and the credential on the posture `Flezha.AuthZ.RunnerProof` reads, which
  # asks for one before the bootstrap is claimed too.
  #
  # Quotas are wide enough that the bench never measures a ceiling: the defaults
  # cap a namespace at 100 relays, which `-n 100` sits exactly on.
  defp ensure_namespace(namespace) do
    if AuthZ.enabled?() do
      case NamespaceStore.create(namespace, quotas: @bench_quotas, created_by_principal: "bench") do
        {:ok, _entry} ->
          IO.puts("Registered namespace #{namespace} in the authz catalog")

        {:error, {:namespace_exists, ^namespace}} ->
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
    if AuthZ.enforcement_posture() != :open do
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
  Run the runtime's own build for `profile` (`:bench` or `:test`) before
  running it.

  The one place a runner is built: the bench calls it, and CI and the gate rows
  reach it through `scripts/build-runner.sh`. The command is the manifest's
  `build.<profile>`, so a manifest is the only place a build is spelled.

  An artifact that EXISTS is not an artifact that is CURRENT, and nothing else
  in this path can tell the difference: a runner built against a two-day-old
  kernel connects, serves, and produces a plausible number. Every manifest's
  build command is incremental, and each one — cargo, maven, mix, npm — knows
  its own dependency graph, which no mtime heuristic here could. So the cheap
  correct move is to run it every time.

  `FLEZHA_BENCH_SKIP_BUILD=1` opts out of the bench build for someone iterating
  on the harness rather than on a number.

  Returns `:built`, `:skipped` or `:none` so the run can report which — an
  artifact that exists tells the reader nothing about whether it is current.
  A manifest without a `build.test` raises: a test run has no report to say so.
  """
  @spec ensure_built(atom(), :bench | :test) :: :built | :skipped | :none
  def ensure_built(runtime, profile \\ :bench) do
    command = PortPilot.manifest(runtime)["build"][Atom.to_string(profile)]

    cond do
      profile == :bench and Config.get(:bench_skip_build) ->
        IO.puts("Skipping the #{runtime} bench build (FLEZHA_BENCH_SKIP_BUILD)")
        :skipped

      is_nil(command) and profile == :test ->
        raise "the #{runtime} conformance manifest declares no build.test; every test run " <>
                "builds its runner first, so nothing may run against an artifact left from before"

      is_nil(command) ->
        :none

      true ->
        IO.puts("Building the #{runtime} #{profile} runner: #{command}")

        # Streamed, so a warning the build prints reaches the caller's log.
        case System.cmd("sh", ["-c", command],
               stderr_to_stdout: true,
               into: IO.binstream(:stdio, :line)
             ) do
          {_output, 0} ->
            :built

          {_output, status} ->
            raise "the #{runtime} #{profile} build failed (exit #{status}); nothing may run " <>
                    "against a stale artifact. Its output is above.\n\n#{command}"
        end
    end
  end

  @doc """
  Let `namespace`'s runners send to its relays, as runner code does, by
  binding their principal the `spawner` role, the narrowest one granting
  `relay.send`. Nothing is written when nothing will be enforced.
  """
  @spec grant_send(binary()) :: :ok
  def grant_send(namespace) do
    if AuthZ.enabled?() do
      with {:error, reason} <-
             RoleStore.bind("runner@#{namespace}", namespace, :spawner, bound_by: "bench"),
           do: Mix.raise("could not let #{namespace}'s runners send: #{inspect(reason)}")
    end

    :ok
  end

  @doc "The runtime this bench run is measuring, set once by the calling task."
  @spec runtime() :: atom()
  def runtime(), do: Process.get(:bench_runtime, :rust)

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

    FleetManager.stop_namespace(namespace)
    Process.delete(:bench_runners)
    Process.delete(:bench_ports)

    IO.puts("Test environment cleanup completed")
  end

  @doc """
  Whether the durability tier keeps lineages this run cannot clear: a store the
  bench does not own, which an earlier run's relays may be restored from.
  """
  def durability_kept?() do
    case Config.get(:durability_backend) do
      :local_path -> not durability_owned?()
      backend -> backend in [:postgres, :s3]
    end
  end

  # Remote keys carry no namespace, so the bench can only clear a whole store,
  # and only one the wrapper chose for it.
  defp durability_owned?(),
    do: Config.get(:durability_backend) == :local_path and Config.get(:bench_durability_owned)

  @doc """
  Remove `namespace`'s durable state and hub registry before the controller
  boots, so nothing a previous run left behind is restored into this one. The
  bench's own `local_path` durability store goes with it.

  Runs `app.config` first: until the runtime config is loaded the state root
  is the default, not the `FLEZHA_STATE_DIR` the run will boot with.
  """
  def reset_namespace(namespace) do
    Mix.Task.run("app.config")

    for dir <- [Paths.namespace_dir(namespace), Layout.file(:process_hub, "#{namespace}_hub")] do
      File.rm_rf!(dir)
    end

    if durability_owned?(), do: File.rm_rf!(Config.get(:durability_path))
  end

  @doc "Spawn `count` runner processes and return their connected runner ids."
  def start_runners(namespace, count, extra_env) do
    runtime = runtime()
    build = ensure_built(runtime)

    Report.record_artifact(
      Report.artifact(runtime, PortPilot.resolve_artifact(runtime, :bench), build)
    )

    connect_timeout = PortPilot.manifest(runtime)["connect_timeout_ms"]

    handler = {__MODULE__, make_ref()}
    bench = self()

    :ok =
      :telemetry.attach(
        handler,
        [:flezha, :runner, :connected],
        fn _event, _measurements, metadata, _config ->
          if metadata.namespace == namespace, do: send(bench, {handler, metadata.runner})
        end,
        nil
      )

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
          {^handler, id} -> id
        after
          connect_timeout -> raise "Performance test compute engine #{i} failed to connect"
        end
      end

    :telemetry.detach(handler)
    runner_ids
  end
end
