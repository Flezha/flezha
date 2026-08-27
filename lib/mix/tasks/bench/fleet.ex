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

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PortPilot

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
    runtime = Process.get(:bench_runtime, :rust)
    transport = Application.get_env(:flezha, :transport, :uds)

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
        runner_ids = start_runners(namespace, count, extra_env)
        IO.puts("Started #{length(runner_ids)} compute engines for performance testing")
        Process.put(:bench_runners, runner_ids)
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
  """
  def ensure_built(runtime) do
    command = PortPilot.manifest(runtime)["build"]["bench"]

    cond do
      System.get_env("FLEZHA_BENCH_SKIP_BUILD") in ["1", "true"] ->
        IO.puts("Skipping the #{runtime} bench build (FLEZHA_BENCH_SKIP_BUILD)")

      is_nil(command) ->
        :ok

      true ->
        IO.puts("Building the #{runtime} bench runner: #{command}")

        case System.cmd("sh", ["-c", command], stderr_to_stdout: true) do
          {_output, 0} ->
            :ok

          {output, status} ->
            raise "the #{runtime} bench build failed (exit #{status}); a bench against a " <>
                    "stale artifact is not a measurement.\n\n#{command}\n\n#{output}"
        end
    end
  end

  # Start the Elixir SDK in-process, advertising the bench factory trio, and
  # block until it connects over the native transport.
  defp start_native_sdk(namespace) do
    handle =
      Enum.reduce(
        Mix.Tasks.Bench.ElixirFactories.all(),
        Flezha.SDK.create_runner(namespace, transport: :native),
        fn m, acc ->
          Flezha.SDK.define_stream(acc, m)
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
  lazily, so without this the first operative of a run pays tens of
  milliseconds of code loading that no deployed controller pays.
  """
  def preload_code do
    modules =
      for app <- [:flezha, :process_hub, :kafka_ex, :kayrock],
          module <- Application.spec(app, :modules) || [],
          do: module

    :code.ensure_modules_loaded(modules)
    :ok
  end

  @doc "Stop every operative in `namespace`, disconnect its runners, drop the namespace."
  def cleanup(namespace) do
    IO.puts("Cleaning up test environment...")

    OperativeManager.stop_all_operatives(namespace)

    case Process.get(:bench_runners) do
      engines when is_list(engines) ->
        # A fleet manager still tearing operatives down may not answer in
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

  @doc "Spawn `count` runner processes and return their connected runner ids."
  def start_runners(namespace, count, extra_env) do
    runtime = Process.get(:bench_runtime, :rust)
    ensure_built(runtime)
    connect_timeout = PortPilot.manifest(runtime)["connect_timeout_ms"]

    Flezha.Service.EventManager.add_handler(:runner_connected_event)

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

    Flezha.Service.EventManager.rem_handler(:runner_connected_event)
    runner_ids
  end
end
