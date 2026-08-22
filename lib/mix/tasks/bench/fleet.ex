defmodule Mix.Tasks.Bench.Fleet do
  @moduledoc """
  Runner-fleet lifecycle shared by the bench tasks.

  `mix bench` and `mix bench.roundtrip` both need the same three things: spawn
  N SDK runner processes into a scratch namespace, wait for each to hand-shake,
  and tear the namespace down afterwards. This module owns that; the tasks own
  the workload.

  The runtime (`:rust` / `:java` / `:elixir`) is read from the `:bench_runtime` process key,
  which the calling task sets once before any fleet call.
  """

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager

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

  # Start the Elixir SDK in-process, advertising the bench factory trio, and
  # block until it connects over the native transport.
  defp start_native_sdk(namespace) do
    handle =
      Enum.reduce(Mix.Tasks.Bench.ElixirFactories.all(), Flezha.SDK.create_runner(namespace, transport: :native), fn m, acc ->
        Flezha.SDK.define_stream(acc, m)
      end)
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
      tries <= 0 -> raise "in-process Elixir SDK never connected"
      FleetManager.active_runners(namespace) != [] -> :ok
      true -> Process.sleep(50); wait_for_connect(namespace, tries - 1)
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
    {:ok, current_path} = :file.get_cwd()
    runtime = Process.get(:bench_runtime, :rust)
    # The elixir_runner app boots a whole flezha + SDK; if it is not pre-built
    # its first `mix run` compiles, so give it a wide connect budget. Pre-build
    # it for the target env (`cd test/apps/elixir_runner && MIX_ENV=prod mix compile`).
    # Java's JVM + Spring/JPA cold start easily exceeds a few seconds; the
    # Elixir runner compiles a whole flezha on first `mix run`.
    connect_timeout =
      case runtime do
        :elixir -> 120_000
        :java -> 60_000
        _ -> 5_000
      end

    Flezha.Service.EventManager.add_handler(:runner_connected_event)

    # Every runner is spawned before the first hand-shake is awaited: the
    # processes boot side by side, and the wait is one connect, not N.
    command = build_command(runtime, current_path, namespace)

    ports =
      for _ <- 1..count do
        {port, _command} = Flezha.Service.PortPilot.open_port(command, env: extra_env)
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

  @doc "Shell command that launches one runner of `runtime` against `namespace`."
  def build_command(:rust, current_path, namespace) do
    candidates = [
      "#{current_path}/target/release/rust_runner",
      "#{current_path}/test/apps/rust_runner/target/release/rust_runner",
      "#{current_path}/target/debug/rust_runner",
      "#{current_path}/test/apps/rust_runner/target/debug/rust_runner"
    ]

    case Enum.find(candidates, &File.regular?/1) do
      nil ->
        "cargo run --release --manifest-path=#{current_path}/test/apps/rust_runner/Cargo.toml --target-dir=./target -- #{namespace}"

      path ->
        if String.contains?(path, "/debug/") do
          IO.warn(
            "mix bench is using the DEBUG Rust runner binary at #{path} — " <>
              "this is ~5× slower than release. Build with " <>
              "`cargo build -p rust_runner --release` for representative numbers."
          )
        end

        "#{path} #{namespace}"
    end
  end

  def build_command(:elixir, current_path, namespace) do
    app_dir = Path.join([current_path, "test/apps/elixir_runner"])

    if not File.dir?(Path.join(app_dir, "lib")) do
      raise "elixir_runner app not found at #{app_dir}"
    end

    # The spawned app runs in its own cwd, so the controller's relative socket /
    # cert paths would miss it — pass them ABSOLUTE (resolved against the
    # controller's cwd). `mix run --no-halt` boots the runner-only app and blocks;
    # its stdin-EOF watcher halts the VM when the harness closes the port.
    abs = fn rel ->
      if String.starts_with?(rel, "/"), do: rel, else: Path.join(current_path, rel)
    end

    sock = abs.(Application.get_env(:flezha, :socket_files_path, "app_data/sockets/"))
    ca = abs.(Application.get_env(:flezha, :ca_cert_path, "app_data/certs/ca.pem"))
    cert = abs.(Application.get_env(:flezha, :client_cert_path, "app_data/certs/client.pem"))
    key = abs.(Application.get_env(:flezha, :client_key_path, "app_data/certs/client.key"))

    env = "FLEZHA_SOCKET_FILES_PATH=#{sock} FLEZHA_CA_CERT_PATH=#{ca} " <>
            "FLEZHA_QUIC_CLIENT_CERT=#{cert} FLEZHA_QUIC_CLIENT_KEY=#{key}"

    "sh -c 'cd #{app_dir} && #{env} exec mix run --no-halt --no-deps-check -- #{namespace}'"
  end

  def build_command(:java, current_path, namespace) do
    jar_path =
      Path.join([
        current_path,
        "test/apps/java_runner/target/java_runner.jar"
      ])

    if not File.exists?(jar_path) do
      raise """
      Java compute engine jar not found at #{jar_path}.

      Build it first with one of:
        mix test.java_sdk   # full pipeline: build flz_jni + Maven + run tests
        mvn -f test/apps/java_runner/pom.xml package

      Then retry: mix bench --runtime java ...
      """
    end

    "java -jar #{jar_path} #{namespace}"
  end
end
