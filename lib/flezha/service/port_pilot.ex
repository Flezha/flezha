defmodule Flezha.Service.PortPilot do
  require Logger

  alias Flezha.Config
  alias Flezha.Config.Backoff
  alias Flezha.Config.Durations
  alias Flezha.Socket.Support

  @port_down_counter 10

  # A runner's output is read a line at a time; a longer line arrives in
  # pieces, joined up to `@joined_bytes` before what is held is logged as is.
  @line_bytes 4096
  @joined_bytes 65_536

  use GenServer
  use Flezha.Process.Timer, keys: [:counter_reset]

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @doc """
  Spawn `command` as a port with the runner's transport environment.

  `:env` adds environment entries; `:port_opts` adds `Port.open/2` options
  beyond `:binary` and the env (a caller that needs `:exit_status` — proving a
  runner really ended non-zero — asks for it here rather than opening its own
  port and re-deriving this environment).
  """
  def open_port(command, opts \\ []) do
    socket_files_path = Support.socket_dir()

    transport =
      Keyword.get(opts, :transport, Config.get(:transport))
      |> to_string()

    ca_cert_path = Config.get(:ca_cert_path)

    quic_address = Config.get(:quic_listen_address)
    # Convert 0.0.0.0 to 127.0.0.1 for the SDK (client can't connect to 0.0.0.0)
    client_address = String.replace(quic_address, "0.0.0.0", "127.0.0.1")

    base_env = [
      {var(:socket_files_path), String.to_charlist(socket_files_path)},
      {var(:transport), String.to_charlist(transport)},
      {var(:ca_cert_path), String.to_charlist(ca_cert_path)},
      {var(:quic_addresses), String.to_charlist(client_address)}
    ]

    extra_env =
      opts
      |> Keyword.get(:env, [])
      |> Enum.map(fn {k, v} ->
        {String.to_charlist(to_string(k)), String.to_charlist(to_string(v))}
      end)

    # An entry the caller supplies REPLACES the base one of the same name rather
    # than trailing it, so a scenario can point a runner somewhere else — or
    # blank a variable to make it walk the ladder — without depending on how
    # the OS resolves a duplicate.
    env =
      Enum.reduce(extra_env, base_env, fn {name, value}, acc ->
        List.keystore(acc, name, 0, {name, value})
      end)

    port =
      Port.open(
        {:spawn, command},
        [:binary, {:env, env} | Keyword.get(opts, :port_opts, [])]
      )

    Port.monitor(port)

    {port, command}
  end

  # ---------- SDK test-app manifests ----------
  #
  # `test/apps/<runtime>_runner/conformance.json` declares how a runtime's test
  # app is launched. The e2e harness (`:test` profile) and the bench fleet
  # (`:bench` profile) both render their launch line from it, so neither
  # carries a per-runtime clause.

  @apps_root "test/apps"
  @manifest_file "conformance.json"

  @doc "Every runtime with a test-app manifest, as atoms."
  @spec runtimes() :: [atom()]
  def runtimes do
    Path.wildcard(Path.join([@apps_root, "*_runner", @manifest_file]))
    |> Enum.map(fn path ->
      path
      |> Path.dirname()
      |> Path.basename()
      |> String.trim_trailing("_runner")
      |> String.to_atom()
    end)
    |> Enum.sort()
  end

  @doc "Manifest path of `runtime`'s test app, relative to the repo root."
  @spec manifest_path(atom()) :: Path.t()
  def manifest_path(runtime), do: Path.join([@apps_root, "#{runtime}_runner", @manifest_file])

  @doc "The decoded manifest of `runtime`, read once per process."
  @spec manifest(atom()) :: map()
  def manifest(runtime) do
    key = {__MODULE__, :manifest, runtime}

    case Process.get(key) do
      nil ->
        path = manifest_path(runtime)

        if not File.exists?(path) do
          raise ArgumentError,
                "no SDK test app for runtime #{inspect(runtime)}: #{path} is missing " <>
                  "(manifests exist for #{inspect(runtimes())})"
        end

        manifest = path |> File.read!() |> Jason.decode!()
        Process.put(key, manifest)
        manifest

      manifest ->
        manifest
    end
  end

  @doc """
  The runtime's allocated SDK language byte, as its manifest declares it.

  The byte the runtime stamps at byte 0 of every durable record and advertises
  on every factory (`flz_core::header`'s allocation table). Distinct per
  runtime: the controller's placement guard reads it, so two runtimes sharing a
  value would make the guard meaningless.
  """
  @spec sdk_lang(atom()) :: pos_integer() | nil
  def sdk_lang(runtime), do: manifest(runtime)["sdk_lang"]

  @doc """
  How `runtime`'s host executes activations: `%{"model" => …, "detail" => …,
  "env" => %{var => what it selects}}`.

  A throughput number taken under one dispatch topology and one taken under
  another are different measurements of the same fixture, and only the
  manifest knows which — so a reporter reads this rather than carrying a
  clause per runtime, exactly as the launch line already does.
  """
  @spec dispatch(atom()) :: map()
  def dispatch(runtime), do: manifest(runtime)["dispatch"] || %{}

  @doc """
  The shell line that launches `runtime`'s test app for `profile` (`:test` or
  `:bench`) into `namespace`, rendered from the manifest's command template.

  `{artifact}` is the first path of the profile's `artifacts` list that exists
  (a later one is used with a warning); none existing raises naming the
  profile's `build` command. `{socket_dir}`, `{ca_cert}`, `{client_cert}` and
  `{client_key}` are absolute, because the app runs in its own cwd.

  `{socket_dir_rel}` is the same directory relative to `{app_dir}`, for a
  template that `cd`s there: a UDS path must fit in `SUN_LEN` (108 bytes), and
  the absolute form does not in a deep checkout — which is why the app that
  keeps the controller's own relative path connects and the one that resolves
  it against `/` does not.
  """
  @spec launch_command(atom(), :test | :bench, String.t()) :: String.t()
  def launch_command(runtime, profile, namespace) when profile in [:test, :bench] do
    manifest = manifest(runtime)
    profile_key = Atom.to_string(profile)
    repo = File.cwd!()
    app_dir = app_dir(runtime)

    socket_dir = absolute(Support.socket_dir())

    {artifact, _resolution} = resolve_artifact(runtime, profile)

    vars = %{
      "artifact" => artifact,
      "app_dir" => app_dir,
      "repo" => repo,
      "namespace" => namespace,
      "socket_dir" => socket_dir,
      "ca_cert" => absolute(Config.get(:ca_cert_path)),
      "client_cert" => absolute(Config.get(:client_cert_path)),
      "client_key" => absolute(Config.get(:client_key_path))
    }

    Regex.replace(~r/\{(\w+)\}/, manifest["command"][profile_key], fn whole, name ->
      Map.get(vars, name) ||
        raise ArgumentError,
              "#{manifest_path(runtime)}: unknown placeholder #{whole} in the #{profile} command"
    end)
  end

  @doc """
  The artifact `runtime`'s `profile` actually launches, and how it was found:
  `{path, :first}` or `{path, {:fallback, first_choice}}`.

  Public because a run has to be able to REPORT what it launched, and a second
  derivation is a second answer that can disagree with the first. One
  resolution, two readers: the launch line and the provenance block.
  """
  @spec resolve_artifact(atom(), :test | :bench) ::
          {String.t(), :first | {:fallback, String.t()}}
  def resolve_artifact(runtime, profile) when profile in [:test, :bench] do
    resolve_artifact(runtime, Atom.to_string(profile), app_dir(runtime), manifest(runtime))
  end

  # An SDK developed in a repository of its own is a submodule here, and its
  # test app directory a symlink into it. The launch line and the artifacts are
  # resolved against where the app really is: cargo, for one, resolves a
  # `path = ".."` dependency against the name it was given, not its target.
  defp app_dir(runtime) do
    dir = Path.join([File.cwd!(), @apps_root, "#{runtime}_runner"])

    case File.read_link(dir) do
      {:ok, target} -> Path.expand(target, Path.dirname(dir))
      {:error, _not_a_link} -> dir
    end
  end

  defp resolve_artifact(runtime, profile, app_dir, manifest) do
    candidates = Enum.map(manifest["artifacts"][profile], &Path.expand(&1, app_dir))

    case Enum.find_index(candidates, &File.exists?/1) do
      nil ->
        raise "no #{profile} artifact for the #{runtime} test app (none of " <>
                "#{inspect(candidates)} exists); build it with `#{manifest["build"][profile]}`"

      0 ->
        {hd(candidates), :first}

      index ->
        # A fallback is only real if the command launches the artifact it was
        # given. A template that never mentions `{artifact}` — one that names
        # its own build profile inline, say — runs the same thing whichever
        # candidate matched, so "falling back" would print a reassuring line
        # and then fail somewhere less obvious. Refuse instead, naming the
        # build that is actually missing.
        unless String.contains?(manifest["command"][profile], "{artifact}") do
          raise "the #{runtime} #{profile} runner needs #{hd(candidates)}, which does not " <>
                  "exist; its launch command does not take an artifact, so the later " <>
                  "candidates cannot serve in its place — build it with " <>
                  "`#{manifest["build"][profile]}`"
        end

        Logger.warning(
          "the #{runtime} #{profile} runner is #{Enum.at(candidates, index)} because " <>
            "#{hd(candidates)} does not exist; build it with `#{manifest["build"][profile]}`"
        )

        {Enum.at(candidates, index), {:fallback, hd(candidates)}}
    end
  end

  defp absolute(path), do: Path.expand(path, File.cwd!())

  @impl true
  def init(_) do
    ports = initialize_ports()

    {:ok,
     arm_counter_reset(%{
       ports: ports,
       response_refs: %{},
       port_down_counter: 0,
       partial: %{}
     })}
  end

  @impl true
  def handle_info(:reset_counter, state) do
    {:noreply, state |> arm_counter_reset() |> Map.put(:port_down_counter, 0)}
  end

  # A runner's own output, logged a line at a time naming the runner.
  @impl true
  def handle_info({port, {:data, {:noeol, chunk}}}, state) do
    held = [Map.get(state.partial, port, "") | chunk]

    if IO.iodata_length(held) >= @joined_bytes do
      log_runner_line(state, port, held)
      {:noreply, %{state | partial: Map.delete(state.partial, port)}}
    else
      {:noreply, put_in(state.partial[port], held)}
    end
  end

  def handle_info({port, {:data, {:eol, line}}}, state) do
    {held, partial} = Map.pop(state.partial, port, "")
    log_runner_line(state, port, [held | line])
    {:noreply, %{state | partial: partial}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :port, port, :normal}, state) do
    {held, partial} = Map.pop(state.partial, port)
    if held, do: log_runner_line(state, port, held)
    state = %{state | partial: partial}
    Logger.error("Port application down")

    counter = Map.get(state, :port_down_counter, 0) + 1
    ports = Map.delete(state.ports, port)

    ports =
      case Map.get(state.ports, port) do
        nil ->
          ports

        command ->
          if counter > @port_down_counter do
            Logger.error(
              "Port application malfunction — too many crashes (#{counter}), " <>
                "backing off #{Backoff.delay(:port_restart)}ms"
            )

            Process.send_after(
              self(),
              {:restart_port, command},
              Backoff.arm(:port_restart, counter, command)
            )

            ports
          else
            {new_port, _} = open_runner(command)
            Map.put(ports, new_port, command)
          end
      end

    {:noreply, %{state | ports: ports, port_down_counter: counter}}
  end

  @impl true
  def handle_info({:restart_port, command}, state) do
    Logger.info("Port application restarting after backoff")
    {port, _} = open_runner(command)
    ports = Map.put(state.ports, port, command)
    {:noreply, %{state | ports: ports, port_down_counter: 0}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Port manager received unexpected message: #{inspect(msg)}")

    {:noreply, state}
  end

  defp initialize_ports() do
    external_processes = Config.get(:external_processes)

    Enum.map(external_processes, &open_runner/1)
    |> Map.new()
  end

  defp open_runner(command),
    do: open_port(command, port_opts: [:stderr_to_stdout, {:line, @line_bytes}])

  # The level the line's own prefix names, as the core writes it.
  defp log_runner_line(state, port, line) do
    line = IO.iodata_to_binary(line)

    level =
      case line do
        "[flezha] warning: " <> _ -> :warning
        "[flezha] error: " <> _ -> :error
        _ -> :info
      end

    Logger.log(level, "[runner #{Map.get(state.ports, port, inspect(port))}] #{line}")
  end

  defp arm_counter_reset(state) do
    arm_timer(state, :counter_reset, Durations.get(:port_counter_reset_ms), :reset_counter)
  end

  # A spawned runner reads its own environment, so the controller hands it the
  # names the table declares rather than four literals of its own.
  defp var(name), do: String.to_charlist(Config.env_name(name))
end
