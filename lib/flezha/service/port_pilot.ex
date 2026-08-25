defmodule Flezha.Service.PortPilot do
  require Logger

  @port_down_counter 10
  @backoff_ms 5_000

  use GenServer

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
    socket_files_path = Application.fetch_env!(:flezha, :socket_files_path)

    transport =
      Keyword.get(opts, :transport, Application.get_env(:flezha, :transport, :uds))
      |> to_string()

    ca_cert_path = Application.get_env(:flezha, :ca_cert_path, "app_data/certs/ca.pem")

    quic_address = Application.get_env(:flezha, :quic_listen_address, "0.0.0.0:4433")
    # Convert 0.0.0.0 to 127.0.0.1 for the SDK (client can't connect to 0.0.0.0)
    client_address = String.replace(quic_address, "0.0.0.0", "127.0.0.1")

    base_env = [
      {~c"FLEZHA_SOCKET_FILES_PATH", String.to_charlist(socket_files_path)},
      {~c"FLEZHA_TRANSPORT", String.to_charlist(transport)},
      {~c"FLEZHA_CA_CERT_PATH", String.to_charlist(ca_cert_path)},
      {~c"FLEZHA_QUIC_ADDRESS", String.to_charlist(client_address)}
    ]

    extra_env =
      opts
      |> Keyword.get(:env, [])
      |> Enum.map(fn {k, v} ->
        {String.to_charlist(to_string(k)), String.to_charlist(to_string(v))}
      end)

    env = base_env ++ extra_env

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
    app_dir = Path.join([repo, @apps_root, "#{runtime}_runner"])

    socket_dir = absolute(Application.get_env(:flezha, :socket_files_path, "app_data/sockets/"))

    vars = %{
      "artifact" => resolve_artifact(runtime, profile_key, app_dir, manifest),
      "app_dir" => app_dir,
      "repo" => repo,
      "namespace" => namespace,
      "socket_dir" => socket_dir,
      "socket_dir_rel" => Path.relative_to(socket_dir, app_dir, force: true),
      "ca_cert" => absolute(Application.get_env(:flezha, :ca_cert_path, "app_data/certs/ca.pem")),
      "client_cert" =>
        absolute(Application.get_env(:flezha, :client_cert_path, "app_data/certs/client.pem")),
      "client_key" =>
        absolute(Application.get_env(:flezha, :client_key_path, "app_data/certs/client.key"))
    }

    Regex.replace(~r/\{(\w+)\}/, manifest["command"][profile_key], fn whole, name ->
      Map.get(vars, name) ||
        raise ArgumentError,
              "#{manifest_path(runtime)}: unknown placeholder #{whole} in the #{profile} command"
    end)
  end

  defp resolve_artifact(runtime, profile, app_dir, manifest) do
    candidates = Enum.map(manifest["artifacts"][profile], &Path.expand(&1, app_dir))

    case Enum.find_index(candidates, &File.exists?/1) do
      nil ->
        raise "no #{profile} artifact for the #{runtime} test app (none of " <>
                "#{inspect(candidates)} exists); build it with `#{manifest["build"][profile]}`"

      0 ->
        hd(candidates)

      index ->
        Logger.warning(
          "the #{runtime} #{profile} runner is #{Enum.at(candidates, index)} because " <>
            "#{hd(candidates)} does not exist; build it with `#{manifest["build"][profile]}`"
        )

        Enum.at(candidates, index)
    end
  end

  defp absolute(path), do: Path.expand(path, File.cwd!())

  @impl true
  def init(_) do
    ports = initialize_ports()

    schedule_counter_reset()

    {:ok, %{ports: ports, response_refs: %{}, port_down_counter: 0}}
  end

  @impl true
  def handle_info(:reset_counter, state) do
    schedule_counter_reset()

    {:noreply, Map.put(state, :port_down_counter, 0)}
  end

  @impl true
  def handle_info({_port, {:data, port_msg}}, state) do
    Logger.warning("Received unexpected message from port: #{inspect(port_msg)}")

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :port, port, :normal}, state) do
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
              "Port application malfunction — too many crashes (#{counter}), backing off #{@backoff_ms}ms"
            )

            Process.send_after(self(), {:restart_port, command}, @backoff_ms)
            ports
          else
            {new_port, _} = open_port(command)
            Map.put(ports, new_port, command)
          end
      end

    {:noreply, %{state | ports: ports, port_down_counter: counter}}
  end

  @impl true
  def handle_info({:restart_port, command}, state) do
    Logger.info("Port application restarting after backoff")
    {port, _} = open_port(command)
    ports = Map.put(state.ports, port, command)
    {:noreply, %{state | ports: ports, port_down_counter: 0}}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Port manager received unexpected message: #{inspect(msg)}")

    {:noreply, state}
  end

  defp initialize_ports() do
    external_processes = Application.fetch_env!(:flezha, :external_processes)

    Enum.map(external_processes, fn command ->
      open_port(command)
    end)
    |> Map.new()
  end

  defp schedule_counter_reset() do
    Process.send_after(self(), :reset_counter, 30_000)
  end
end
