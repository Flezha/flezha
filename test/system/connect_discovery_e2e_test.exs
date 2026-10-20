defmodule FlezhaTest.System.ConnectDiscoveryE2ETest do
  @moduledoc """
  How a runner finds its controller, driven end to end against a real spawned
  runner: the ladder with no configuration at all, the refusal when two
  controllers are listening and none was named, and a QUIC list whose first
  entry does not resolve.

  The ladder's rung-by-rung selection is a unit test (`flz_transport`); what
  these scenarios prove is that a runner process started with the variable
  blank actually walks it and meets the controller — the failure this change
  exists to remove, which no in-process test can reproduce.
  """

  use FlezhaTest.SystemCase, capability: :connect_discovery

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.PortPilot
  alias Flezha.Socket.Support
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 120_000

  # A fresh fleet per test: each scenario starts its runner under a different
  # socket-ladder environment, so the runner is the subject, not a fixture.
  setup do
    namespace = "discovery_#{System.unique_integer([:positive])}"
    on_exit(fn -> FleetManager.stop_namespace(namespace) end)
    {:ok, namespace: namespace}
  end

  describe "the socket ladder" do
    # The repository rung is relative to the runner's working directory. One of
    # this test's own keeps it from the checkout's `app_data/sockets`, which a
    # developer's controller or another run's decoy may hold live.
    setup do
      {:ok, cwd: tmp_dir("cwd")}
    end

    test "a runner started with no socket path finds the controller through the runtime dir",
         ctx do
      runtime_dir = tmp_dir("xdg")
      # `$XDG_RUNTIME_DIR/flezha` is rung 2; pointing it at the controller's own
      # directory is the zero-configuration case two ordinary processes hit.
      :ok = File.ln_s(Support.socket_dir(), Path.join(runtime_dir, "flezha"))

      id =
        NodeTestHelper.start_runner(ctx.namespace,
          env: [{"FLEZHA_SOCKET_FILES_PATH", ""}, {"XDG_RUNTIME_DIR", runtime_dir}],
          port_opts: [cd: ctx.cwd]
        )

      assert id in FleetManager.active_runners(ctx.namespace)
    end

    test "two live candidates are refused and both are named, not ranked", ctx do
      runtime_dir = tmp_dir("xdg")
      :ok = File.ln_s(Support.socket_dir(), Path.join(runtime_dir, "flezha"))

      # A second live control socket at the repository rung, so the runner sees
      # two controllers and no variable saying which is meant. That rung is
      # relative to the runner's own working directory, and a test app either
      # runs where it is started or `cd`s into its own directory, so both are
      # planted.
      decoys = Enum.map(repository_rungs(ctx.cwd), &listen_at/1)

      output =
        run_until(ctx.namespace, 15_000,
          env: [{"FLEZHA_SOCKET_FILES_PATH", ""}, {"XDG_RUNTIME_DIR", runtime_dir}],
          cwd: ctx.cwd
        )

      Enum.each(decoys, &:gen_tcp.close/1)

      assert FleetManager.active_runners(ctx.namespace) == [],
             "a runner that cannot tell two controllers apart must not pick one"

      # Every assertion dumps what the runner actually said: a refusal that
      # does not name its candidates is the failure being tested for.
      assert output =~ Path.expand(Support.socket_dir()),
             "the refusal names the first candidate, but said: #{output}"

      assert Enum.any?(named_rungs(ctx.cwd), &(output =~ &1)),
             "the refusal names the second candidate, but said: #{output}"

      assert output =~ "FLEZHA_SOCKET_FILES_PATH",
             "the refusal names the variable that resolves it, but said: #{output}"
    end
  end

  # A runtime whose manifest lists no QUIC transport simply has no scenario
  # here, the way every other transport-axis suite is written.
  if :quic in NodeTestHelper.transports() do
    describe "the QUIC candidate list" do
      @describetag transport: :quic

      test "an entry that does not resolve does not mask a healthy one", ctx do
        listen = Application.get_env(:flezha, :quic_listen_address, "0.0.0.0:4433")
        healthy = String.replace(listen, "0.0.0.0", "127.0.0.1")

        id =
          NodeTestHelper.start_runner(ctx.namespace,
            transport: :quic,
            env: [{"FLEZHA_QUIC_ADDRESSES", "no-such-controller.invalid:4433,#{healthy}"}]
          )

        assert id in FleetManager.active_runners(ctx.namespace)
      end
    end
  end

  # --- helpers ---

  # Where `app_data/sockets` lands for a runner started in `cwd` and for one
  # that `cd`s into its own app directory — the rung is relative to the
  # runner's working directory, and test apps differ.
  defp repository_rungs(cwd) do
    app_dir = Path.expand("test/apps/#{NodeTestHelper.spawn_runtime()}_runner")
    app_rung = Path.join(app_dir, "app_data/sockets")

    unless File.dir?(app_rung), do: on_exit(fn -> File.rm_rf(Path.join(app_dir, "app_data")) end)

    [Path.join(cwd, "app_data/sockets"), app_rung]
  end

  # The rungs as a runner may name them: a test app under `test/apps` is a
  # symlink, and a runner that reports its working directory resolved names
  # the rung under the directory the link points at.
  defp named_rungs(cwd) do
    link = Path.expand("test/apps/#{NodeTestHelper.spawn_runtime()}_runner")

    resolved =
      case File.read_link(link) do
        {:ok, target} -> [Path.join(Path.expand(target, Path.dirname(link)), "app_data/sockets")]
        {:error, _} -> []
      end

    repository_rungs(cwd) ++ resolved
  end

  # A live control socket in `dir`. Only the socket file is removed afterwards;
  # `repository_rungs/0` names directories the harness already owns.
  defp listen_at(dir) do
    File.mkdir_p!(dir)
    socket = Path.join(dir, "runner_control_channel.sock")
    File.rm(socket)
    {:ok, listener} = :gen_tcp.listen(0, [{:ifaddr, {:local, socket}}, :binary])
    on_exit(fn -> File.rm(socket) end)

    listener
  end

  defp tmp_dir(prefix) do
    dir = Path.join(System.tmp_dir!(), "flezha_#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    dir
  end

  # Spawn a runner that is expected NOT to connect, and return what it said.
  defp run_until(namespace, budget, opts) do
    command = PortPilot.launch_command(NodeTestHelper.spawn_runtime(), :test, namespace)

    {port, _command} =
      NodeTestHelper.open_runner_port(command,
        transport: :uds,
        env: Keyword.fetch!(opts, :env),
        port_opts: [:exit_status, cd: Keyword.fetch!(opts, :cwd)]
      )

    output = collect(port, System.monotonic_time(:millisecond) + budget, "")
    if Port.info(port), do: Port.close(port)
    output
  end

  defp collect(port, deadline, acc) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      acc
    else
      receive do
        {^port, {:data, data}} -> collect(port, deadline, acc <> data)
        {^port, {:exit_status, status}} -> acc <> "\n[runner exited with status #{status}]"
      after
        remaining -> acc
      end
    end
  end
end
