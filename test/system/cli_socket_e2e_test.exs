defmodule FlezhaTest.System.CliSocketE2ETest do
  @moduledoc """
  The built `flz` against this suite's live controller over its UDS control
  socket: text and JSON answers, a refusal's stream and code, a streaming verb
  ended by its window and by a signal, a killed listener, `controller status`
  and an MCP tool call, with no `flzvm` started for any of them. Then a request
  in a protocol the controller does not speak, a controller configured for
  QUIC that `flz` still reaches and stops, and `flz reset` against that
  controller while it runs and once it has stopped.

  Tagged `:system`; run with
  `mix test --include system test/system/cli_socket_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import FlezhaTest.Helper.ListenerHarness, only: [listeners: 2, start_bound!: 2]
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 3, os_pid: 1, signal: 2, stop_namespace: 1, unique_id_path: 1]

  alias Flezha.Observability.{Bus, Registry, Signal, TestSinks}
  alias Flezha.Service.Cli
  alias Flezha.Cli.Socket
  alias Flezha.Socket.Support
  alias FlezhaTest.Helper.CliCapture
  alias FlezhaTest.Helper.ControlFrameTestHelper, as: Frames
  alias FlezhaTest.Helper.NodeTestHelper

  # The socket, the listener and the reset probe are controller mechanics; the
  # reference runner's fixtures are what the listener needs.
  @moduletag reference_runtime: true
  @moduletag timeout: 300_000

  setup_all do
    {_, 0} = System.cmd("cargo", ["build", "-q", "-p", "flz"], stderr_to_stdout: true)
    dir = NodeTestHelper.claim_tmp_dir("flz_socket_e2e")
    flz = Path.join(dir, "flz")
    File.cp!(Path.expand("_build/.target/debug/flz"), flz)
    File.chmod!(flz, 0o755)

    # Records any start: no verb below may start one.
    flzvm = Path.join(dir, "flzvm")
    File.write!(flzvm, "#!/bin/sh\necho \"$@\" >> \"$(dirname \"$0\")/flzvm.starts\"\nexit 1\n")
    File.chmod!(flzvm, 0o755)

    namespace = "cli_socket_e2e_#{System.unique_integer([:positive])}"
    boot_runner(namespace, unique_id_path("cli_socket_e2e"), [])

    on_exit(fn ->
      stop_namespace(namespace)
      File.rm_rf!(dir)
    end)

    %{flz: flz, dir: dir, namespace: namespace}
  end

  # Every verb here reaches the controller without a VM of its own.
  setup ctx do
    on_exit(fn ->
      refute File.exists?(Path.join(ctx.dir, "flzvm.starts")), "a verb started flzvm"
    end)
  end

  describe "a verb against the live controller" do
    test "answers as it does in process, in text and in JSON", ctx do
      {{:ok, :ok}, text, ""} =
        CliCapture.run(fn -> Cli.call("Relay.list_namespaces", "[]", []) end)

      assert flz(ctx, ["namespace", "list"]) == {0, "Fetching namespaces...\n" <> text, ""}

      {{:ok, :ok}, json, ""} =
        CliCapture.run(fn -> Cli.call("Relay.list_namespaces", "[]", output: "json") end)

      assert flz(ctx, ["namespace", "list", "--json"]) == {0, json, ""}
    end

    test "refuses on stderr with its class's code", ctx do
      ghost = "ghost_#{System.unique_integer([:positive])}"
      {code, out, err} = flz(ctx, ["namespace", "delete", ghost])

      assert {code, out} == {5, ""}
      assert err =~ ~r/^error: .*#{ghost}.*\(namespace\.not_found\)$/m
      assert err =~ "hint: flz namespace list"
    end

    test "controller status answers", ctx do
      {0, out, ""} = flz(ctx, ["controller", "status"])
      assert out =~ "shutdown_in_progress:          false"
    end

    test "flz mcp answers a tool call from the controller", ctx do
      port = open(ctx, ["mcp"])

      call = %{
        jsonrpc: "2.0",
        id: 2,
        method: "tools/call",
        params: %{name: "relay_list", arguments: %{namespace: ctx.namespace}}
      }

      Port.command(port, Jason.encode!(call) <> "\n")
      reply = read_until(port, &String.contains?(&1, "\n"))
      Port.close(port)

      assert %{"id" => 2} = decoded = Jason.decode!(reply)
      refute inspect(decoded) =~ "controller.unreachable"
    end
  end

  describe "a streaming verb" do
    setup :tail_source

    test "prints each signal as it comes and exits 0 when its window closes", ctx do
      port = open(ctx, ["observe", "tail", to_string(ctx.src), "--duration-ms", "3000"])

      assert wait_until(fn -> TestSinks.subscribers(ctx.src) != [] end, 10_000),
             "the tail never subscribed"

      Bus.emit(Signal.new(%{kind: :event, source: ctx.src, name: :socket_probe}))

      assert read_until(port, &String.contains?(&1, "#{ctx.src}.socket_probe")) =~ "kind=event"
      assert_receive {^port, {:exit_status, 0}}, 10_000
    end

    test "ended by SIGINT leaves nothing serving it on the controller", ctx do
      port = open(ctx, ["observe", "tail", to_string(ctx.src), "--duration-ms", "60000"])

      assert wait_until(fn -> TestSinks.subscribers(ctx.src) != [] end, 10_000),
             "the tail never subscribed"

      signal(os_pid(port), "-INT")

      assert_receive {^port, {:exit_status, _}}, 10_000
      assert wait_until(fn -> TestSinks.subscribers(ctx.src) == [] end, 10_000)
      assert wait_until(fn -> requests() == [] end, 10_000), "a request is still served"
    end
  end

  describe "a killed flz relay listen" do
    test "has its listener plugin disabled", %{namespace: ns} = ctx do
      id = start_bound!(ns, "bench_echo")
      port = open(ctx, ["relay", "listen", id, "-n", ns, "--duration-ms", "120000"])
      assert wait_until(fn -> listeners(ns, id) != [] end, 30_000), "no listener attached"

      signal(os_pid(port), "-KILL")

      assert_receive {^port, {:exit_status, _}}, 10_000
      assert wait_until(fn -> listeners(ns, id) == [] end, 30_000), "the listener was left on"
      assert wait_until(fn -> requests() == [] end, 10_000), "a request is still served"
    end
  end

  describe "a request the controller does not run" do
    test "in a protocol it does not speak is refused naming both", _ctx do
      body = %{protocol: 2, entry: "cli", name: "Namespace.list", args: [], opts: %{}}
      frames = Frames.cli_request(body)

      assert {"cli_stderr", refusal} = List.keyfind(frames, "cli_stderr", 0)
      assert refusal =~ "speaks flz request protocol 1, and this request uses 2"
      assert List.keyfind(frames, "cli_exit", 0) == {"cli_exit", ~s({"code":2})}
      refute List.keymember?(frames, "cli_stdout", 0)
    end
  end

  describe "a controller configured for QUIC" do
    test "answers flz on its socket, stops for it, and flz reset reads it both ways", ctx do
      # The peer is linked to this process, and a stop ends it.
      Process.flag(:trap_exit, true)

      {peer, node} =
        NodeTestHelper.start_peer_node(:cli_quic,
          app_env: [transports: [:quic], quic_listen_address: "127.0.0.1:0"]
        )

      on_exit(fn -> NodeTestHelper.stop_peer_node(peer, node) end)

      sockets = :erpc.call(node, Support, :socket_dir, [])
      control = Path.join(sockets, "runner_control_channel.sock")
      assert File.exists?(control)

      assert Enum.sort(:erpc.call(node, Flezha.Transport.Server, :list_active_transports, [])) ==
               [:quic, :uds]

      {0, status, ""} = flz(ctx, ["controller", "status"], sockets)
      assert status =~ "node:                          #{node}"

      # A data root of the test's own: the probe reads the peer's socket, the
      # reset acts on this tree alone.
      root = Path.join(ctx.dir, "storage")
      kept = Path.join(root, "app/state/a.bin")
      File.mkdir_p!(Path.dirname(kept))
      File.write!(kept, "x")
      # A loaded machine can take longer than the default second to answer
      # the probe, which then reads the controller as down.
      reset = ["reset", "--storage-root", root, "--probe-timeout-ms", "15000"]

      {0, plan, ""} = flz(ctx, reset ++ ["--dry-run", "--json"], sockets)
      assert plan =~ ~s("dry_run":true)

      {6, _out, refused} = flz(ctx, reset ++ ["--yes"], sockets)
      assert refused =~ "(reset.controller_running)"
      assert File.exists?(kept)

      # The stop's outcome travels over the socket the stop is closing.
      {0, stopped, progress} = flz(ctx, ["controller", "stop"], sockets)
      assert stopped =~ "[shutdown] complete"
      refute stopped =~ "flz-controller-stop-result"
      assert progress =~ "Stopping flezha controller..."
      assert wait_until(fn -> not File.exists?(control) end, 30_000)

      {0, plan, ""} = flz(ctx, reset ++ ["--dry-run", "--json"], sockets)
      assert plan =~ ~s("dry_run":true)
      assert File.exists?(kept)

      {0, _out, _err} = flz(ctx, reset ++ ["--yes"], sockets)
      refute File.exists?(kept)
    end
  end

  # ---------- flz ----------

  # One run to its end: exit code, stdout, stderr.
  defp flz(ctx, args, sockets \\ Support.socket_dir()) do
    err = Path.join(ctx.dir, "stderr_#{System.unique_integer([:positive])}")

    {out, code} =
      System.cmd("sh", ["-c", ~s("$0" "$@" 2>"$FLZ_STDERR"), ctx.flz | args],
        env: [{"FLEZHA_SOCKET_FILES_PATH", sockets}, {"FLZ_STDERR", err}]
      )

    {code, out, File.read!(err)}
  end

  # A run that keeps going, its stdout delivered to the test.
  defp open(ctx, args) do
    Port.open({:spawn_executable, ctx.flz}, [
      :binary,
      :exit_status,
      args: args,
      env: [{~c"FLEZHA_SOCKET_FILES_PATH", to_charlist(Support.socket_dir())}]
    ])
  end

  defp read_until(port, done?, read \\ "") do
    if done?.(read) do
      read
    else
      receive do
        {^port, {:data, data}} -> read_until(port, done?, read <> data)
      after
        10_000 -> flunk("flz printed only #{inspect(read)}")
      end
    end
  end

  defp requests, do: Task.Supervisor.children(Socket.supervisor())

  # ---------- the tail's source ----------

  defp tail_source(_ctx) do
    src = :"cli_socket_src_#{System.unique_integer([:positive])}"
    :ok = Registry.register_source(%{name: src, kinds: [:event], flavour: :declarative})
    on_exit(fn -> Registry.unregister_source(src) end)
    TestSinks.claim_pubsub_sink()
    %{src: src}
  end
end
