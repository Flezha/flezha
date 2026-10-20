defmodule FlezhaTest.System.BootReadinessDrillTest do
  @moduledoc """
  Three surfaces answer "may this node serve?" and they answer together.

  The drill walks a booting controller through every verdict its precondition
  table can hold — `:booting`, then `:recovering` for a replaying hub, then
  `:recovering` for a durable registry that has not been reloaded, then
  `:ready` — and at each step samples all three surfaces on ONE clock before
  anything else moves:

  | surface | how it is driven |
  |---|---|
  | `GET /readyz` | the bound listener, over a real TCP socket |
  | `flz boot await --timeout 0` | the verb the CLI's RPC dispatches to |
  | the image `HEALTHCHECK` | the `CMD` argv read out of `docker/prod/Dockerfile.controller` |

  The third column is the point of reading the Dockerfile rather than copying
  its string: a `HEALTHCHECK` edited to key on something other than the verdict
  fails here. The same command run through the real `flz` binary over a control
  socket is `FlezhaTest.System.PluginPackageReleaseTest`, which uses it as its
  own readiness probe.

  Every sample asserts not-ready *and* names the precondition holding it, so a
  surface that refuses for the wrong reason is a failure too.
  """

  use FlezhaTest.SystemCase, capability: :state_persistence

  import ExUnit.CaptureIO

  alias Flezha.Boot.Readiness
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Snapshot
  alias Flezha.Healthz.Server
  alias Flezha.Service.{Cli, HubManager}
  alias Flezha.Service.HubManager.Starter
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @dockerfile "docker/prod/Dockerfile.controller"
  @ready_within_ms 10_000

  setup do
    on_exit(TestHelpers.override_env(:healthz_port, 0))

    Snapshot.reset_recovery_states()
    Readiness.reset()

    {:ok, _pid} = start_supervised(Server)
    {:ok, port} = Server.listen_port()

    on_exit(fn ->
      Snapshot.reset_recovery_states()
      Readiness.publish_sequence_complete()
    end)

    {:ok, port: port}
  end

  test "no surface reports ready before the last precondition is met", %{port: port} do
    # 1. The boot sequence has not finished. This is the window the readiness
    #    probe used to read `200 healthy` through `/healthz`.
    assert_all_refuse(port, "booting", "boot_sequence_complete")

    # 2. The sequence is done and a hub is inside ProcessHub's recovery state
    #    machine. Liveness must stay 200 here; readiness must not.
    Readiness.publish_sequence_complete()
    hub_id = :"drill_hub_#{System.unique_integer([:positive])}"
    Snapshot.observe_recovery(hub_id, :recovery_pending)

    assert_all_refuse(port, "recovering", "hubs_recovered")
    assert {200, health} = healthz(port)
    assert health["status"] == "recovering"

    # 3. The bounded boot wait elapsing is not readiness either: a `:timeout`
    #    hub has not recovered, and the row is live, so nothing restarts.
    Snapshot.observe_recovery(hub_id, :timeout)
    assert_all_refuse(port, "recovering", "hubs_recovered")

    # 4. Hubs are back, but a durable registry on disk has not been reloaded:
    #    a hub this boot never started left one, and auto-restore is on.
    Snapshot.observe_recovery(hub_id, :normal)
    %{tmp: tmp} = FlezhaTest.Case.install([:tmp_dir])
    on_exit(TestHelpers.override_env(:auto_restore_enabled, true))
    on_exit(TestHelpers.override_env(:process_hub_registry_root, tmp))
    registry = HubRegistryTestHelper.registry_file("drill-unloaded")
    File.mkdir_p!(Path.dirname(registry))
    File.write!(registry, "")

    assert_all_refuse(port, "recovering", "durable_registry_loaded")
  end

  test "every surface flips to ready at the same sample, and not before", %{port: port} do
    Readiness.publish_sequence_complete()
    hub_id = :"drill_flip_#{System.unique_integer([:positive])}"
    Snapshot.observe_recovery(hub_id, :recovering)

    refute ready_verdict?(port) |> elem(0)

    # The one change: the hub reaches `:normal`. No restart, no re-boot.
    Snapshot.observe_recovery(hub_id, :normal)

    {readyz?, verb?, healthcheck?} = ready_verdict?(port)

    assert readyz?, "GET /readyz did not report ready"
    assert verb?, "flz boot await --timeout 0 did not report ready"
    assert healthcheck?, "the image HEALTHCHECK did not report ready"
  end

  # Real hub, shipped recovery config: a node with nobody to wait for must not
  # sit at `recovering` for the whole grace. The registry root is claimed, not
  # ExUnit's `:tmp_dir`: that path is the same in every BEAM on the checkout, so
  # a second suite running this test would add its hub's registry to ours.
  test "a controller with no peers is ready in seconds, hub recovery met last" do
    assert Node.list() == [], "this boot needs a node with no peers: #{inspect(Node.list())}"

    %{tmp: tmp} = FlezhaTest.Case.install([:tmp_dir])
    on_exit(TestHelpers.override_env(:auto_restore_enabled, true))
    on_exit(TestHelpers.override_env(:process_hub_registry_root, tmp))

    # A grace under the deadline would pass this test on the timer alone.
    assert Durations.get(:process_hub_reconcile_grace_ms) > @ready_within_ms

    HubRegistryTestHelper.stop_new_hubs_on_exit()

    # The previous boot, leaving its registry on disk the way a restart finds it.
    namespace = "drill-no-peers-#{System.unique_integer([:positive])}"
    {:ok, _pid} = HubManager.start_hub(namespace)
    HubRegistryTestHelper.stop_hub_keeping_registry(namespace)
    assert File.regular?(HubRegistryTestHelper.registry_file(namespace))

    # This boot: the owner that creates the declared set's hubs, then the end
    # of the sequence.
    started = System.monotonic_time(:millisecond)
    {:ok, _pid} = start_supervised(Starter)
    Readiness.publish_sequence_complete()

    ready? = NodeTestHelper.wait_until(&ready_or_record_unmet/0, @ready_within_ms)
    elapsed = System.monotonic_time(:millisecond) - started

    assert ready?, "not ready #{elapsed} ms after boot, unmet: #{inspect(Process.get(:unmet))}"
    assert Process.get(:unmet) == [:hubs_recovered]
    assert namespace in HubManager.get_running_namespaces()
  end

  test "the shipped HEALTHCHECK evaluates the readiness verdict once" do
    argv = healthcheck_argv()

    assert ["boot", "await" | rest] = argv,
           "the image HEALTHCHECK no longer runs a `flz boot` verb: #{inspect(argv)}"

    assert "0" in rest,
           "the image HEALTHCHECK must evaluate once, not block: #{inspect(argv)}"
  end

  # ---------- the three surfaces, sampled together ----------

  defp assert_all_refuse(port, verdict, unmet) do
    {status, body} = readyz(port)

    assert status == 503, "GET /readyz answered #{status} while #{unmet} is unmet"
    assert body["status"] == verdict
    assert unmet in body["unmet"]

    for {name, run} <- [
          {"flz boot await --timeout 0", &verb_await/0},
          {"HEALTHCHECK", &healthcheck/0}
        ] do
      assert {{:shutdown, 7}, out} = run.(), "#{name} reported ready while #{unmet} is unmet"
      assert out =~ verdict
      assert out =~ unmet
    end
  end

  defp ready_or_record_unmet() do
    case Readiness.verdict() do
      {:ready, _rows} ->
        true

      {_verdict, rows} ->
        Process.put(:unmet, for(%{key: key, met: false} <- rows, do: key))
        false
    end
  end

  defp ready_verdict?(port) do
    {status, _body} = readyz(port)

    {status == 200, match?({:ok, _}, verb_await()), match?({:ok, _}, healthcheck())}
  end

  # The image's own command, read from the file that ships it.
  defp healthcheck() do
    ["boot", "await", "--timeout", secs] = healthcheck_argv()

    verb_await(String.to_integer(secs) * 1000)
  end

  defp healthcheck_argv() do
    File.read!(@dockerfile)
    |> String.split("\n")
    |> Enum.find_value(fn line ->
      case Regex.run(~r/^\s*CMD\s+(\S*flz)\s+(.*?)\s*\|\|/, line) do
        [_, _bin, args] -> String.split(args)
        nil -> nil
      end
    end) ||
      flunk(
        "#{@dockerfile} declares no `CMD <path>/flz <args> ||` HEALTHCHECK. " <>
          "The image is no longer keyed on the readiness verdict."
      )
  end

  defp verb_await(timeout_ms \\ 0) do
    stderr =
      capture_io(:stderr, fn ->
        stdout =
          capture_io(fn ->
            outcome =
              try do
                Cli.call("Controller.boot_await", Jason.encode!([timeout_ms]), [])
                :ok
              catch
                :exit, reason -> reason
              end

            Process.put(:drill_outcome, outcome)
          end)

        Process.put(:drill_stdout, stdout)
      end)

    case Process.get(:drill_outcome) do
      :ok -> {:ok, Process.get(:drill_stdout)}
      reason -> {reason, stderr}
    end
  end

  defp readyz(port), do: get_json(port, "/readyz")
  defp healthz(port), do: get_json(port, "/healthz")

  defp get_json(port, path) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 1_000)

    :ok =
      :gen_tcp.send(sock, "GET #{path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")

    raw = read_until_close(sock)
    :gen_tcp.close(sock)

    [head, body] = String.split(raw, "\r\n\r\n", parts: 2)
    [_, code | _] = String.split(head, " ")

    {String.to_integer(code), Jason.decode!(body)}
  end

  defp read_until_close(sock, acc \\ <<>>) do
    case :gen_tcp.recv(sock, 0, 2_000) do
      {:ok, chunk} -> read_until_close(sock, acc <> chunk)
      _ -> acc
    end
  end
end
