defmodule FlezhaTest.System.ControlPlaneWedgeE2ETest do
  @moduledoc """
  A worker aborted mid-dispatch recovers without losing its entity: the OTHER
  way a runner stops doing work, and a durability assertion rather than a
  remedy. The remedy path a wedged runner takes — verdict, terminal restart,
  eviction, rebind — is a row of `test/system/remedy_drill_test.exs`.

  Tagged `:system` (boots real subprocesses); run with
  `mix test --include system test/system/control_plane_wedge_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :liveness

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 3, peek_outputs: 0, produces?: 5, unique_id_path: 1]

  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager

  @factory "wedge_probe"
  @heartbeat_ms 300

  # A fresh fleet per test: every drill evicts or terminates its runner.
  setup do
    namespace = "wedge_e2e_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    %{namespace: namespace}
  end

  # A worker killed mid-dispatch is the OTHER way a runner stops doing work:
  # not a pool that stalls, but one activation whose host died holding it.
  # Per-entity exclusivity is the handout, so without the host answering it —
  # abandoning, replacing the worker, recreating the Entity, whatever that
  # runtime's answer is — the Entity would be retired for the life of the
  # process and nothing would ever say so.
  @tag capability: :pool_wedge, skip: FlezhaTest.SystemCase.deferral(:pool_wedge)
  @tag timeout: 180_000
  test "a worker aborted mid-dispatch recovers without losing its entity",
       %{namespace: namespace} do
    {operative_id, _port, _runner} = probe_on_its_own_runner(namespace, "aborted")

    assert produces?(operative_id, namespace, "one", "ok:one", 60_000),
           "the operative never produced before the abort"

    # The fixture ends this activation as violently as its host allows.
    OperativeManager.send_io(operative_id, namespace, "abort", %{"input_type" => "2"})

    # No operator action, no rebind: a pool that did not come back to size, or
    # an entity left holding an unanswered handout, would never serve this.
    assert produces?(operative_id, namespace, "two", "ok:two", 120_000),
           "the entity was never served again after its worker was aborted"

    # And the aborting item never ran twice: it produced at most once, whether
    # its host re-handed it (abandon / retry ladder) or lost it with the worker.
    aborted = Enum.count(peek_outputs(), fn {msg, _md} -> msg == "ok:abort" end)

    assert aborted <= 1,
           "the aborting item produced #{aborted} outputs — a re-hand must not double-execute"
  end

  # ---------- helpers ----------

  # A fresh runner and a `wedge_probe` operative bound to it, with the outputs
  # forwarded to the calling test. Every drill starts here: the interesting
  # part of each is what happens to that pair afterwards.
  defp probe_on_its_own_runner(namespace, label, opts \\ []) do
    operative_id = "#{label}_#{System.unique_integer([:positive])}"

    {port, runner_id} =
      boot_runner(namespace, unique_id_path("wedge_e2e"), [env: heartbeat_env()] ++ opts)

    assert {:ok, %{runner_id: ^runner_id}} =
             OperativeManager.start_operative(namespace, builder(operative_id, self()))

    {operative_id, port, runner_id}
  end

  defp builder(operative_id, recv_pid) do
    sink = %Flezha.Plugin{
      id: "wedge_sink",
      namespace: nil,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: recv_pid, include_metadata: true},
      hooks: [{:on_output, []}]
    }

    %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{},
      opts: [plugins: [sink]]
    }
  end

  defp heartbeat_env(), do: [{"FLEZHA_DATA_HEARTBEAT_MS", Integer.to_string(@heartbeat_ms)}]
end
