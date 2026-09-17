defmodule FlezhaTest.System.SdkAdminE2ETest do
  @moduledoc """
  let-an-application-administer-its-namespace §7: an application starts, feeds
  and stops its own operatives with no operator credential, under enforcement,
  against a live runner.

  Nothing here calls the controller directly. Every act is issued by the
  reference runner's own application code, which reaches the controller through
  the `AdminRequest` frame and prints the controller's answer; what the test
  reads is that stdout. `FLEZHA_ADMIN_SCRIPT` names the calls the drill makes.

  Tagged `:system` (boots a real runner subprocess). Run with
  `mix test --include system test/system/sdk_admin_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  alias Flezha.AuthZ
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.RoleStore
  alias Flezha.Observability.Registry, as: ObservabilityRegistry
  alias Flezha.Observability.TestSinks
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 300_000
  # Pinned to the reference runtime: what these scenarios prove is the
  # controller's door and the frame every SDK sends, and the drill that issues
  # the calls is the reference app's. Each SDK's own suite holds its wrapper to
  # the same verbs and the same argument shapes.
  @moduletag reference_runtime: true

  @ns "tenant-admin"
  @unbound_ns "tenant-unbound"

  setup_all do
    _ = AuthTestHelper.setup_auth_stores(prefix: "flz_sdk_admin_e2e", claim: true)
    assert AuthZ.enforcement_posture() == :enforced

    {:ok, _} = NamespaceStore.create(@ns, quotas: %{})
    {:ok, _} = NamespaceStore.create(@unbound_ns, quotas: %{})

    # The whole operator story: one command, once per namespace. Nothing is
    # bound for `@unbound_ns`, which is what makes its refusal the default.
    :ok = RoleStore.bind("runner@" <> @ns, @ns, :app)

    # Under enforcement a runner proves the namespace it names before anything
    # else judges it, so the drill carries a credential for each.
    {bound_secret, _hash} = AuthTestHelper.issue_runner_credential(@ns)
    {unbound_secret, _hash} = AuthTestHelper.issue_runner_credential(@unbound_ns)

    on_exit(fn ->
      for ns <- [@ns, @unbound_ns] do
        try do
          OperativeManager.stop_all_operatives(ns)
          FleetManager.stop_namespace(ns)
        catch
          _kind, _reason -> :ok
        end
      end
    end)

    %{secrets: %{@ns => bound_secret, @unbound_ns => unbound_secret}}
  end

  describe "an application administers the namespace its connection proved" do
    test "it starts an operative, sees it running, feeds it and stops it", %{secrets: secrets} do
      script = [
        call("Operative.start", [%{factory_id: "do_nothing"}]),
        call("Operative.start", [%{id: "app_started", factory_id: "do_nothing"}]),
        call("Operative.start", [%{id: "app_started", factory_id: "do_nothing"}]),
        call("Operative.list_operatives", []),
        call("Operative.send", ["app_started", "from-the-application", ""]),
        call("Operative.stop", ["app_started"])
      ]

      {port, _runner_id} = drill(@ns, script, secrets)

      answers = answers(port, 6)

      assert [unnamed, named, again, list, send, stop] = answers
      assert {"Operative.start", "ok", unnamed} = unnamed
      assert %{"id" => generated, "outcome" => "started"} = Jason.decode!(unnamed)
      assert generated =~ ~r/\Ado_nothing-[a-z2-7]{16}\z/

      assert {"Operative.start", "ok", named} = named
      assert Jason.decode!(named) == %{"id" => "app_started", "outcome" => "started"}
      assert {"Operative.start", "ok", again} = again
      assert Jason.decode!(again) == %{"id" => "app_started", "outcome" => "already_running"}

      assert {"Operative.list_operatives", "ok", running} = list
      assert "app_started" in Jason.decode!(running)
      assert generated in Jason.decode!(running)
      assert {"Operative.send", "ok", _delivered} = send
      assert {"Operative.stop", "ok", _stopped} = stop

      # The operator's own surface agrees: the operative the application
      # started was the cluster's, and stopping it took it away.
      assert {:ok, ids} = OperativeManager.get_operative_ids(@ns)
      assert "app_started" not in ids
    end

    test "a start is recorded naming the requester, and a read is not", %{secrets: secrets} do
      watch_operative_records()

      {port, _runner_id} =
        drill(
          @ns,
          [
            call("Operative.start", [%{id: "app_read_target", factory_id: "do_nothing"}]),
            call("Operative.entity_state", ["app_read_target", "default"]),
            call("PluginManager.list", [])
          ],
          secrets
        )

      assert [_start, state, plugins] = answers(port, 3)
      assert {"Operative.entity_state", verdict, _} = state
      assert verdict in ["ok", "err"]
      assert {"PluginManager.list", "ok", _} = plugins

      # The start's record names the connection, not a token, and is the shape
      # an operator-issued start writes.
      record = TestSinks.receive_signal(&(&1.name == :start), 5_000)
      assert record, "the SDK-issued start wrote no audit record"
      assert record.metadata.action == :"operative.start"
      assert record.metadata.namespace == @ns
      assert record.metadata.principal == "runner@" <> @ns
      assert record.metadata.caller_source == :sdk
      assert record.metadata.operative == "app_read_target"

      # And the two reads wrote nothing: their rows declare no audit.
      refute TestSinks.receive_signal(fn _signal -> true end, 500)
    end

    test "a started operative outlives the runner that asked for it", %{secrets: secrets} do
      {port, runner_id} =
        drill(
          @ns,
          [call("Operative.start", [%{id: "app_outlives", factory_id: "do_nothing"}])],
          secrets
        )

      assert [{"Operative.start", "ok", _}] = answers(port, 1)
      assert {:ok, running} = OperativeManager.get_operative_ids(@ns)
      assert "app_outlives" in running

      NodeTestHelper.stop_runner(@ns, runner_id)

      # Owned by the cluster, not by the connection: an operator still sees it.
      assert {:ok, after_disconnect} = OperativeManager.get_operative_ids(@ns)
      assert "app_outlives" in after_disconnect
    end

    test "it enables a plugin on its own operative and disables it again", %{secrets: secrets} do
      plugin = %{
        id: "app_sink",
        provider: "operative_stdout",
        operative_id: "app_plugin_target",
        hooks: [["on_output", %{}]]
      }

      {port, _runner_id} =
        drill(
          @ns,
          [
            call("Operative.start", [%{id: "app_plugin_target", factory_id: "do_nothing"}]),
            call("PluginManager.enable", ["app_sink", plugin]),
            call("PluginManager.list", []),
            call("PluginManager.disable", ["app_sink"])
          ],
          secrets
        )

      assert [_start, enable, list, disable] = answers(port, 4)
      assert {"PluginManager.enable", "ok", _enabled} = enable
      assert {"PluginManager.list", "ok", plugins} = list
      assert plugins =~ "app_sink"
      assert {"PluginManager.disable", "ok", _disabled} = disable
    end
  end

  describe "the authority is granted, and revoked, by an operator" do
    test "a namespace nobody bound refuses a start and names the missing grant", %{
      secrets: secrets
    } do
      {port, _runner_id} =
        drill(
          @unbound_ns,
          [
            call("Operative.start", [%{id: "unbound_start", factory_id: "do_nothing"}])
          ],
          secrets
        )

      assert [{"Operative.start", "err", reason}] = answers(port, 1)
      assert reason =~ "role_insufficient"
      assert {:ok, ids} = OperativeManager.get_operative_ids(@unbound_ns)
      assert "unbound_start" not in ids
    end

    test "an over-quota namespace refuses the application exactly as it refuses an operator", %{
      secrets: secrets
    } do
      {:ok, _} = NamespaceStore.set_quotas(@ns, %{max_operatives: 0})
      on_exit(fn -> {:ok, _} = NamespaceStore.set_quotas(@ns, %{}) end)

      {port, _runner_id} =
        drill(
          @ns,
          [call("Operative.start", [%{id: "app_over_quota", factory_id: "do_nothing"}])],
          secrets
        )

      assert [{"Operative.start", "err", reason}] = answers(port, 1)
      assert reason =~ "quota_exceeded"
      assert reason =~ "max_operatives"
    end

    test "unbinding mid-session refuses the next call, with no reconnect", %{secrets: secrets} do
      {port, _runner_id} =
        drill(
          @ns,
          [
            call("Operative.list_operatives", []),
            Map.put(call("Operative.list_operatives", []), :after_ms, 3_000)
          ],
          secrets
        )

      # The first call proves the binding is live; then it is taken away while
      # the connection stays open.
      assert [{"Operative.list_operatives", "ok", _}] = answers(port, 1)
      :ok = RoleStore.unbind("runner@" <> @ns)
      on_exit(fn -> :ok = RoleStore.bind("runner@" <> @ns, @ns, :app) end)

      assert [_first, {"Operative.list_operatives", "err", reason}] = answers(port, 2)
      assert reason =~ "role_insufficient"
    end
  end

  # ---------- the drill ----------

  defp call(verb, args), do: %{verb: verb, args: args}

  # Every record the operative service writes, forwarded to this test process.
  # The audit assertion needs the record, not a count, so the sink is the one
  # that hands signals back rather than the ring the operator's tooling reads.
  defp watch_operative_records do
    sink = :"admin_records_#{:erlang.unique_integer([:positive])}"

    {:ok, _pid} =
      ObservabilityRegistry.register_sink(%{
        name: sink,
        module: TestSinks.Counting,
        config: %{
          counter_ref: :counters.new(1, [:atomics]),
          parent: self(),
          sources: [:operative_service, :plugin_service]
        }
      })

    on_exit(fn -> ObservabilityRegistry.unregister_sink(sink) end)
  end

  # A runner whose application code makes the calls, and its port, which is
  # what carries the answers back.
  defp drill(namespace, script, secrets) do
    {port, runner_id} =
      NodeTestHelper.start_runner_port(namespace,
        env: [
          {"FLEZHA_ADMIN_SCRIPT", Jason.encode!(script)},
          {"FLEZHA_NAMESPACE_SECRET", Map.fetch!(secrets, namespace)}
        ]
      )

    on_exit(fn ->
      try do
        NodeTestHelper.stop_runner(namespace, runner_id)
      catch
        _kind, _reason -> :ok
      end
    end)

    {port, runner_id}
  end

  # `admin <verb> ok <json>` / `admin <verb> err <text>`, one line per call, in
  # call order. Every other line the runner prints is ignored.
  # What the port has said so far is remembered per port, so a scenario that
  # reads one answer, changes something, and reads the next still sees both.
  defp answers(port, count, budget \\ 60_000) do
    seen =
      NodeTestHelper.read_port(
        port,
        &(length(parse_answers(&1)) >= count),
        budget,
        Process.get({:admin_seen, port}, "")
      )

    Process.put({:admin_seen, port}, seen)
    read = parse_answers(seen)

    assert length(read) >= count,
           "the runner printed fewer than #{count} admin answers:\n#{seen}"

    Enum.take(read, count)
  end

  defp parse_answers(seen) do
    seen
    |> String.split("\n", trim: true)
    |> Enum.flat_map(&parse_answer/1)
  end

  defp parse_answer(line) do
    case String.split(String.trim(line), " ", parts: 4) do
      ["admin", verb, verdict, answer] -> [{verb, verdict, answer}]
      ["admin", verb, verdict] -> [{verb, verdict, ""}]
      _other -> []
    end
  end
end
