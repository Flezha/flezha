defmodule Flezha.Service.AdminTest do
  @moduledoc """
  let-an-application-administer-its-namespace: the door application code
  reaches the published consumer surface through.

  The reproduction the change opened with is the first describe: a connected
  runner's identity is refused every operator action, because the connect path
  stamped its role rather than resolving one.
  """
  # async: false: writes Application env and a shared on-disk auth root.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_sdk_admin", claim: true, register_source: true}],
    async: false

  alias Flezha.AuthZ
  alias Flezha.AuthZ.Policy
  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.Scope
  alias Flezha.AuthZ.Verbs
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Observability.TestSinks
  alias Flezha.Service.Admin
  alias Flezha.Runner.FleetManager.Session

  @ns "team-orders"
  @other "team-payments"
  @principal "runner@team-orders"

  defp bind_app!, do: :ok = RoleStore.bind(@principal, @ns, :app)

  describe "the refusal this change starts from" do
    test "a connected runner's identity holds runner.connect and no operator action" do
      context = Session.runner_caller_context(@ns)
      assert context.role == :runner

      assert :ok = AuthZ.authorize(context, :"runner.connect", Scope.build(@ns))

      for action <- [
            :"operative.start",
            :"operative.stop",
            :"operative.send",
            :"plugin.enable",
            :"plugin.disable"
          ] do
        assert {:error, {:forbidden, :role_insufficient}} =
                 AuthZ.authorize(context, action, Scope.build(@ns))
      end
    end

    test "the door refuses a start and the reason names the missing grant" do
      assert {:error, reason} = Admin.call(@ns, "Operative.start", ~s([{"id":"o1"}]))
      assert reason == "forbidden: role_insufficient"
    end
  end

  describe "the role is resolved rather than stamped" do
    test "an unbound principal stays runner, so nothing existing gains authority" do
      assert RoleStore.lookup(@principal) == :error
      assert Session.runner_caller_context(@ns).role == :runner
    end

    test "a binding takes effect on the next call, with no reconnect" do
      assert Session.runner_caller_context(@ns).role == :runner
      bind_app!()
      assert Session.runner_caller_context(@ns).role == :app
    end

    test "unbinding takes effect on the next call, with no reconnect" do
      bind_app!()
      assert Session.runner_caller_context(@ns).role == :app

      :ok = RoleStore.unbind(@principal)

      assert Session.runner_caller_context(@ns).role == :runner

      assert {:error, "forbidden: role_insufficient"} =
               Admin.call(@ns, "Operative.stop", ~s(["o1"]))
    end

    test "a binding made for another namespace grants nothing here" do
      :ok = RoleStore.bind(@principal, @other, :app)
      assert Session.runner_caller_context(@ns).role == :runner
    end

    test "the namespace is the proven one whatever the store's binding says" do
      :ok = RoleStore.bind(@principal, @ns, :app)
      assert Session.runner_caller_context(@ns).namespace == @ns
      assert Session.runner_caller_context(@other).namespace == @other
    end
  end

  describe "the request names no namespace and no principal" do
    setup do
      bind_app!()
      :ok
    end

    test "the proven namespace is inserted where the row declares it, so there is no slot" do
      # `list_operatives` takes (namespace, context). The wire array holds the
      # arguments after the namespace, so naming one is an argument too many.
      assert {:ok, _json} = Admin.call(@ns, "Operative.list_operatives", "[]")

      assert {:error, reason} =
               Admin.call(@ns, "Operative.list_operatives", ~s(["#{@other}"]))

      assert reason =~ "takes no argument list of this length"
    end

    test "a row-scoped verb's namespace field is stamped, not read from the caller" do
      # `plugin_enable` is judged in the namespace its own fields name, so the
      # slot is a named one rather than a position. A caller writing another
      # namespace there is judged in the proven one anyway, which the record
      # the act writes is the proof of.
      boundary = System.monotonic_time(:microsecond)

      _ =
        Admin.call(
          @ns,
          "PluginManager.enable",
          ~s(["p1", {"id": "p1", "provider": "Elixir.Flezha.Plugin.Providers.Kafka.Producer",
                     "operative_id": "o1", "namespace": "#{@other}"}])
        )

      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:plugin_service, boundary)
      assert record.metadata.namespace == @ns
    end

    test "the frame has no namespace field and no principal field" do
      names =
        %RunnerCommands.AdminRequest{}
        |> Map.from_struct()
        |> Map.keys()
        |> Enum.reject(&(&1 == :__uf__))
        |> MapSet.new()

      assert names == MapSet.new([:verb, :args])
    end
  end

  describe "reachability follows the grant, not a list" do
    setup do
      bind_app!()
      :ok
    end

    test "a read the app role grants is authorized" do
      assert {:ok, json} = Admin.call(@ns, "Operative.list_operatives", "[]")
      assert {:ok, _list} = Jason.decode(json)
    end

    test "an operator-only verb is refused at the policy point" do
      assert {:error, "forbidden: role_insufficient"} =
               Admin.call(@ns, "AuditLog.rotate", "[]")
    end

    test "an auth-state verb is refused by grant" do
      assert {:error, "forbidden: role_insufficient"} =
               Admin.call(@ns, "Namespace.create", ~s([{}]))
    end

    test "nothing binds plugin.install, plugin.upgrade, plugin.uninstall or controller.stop on the surface" do
      # The node-wide verbs are out of reach because no published function
      # carries them at all, and each is `scope: :system` besides.
      for action <- [
            :"plugin.install",
            :"plugin.upgrade",
            :"plugin.uninstall",
            :"controller.stop"
          ] do
        refute Policy.grants?(:app, action)

        rows = Enum.filter(Verbs.all(), &(&1.action == action))
        assert rows != []
        assert Enum.all?(rows, &(&1.scope == :system))
      end
    end

    test "the terminal's own wire contract is not a door, though it lives under the prefix" do
      assert {:error, reason} =
               Admin.call(@ns, "CliManager.operative_stop", ~s(["o1"]))

      assert reason =~ "no published function is named"
    end

    test "a name nothing declares is refused without minting an atom" do
      before = :erlang.system_info(:atom_count)

      assert {:error, _} = Admin.call(@ns, "Operative.no_such_function", "[]")
      assert {:error, _} = Admin.call(@ns, "NoSuchModule.anything", "[]")
      assert {:error, _} = Admin.call(@ns, "malformed", "[]")

      assert :erlang.system_info(:atom_count) == before
    end
  end

  describe "the act is recorded like any other" do
    setup do
      bind_app!()
      :ok
    end

    test "an SDK-issued start writes the row's audit record, naming the requester" do
      boundary = System.monotonic_time(:microsecond)

      # The start itself fails (no runner advertises the factory); the record is
      # written either way, because an authorized act that failed is still an act.
      _ = Admin.call(@ns, "Operative.start", ~s([{"id":"o1","factory_id":"f1"}]))
      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:operative_service, boundary)
      assert record.name == :start
      assert record.metadata.action == :"operative.start"
      assert record.metadata.principal == @principal
      assert record.metadata.namespace == @ns
      assert record.metadata.caller_source == :sdk
      assert record.metadata.operative == "o1"
    end

    test "a read writes no audit record, matching its row's declaration" do
      boundary = System.monotonic_time(:microsecond)

      assert {:ok, _json} = Admin.call(@ns, "Operative.list_operatives", "[]")
      TestSinks.settle(RingBufferSink)

      assert RingBufferSink.query(:operative_service, boundary) == []
    end
  end
end
