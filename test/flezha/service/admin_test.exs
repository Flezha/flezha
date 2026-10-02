defmodule Flezha.Service.AdminTest do
  @moduledoc """
  let-an-application-administer-its-namespace: the door application code
  reaches the published consumer surface through.

  The reproduction the change opened with is the first describe: a connected
  runner's identity is refused every user action, because the connect path
  stamped its role rather than resolving one.
  """
  # async: false: writes Application env and a shared on-disk auth root.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_sdk_admin", claim: true, register_source: true}],
    async: false

  use Mimic

  alias Flezha.AuthZ
  alias Flezha.AuthZ.Policy
  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.Scope
  alias Flezha.AuthZ.Verb
  alias Flezha.AuthZ.Verbs
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Observability.TestSinks
  alias Flezha.Service.Admin
  alias Flezha.Runner.FleetManager.Session

  @ns "team-orders"
  @other "team-payments"
  @principal "runner@team-orders"

  defp bind_app!, do: :ok = RoleStore.bind(@principal, @ns, :app)

  # Each verb's own smallest well-formed argument list: the calls below are
  # asserted on their refusal, never on their result.
  defp args_for("Operator.start"), do: ~s([{"id":"o1"}])
  defp args_for("Operator.send"), do: ~s(["o1", "hello", "in"])
  defp args_for("Operator.stop"), do: ~s(["o1"])
  defp args_for("PluginManager.enable"), do: ~s(["p1", {"provider": "operator_stdout"}])
  defp args_for("PluginManager.disable"), do: ~s(["p1", {}])

  describe "the refusal this change starts from" do
    test "a connected runner's identity holds runner.connect and no user action" do
      context = Session.runner_caller_context(@ns)
      assert context.roles == [:runner]

      assert :ok = AuthZ.authorize(context, :"runner.connect", Scope.build(@ns))

      for action <- [
            :"operator.start",
            :"operator.stop",
            :"operator.send",
            :"plugin.enable",
            :"plugin.disable"
          ] do
        assert {:error, {:forbidden, :role_insufficient}} =
                 AuthZ.authorize(context, action, Scope.build(@ns))
      end
    end

    test "the door refuses a start and the error names the missing grant" do
      assert {:error, %Flezha.Error{code: "authz.role_insufficient", class: :denied}} =
               Admin.call(@ns, "Operator.start", ~s([{"id":"o1"}]))
    end
  end

  describe "the role is resolved rather than stamped" do
    test "an unbound principal stays runner, so nothing existing gains authority" do
      assert RoleStore.lookup(@principal) == :error
      assert Session.runner_caller_context(@ns).roles == [:runner]
    end

    test "a binding takes effect on the next call, with no reconnect" do
      assert Session.runner_caller_context(@ns).roles == [:runner]
      bind_app!()
      assert Session.runner_caller_context(@ns).roles == [:app]
    end

    test "unbinding takes effect on the next call, with no reconnect" do
      bind_app!()
      assert Session.runner_caller_context(@ns).roles == [:app]

      :ok = RoleStore.unbind(@principal)

      assert Session.runner_caller_context(@ns).roles == [:runner]

      assert {:error, %Flezha.Error{code: "authz.role_insufficient"}} =
               Admin.call(@ns, "Operator.stop", ~s(["o1"]))
    end

    test "a binding made for another namespace grants nothing here" do
      :ok = RoleStore.bind(@principal, @other, :app)
      assert Session.runner_caller_context(@ns).roles == [:runner]
    end

    test "the namespace is the proven one whatever the store's binding says" do
      :ok = RoleStore.bind(@principal, @ns, :app)
      assert Session.runner_caller_context(@ns).namespace == @ns
      assert Session.runner_caller_context(@other).namespace == @other
    end
  end

  describe "the roles a connection holds combine" do
    test "spawner and stopper together start, send and stop, but enable no plugin" do
      :ok = RoleStore.bind(@principal, @ns, :spawner)
      :ok = RoleStore.bind(@principal, @ns, :stopper)

      assert Session.runner_caller_context(@ns).roles == [:spawner, :stopper]

      for verb <- ["Operator.start", "Operator.send", "Operator.stop"] do
        refute denied?(Admin.call(@ns, verb, args_for(verb))), verb
      end

      assert denied?(Admin.call(@ns, "PluginManager.enable", args_for("PluginManager.enable")))
    end

    test "a stopper alone stops, and nothing else" do
      :ok = RoleStore.bind(@principal, @ns, :stopper)
      assert Session.runner_caller_context(@ns).roles == [:stopper]

      refute denied?(Admin.call(@ns, "Operator.stop", args_for("Operator.stop")))

      for verb <- [
            "Operator.start",
            "Operator.send",
            "PluginManager.enable",
            "PluginManager.disable"
          ] do
        assert denied?(Admin.call(@ns, verb, args_for(verb))), verb
      end
    end

    test "the record names the roles the caller was judged with" do
      :ok = RoleStore.bind(@principal, @ns, :spawner)
      :ok = RoleStore.bind(@principal, @ns, :stopper)
      boundary = System.monotonic_time(:microsecond)

      _ = Admin.call(@ns, "Operator.stop", ~s(["o1"]))
      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:operator_service, boundary)
      assert record.name == :stop
      assert record.metadata.caller_roles == [:spawner, :stopper]
    end

    test "a stopper stops nothing in another namespace" do
      :ok = RoleStore.bind(@principal, @ns, :stopper)
      context = Session.runner_caller_context(@ns)

      assert {:error, {:forbidden, :namespace_mismatch}} =
               AuthZ.authorize(context, :"operator.stop", Scope.build(@other))
    end

    test "a user's role is never held by a connection, though the connect passes" do
      :ok = RoleStore.bind(@principal, @ns, :ops)
      context = Session.runner_caller_context(@ns)

      assert context.roles == [:runner]
      assert :ok = Verb.run(:runner_connect, context, [@ns], fn _call -> :ok end)

      assert {:error, {:forbidden, :role_insufficient}} =
               AuthZ.authorize(context, :"operator.stop", Scope.build(@ns))
    end

    test "spawner plus admin holds spawner alone, so the quota surface stays shut" do
      :ok = RoleStore.bind(@principal, @ns, :spawner)
      :ok = RoleStore.bind(@principal, @ns, :admin)

      assert Session.runner_caller_context(@ns).roles == [:spawner]

      assert denied?(Admin.call(@ns, "Namespace.delete", "[]"))

      # `Namespace.set_quotas` takes an object the door hands on as a keyword
      # list, which its own guard refuses before any policy runs, so the rule
      # that keeps it shut is asserted at the policy point itself.
      assert {:error, {:forbidden, :role_insufficient}} =
               AuthZ.authorize(
                 Session.runner_caller_context(@ns),
                 :"authz.namespace_set_quota",
                 Scope.build(@ns)
               )
    end
  end

  describe "the request names no namespace and no principal" do
    setup do
      bind_app!()
      :ok
    end

    test "the proven namespace is inserted where the row declares it, so there is no slot" do
      # `list_operators` takes (namespace, context). The wire array holds the
      # arguments after the namespace, so naming one is an argument too many.
      assert {:ok, _json} = Admin.call(@ns, "Operator.list_operators", "[]")

      assert {:error, %Flezha.Error{code: "admin.wrong_arity"}} =
               Admin.call(@ns, "Operator.list_operators", ~s(["#{@other}"]))
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
          ~s(["p1", {"provider": "Elixir.Flezha.Plugin.Provider.HttpPush",
                     "operator_id": "o1", "namespace": "#{@other}"}])
        )

      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:plugin_service, boundary)
      assert record.metadata.namespace == @ns
    end

    # Fails when: fields the enable does not read leave the requester waiting
    # on a body that raised, or enable a plugin with less than was written.
    test "an enable whose fields carry one it does not read is answered with an error" do
      assert {:error, %Flezha.Error{code: "plugin.fields_unknown", details: details}} =
               Admin.call(
                 @ns,
                 "PluginManager.enable",
                 ~s(["p_old", {"provider": "operator_stdout", "operator_id": "o1",
                               "channel": "on_output"}])
               )

      assert details["fields"] == ["channel"]
      assert Flezha.Service.PluginManager.plugins(@ns) == []
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
      assert {:ok, json} = Admin.call(@ns, "Operator.list_operators", "[]")
      assert {:ok, _list} = Jason.decode(json)
    end

    test "a user-only verb is refused at the policy point" do
      assert {:error, %Flezha.Error{code: "authz.role_insufficient"}} =
               Admin.call(@ns, "AuditLog.rotate", "[]")
    end

    test "an auth-state verb is refused by grant" do
      assert {:error, %Flezha.Error{code: "authz.role_insufficient"}} =
               Admin.call(@ns, "Namespace.create", ~s([{}]))
    end

    test "nothing binds controller.stop on the surface" do
      # The node-wide verb is out of reach because no published function
      # carries it at all, and it is `scope: :system` besides.
      refute Policy.grants?(:app, :"controller.stop")

      rows = Enum.filter(Verbs.all(), &(&1.action == :"controller.stop"))
      assert rows != []
      assert Enum.all?(rows, &(&1.scope == :system))
    end

    test "the terminal's own wire contract is not a door, though it lives under the prefix" do
      assert {:error, %Flezha.Error{code: "admin.unknown_function"}} =
               Admin.call(@ns, "CliManager.operator_stop", ~s(["o1"]))
    end

    test "a name nothing declares is refused without minting an atom" do
      before = :erlang.system_info(:atom_count)

      assert {:error, _} = Admin.call(@ns, "Operator.no_such_function", "[]")
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
      _ = Admin.call(@ns, "Operator.start", ~s([{"id":"o1","factory_id":"f1"}]))
      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:operator_service, boundary)
      assert record.name == :start
      assert record.metadata.action == :"operator.start"
      assert record.metadata.principal == @principal
      assert record.metadata.namespace == @ns
      assert record.metadata.caller_source == :sdk
      assert record.metadata.operator == "o1"
      assert record.metadata.outcome == :error
      assert record.metadata.error_code == "operator.factory_unknown"
    end

    test "a denial is recorded with the code the caller receives" do
      :ok = RoleStore.unbind(@principal)
      boundary = System.monotonic_time(:microsecond)

      assert {:error, %Flezha.Error{code: "authz.role_insufficient"}} =
               Admin.call(@ns, "Operator.stop", ~s(["o1"]))

      TestSinks.settle(RingBufferSink)

      assert [denial] = RingBufferSink.query(:platform_authz, boundary)
      assert denial.name == :authorization_denied
      assert denial.metadata.reason == "authz.role_insufficient"
    end

    test "a start over quota is refused with the quota's code and its details" do
      Mimic.stub(Flezha.Service.OperatorManager, :start_operator, fn _ns, _operator ->
        {:error, {:quota_exceeded, :max_operators, %{current: 3, limit: 3}}}
      end)

      assert {:error, %Flezha.Error{code: "quota.exceeded", class: :conflict} = error} =
               Admin.call(@ns, "Operator.start", ~s([{"id":"o1","factory_id":"f1"}]))

      assert error.details == %{"kind" => "max_operators", "current" => 3, "limit" => 3}
    end

    test "a read writes no audit record, matching its row's declaration" do
      boundary = System.monotonic_time(:microsecond)

      assert {:ok, _json} = Admin.call(@ns, "Operator.list_operators", "[]")
      TestSinks.settle(RingBufferSink)

      assert RingBufferSink.query(:operator_service, boundary) == []
    end
  end

  defp denied?(answer),
    do: match?({:error, %Flezha.Error{code: "authz.role_insufficient"}}, answer)
end
