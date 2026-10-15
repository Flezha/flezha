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

  import FlezhaTest.Case, only: [refute_minted: 2]

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
  defp args_for("Relay.start"), do: ~s([{"id":"o1"}])
  defp args_for("Relay.send"), do: ~s(["o1", "hello", "in"])
  defp args_for("Relay.stop"), do: ~s(["o1"])
  defp args_for("PluginManager.enable"), do: ~s(["p1", {"provider": "relay_stdout"}])
  defp args_for("PluginManager.disable"), do: ~s(["p1", {}])

  # The door as a runner connection reaches it: the namespace it proved.
  defp door(verb, args),
    do: Admin.call(verb, args, Session.runner_caller_context(@ns), {:proven, @ns})

  describe "the refusal this change starts from" do
    test "a connected runner's identity holds runner.connect and no user action" do
      context = Session.runner_caller_context(@ns)
      assert context.roles == [:runner]

      assert :ok = AuthZ.authorize(context, :"runner.connect", Scope.build(@ns))

      for action <- [
            :"relay.start",
            :"relay.stop",
            :"relay.send",
            :"plugin.enable",
            :"plugin.disable"
          ] do
        assert {:error, {:forbidden, :role_insufficient}} =
                 AuthZ.authorize(context, action, Scope.build(@ns))
      end
    end

    test "the door refuses a start and the error names the missing grant" do
      assert {:error, %Flezha.Error{code: "authz.role_insufficient", class: :denied}} =
               door("Relay.start", ~s([{"id":"o1"}]))
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
               door("Relay.stop", ~s(["o1"]))
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

      for verb <- ["Relay.start", "Relay.send", "Relay.stop"] do
        refute denied?(door(verb, args_for(verb))), verb
      end

      assert denied?(door("PluginManager.enable", args_for("PluginManager.enable")))
    end

    test "a stopper alone stops, and nothing else" do
      :ok = RoleStore.bind(@principal, @ns, :stopper)
      assert Session.runner_caller_context(@ns).roles == [:stopper]

      refute denied?(door("Relay.stop", args_for("Relay.stop")))

      for verb <- [
            "Relay.start",
            "Relay.send",
            "PluginManager.enable",
            "PluginManager.disable"
          ] do
        assert denied?(door(verb, args_for(verb))), verb
      end
    end

    test "the record names the roles the caller was judged with" do
      :ok = RoleStore.bind(@principal, @ns, :spawner)
      :ok = RoleStore.bind(@principal, @ns, :stopper)
      boundary = System.monotonic_time(:microsecond)

      _ = door("Relay.stop", ~s(["o1"]))
      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:relay_service, boundary)
      assert record.name == :stop
      assert record.metadata.caller_roles == [:spawner, :stopper]
    end

    test "a stopper stops nothing in another namespace" do
      :ok = RoleStore.bind(@principal, @ns, :stopper)
      context = Session.runner_caller_context(@ns)

      assert {:error, {:forbidden, :namespace_mismatch}} =
               AuthZ.authorize(context, :"relay.stop", Scope.build(@other))
    end

    test "a user's role is never held by a connection, though the connect passes" do
      :ok = RoleStore.bind(@principal, @ns, :ops)
      context = Session.runner_caller_context(@ns)

      assert context.roles == [:runner]
      assert :ok = Verb.run(:runner_connect, context, [@ns], fn _call -> :ok end)

      assert {:error, {:forbidden, :role_insufficient}} =
               AuthZ.authorize(context, :"relay.stop", Scope.build(@ns))
    end

    test "spawner plus admin holds spawner alone, so the quota surface stays shut" do
      :ok = RoleStore.bind(@principal, @ns, :spawner)
      :ok = RoleStore.bind(@principal, @ns, :admin)

      assert Session.runner_caller_context(@ns).roles == [:spawner]

      assert denied?(door("Namespace.set_quotas", ~s([{"max_relays": 1}])))

      # Deleting a namespace is not reachable over a connection at all.
      assert {:error, %Flezha.Error{code: "control.unknown_function"}} =
               door("Namespace.delete", "[]")
    end
  end

  describe "the request names no namespace and no principal" do
    setup do
      bind_app!()
      :ok
    end

    test "the proven namespace is inserted where the row declares it, so there is no slot" do
      # `list_relays` takes (namespace, context). The wire array holds the
      # arguments after the namespace, so naming one is an argument too many.
      assert {:ok, _json} = door("Relay.list_relays", "[]")

      assert {:error, %Flezha.Error{code: "control.wrong_arity"}} =
               door("Relay.list_relays", ~s(["#{@other}"]))
    end

    test "a row-scoped verb's namespace field is stamped, not read from the caller" do
      # `plugin_enable` is judged in the namespace its own fields name, so the
      # slot is a named one rather than a position. A caller writing another
      # namespace there is judged in the proven one anyway, which the record
      # the act writes is the proof of.
      boundary = System.monotonic_time(:microsecond)

      _ =
        door(
          "PluginManager.enable",
          ~s(["p1", {"provider": "Elixir.Flezha.Plugin.Provider.HttpPush",
                     "relay_id": "o1", "namespace": "#{@other}"}])
        )

      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:plugin_service, boundary)
      assert record.metadata.namespace == @ns
    end

    # Fails when: fields the enable does not read leave the requester waiting
    # on a body that raised, or enable a plugin with less than was written.
    test "an enable whose fields carry one it does not read is answered with an error" do
      assert {:error, %Flezha.Error{code: "plugin.fields_unknown", details: details}} =
               door(
                 "PluginManager.enable",
                 ~s(["p_old", {"provider": "relay_stdout", "relay_id": "o1",
                               "trigger": "on_output"}])
               )

      assert details["fields"] == ["trigger"]
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
      assert {:ok, json} = door("Relay.list_relays", "[]")
      assert {:ok, _list} = Jason.decode(json)
    end

    test "a user-only verb is refused at the policy point" do
      assert {:error, %Flezha.Error{code: "authz.role_insufficient"}} =
               door("AuditLog.rotate", "[]")
    end

    test "an auth-state verb is refused by grant" do
      assert {:error, %Flezha.Error{code: "authz.role_insufficient"}} =
               door("Namespace.create", ~s([{}]))
    end

    test "nothing binds controller.stop on the surface" do
      # The node-wide verb is out of reach because no published function
      # carries it at all, and it is `scope: :system` besides.
      refute Policy.grants?(:app, :"controller.stop")

      rows = Enum.filter(Verbs.all(), &(&1.action == :"controller.stop"))
      assert rows != []
      assert Enum.all?(rows, &(&1.scope == :system))
    end

    test "the terminal's own entry is not a door, though it lives under the prefix" do
      assert {:error, %Flezha.Error{code: "control.unknown_function"}} =
               door("Cli.call", ~s(["Relay.stop", "[]", []]))
    end

    test "a name nothing declares is refused without minting an atom" do
      names = [
        "no_such_function_7d2b",
        "Elixir.Flezha.Service.NoSuchModule7d2b",
        "malformed_7d2b"
      ]

      refute_minted(names, fn ->
        assert {:error, _} = door("Relay.no_such_function_7d2b", "[]")
        assert {:error, _} = door("NoSuchModule7d2b.anything", "[]")
        assert {:error, _} = door("malformed_7d2b", "[]")
      end)
    end

    # Fails when: a runner reaches a stream, whose task and listener stay
    # attached long after the one reply it can receive.
    test "an operation that streams is not reachable from a runner connection" do
      for {name, args} <- [{"Relay.listen", ~s(["o1"])}, {"Observability.tail", ~s(["x"])}] do
        assert {:error, %Flezha.Error{code: "control.unknown_function"}} = door(name, args)
      end
    end

    # Fails when: an option key a runner writes is minted, one atom per request.
    test "a plugin option key naming no atom is not minted" do
      refute_minted(["opt_never_7c1e"], fn ->
        assert {:error, _} =
                 door("PluginManager.update", ~s(["p_none", {"opts": {"opt_never_7c1e": 1}}]))
      end)
    end

    # Fails when: `null` reaches the body as `nil` instead of the declared default.
    test "an optional argument written as null takes its default" do
      assert {:error, %Flezha.Error{code: code}} =
               door("Relay.send", ~s(["o1", "m", "", null]))

      refute code == "internal.uncatalogued"
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
      _ = door("Relay.start", ~s([{"id":"o1","factory_id":"f1"}]))
      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:relay_service, boundary)
      assert record.name == :start
      assert record.metadata.action == :"relay.start"
      assert record.metadata.principal == @principal
      assert record.metadata.namespace == @ns
      assert record.metadata.caller_source == :sdk
      assert record.metadata.relay == "o1"
      assert record.metadata.outcome == :error
      assert record.metadata.error_code == "relay.factory_unknown"
    end

    test "a denial is recorded with the code the caller receives" do
      :ok = RoleStore.unbind(@principal)
      boundary = System.monotonic_time(:microsecond)

      assert {:error, %Flezha.Error{code: "authz.role_insufficient"}} =
               door("Relay.stop", ~s(["o1"]))

      TestSinks.settle(RingBufferSink)

      assert [denial] = RingBufferSink.query(:platform_authz, boundary)
      assert denial.name == :authorization_denied
      assert denial.metadata.reason == "authz.role_insufficient"
    end

    test "a start over quota is refused with the quota's code and its details" do
      Mimic.stub(Flezha.Service.RelayManager, :start_relay, fn _ns, _relay, _opts ->
        {:error, {:quota_exceeded, :max_relays, %{current: 3, limit: 3}}}
      end)

      assert {:error, %Flezha.Error{code: "quota.exceeded", class: :conflict} = error} =
               door("Relay.start", ~s([{"id":"o1","factory_id":"f1"}]))

      assert error.details == %{"kind" => "max_relays", "current" => 3, "limit" => 3}
    end

    test "a read writes no audit record, matching its row's declaration" do
      boundary = System.monotonic_time(:microsecond)

      assert {:ok, _json} = door("Relay.list_relays", "[]")
      TestSinks.settle(RingBufferSink)

      assert RingBufferSink.query(:relay_service, boundary) == []
    end
  end

  describe "an HTTP caller" do
    # The context the HTTP surface builds from a verified token.
    defp http_context(role), do: %{source: :http, principal: "alice", namespace: @ns, role: role}

    # Fails when: a `:given` caller over HTTP reaches a verb kept off the
    # network, or a stream that holds its request for as long as it runs.
    test "reaches no operation that is not remote or that streams" do
      unreachable =
        Enum.filter(Flezha.Service.catalogue(), &(not &1.remote or &1.stream == true))

      assert Enum.any?(unreachable, &(&1.name == "Controller.stop"))

      for entry <- unreachable do
        assert {:error, %Flezha.Error{code: "control.unknown_function"}} =
                 Admin.call(entry.name, "[]", http_context(:viewer), :given)
      end
    end

    test "is offered only the remote operations its role grants" do
      offered = Admin.catalogue(http_context(:viewer))

      assert offered != []
      assert Enum.all?(offered, &(&1.remote and &1.stream != true))
      assert Enum.all?(offered, &(&1.action == :public or Policy.grants?(:viewer, &1.action)))
      refute Enum.any?(offered, &(&1.name == "Relay.start"))
      assert Enum.any?(Admin.catalogue(http_context(:app)), &(&1.name == "Relay.start"))
      assert Admin.catalogue(%{source: :http}) == []
    end

    test "with authorization open, a caller with no role is offered every operation it reaches" do
      prev = Application.get_env(:flezha, :authn_authz, [])
      Application.put_env(:flezha, :authn_authz, Keyword.put(prev, :enabled, false))

      try do
        offered = Admin.catalogue(%{source: :mcp})

        assert offered == Admin.reachable(%{source: :mcp})
        assert Enum.any?(offered, &(&1.name == "Relay.start"))
        refute Enum.any?(offered, &(&1.name == "Controller.stop"))
      after
        Application.put_env(:flezha, :authn_authz, prev)
      end
    end

    test "a start is recorded naming the principal and the source http" do
      boundary = System.monotonic_time(:microsecond)
      args = Jason.encode!([@ns, %{id: "o1", factory_id: "f1"}])
      _ = Admin.call("Relay.start", args, http_context(:app), :given)
      TestSinks.settle(RingBufferSink)

      assert [record] = RingBufferSink.query(:relay_service, boundary)
      assert record.metadata.principal == "alice"
      assert record.metadata.caller_source == :http
      assert record.metadata.action == :"relay.start"
    end
  end

  defp denied?(answer),
    do: match?({:error, %Flezha.Error{code: "authz.role_insufficient"}}, answer)
end
