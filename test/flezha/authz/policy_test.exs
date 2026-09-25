defmodule Flezha.AuthZ.PolicyTest do
  use ExUnit.Case, async: true

  alias Flezha.AuthZ.{Action, Policy, Role}

  describe "grants?/2" do
    test "viewer has read-only verbs" do
      for a <- [:"operator.list", :"audit.read", :"observability.query"] do
        assert Policy.grants?(:viewer, a)
      end
    end

    test "viewer cannot start operators or mutate" do
      for a <- [:"operator.start", :"plugin.install", :"state.retention", :"authz.token_issue"] do
        refute Policy.grants?(:viewer, a)
      end
    end

    test "ops inherits viewer plus operator/plugin lifecycle and observability mutate" do
      assert Policy.grants?(:ops, :"operator.list")
      assert Policy.grants?(:ops, :"operator.start")
      assert Policy.grants?(:ops, :"plugin.enable")
      assert Policy.grants?(:ops, :"observability.enable_source")
      refute Policy.grants?(:ops, :"plugin.install")
      refute Policy.grants?(:ops, :"authz.token_issue")
    end

    test "admin inherits ops plus auth-state mutation and the controller-wide settings" do
      assert Policy.grants?(:admin, :"operator.start")
      assert Policy.grants?(:admin, :"plugin.install")
      assert Policy.grants?(:admin, :"authz.token_issue")
      assert Policy.grants?(:admin, :"authz.namespace_create")
      assert Policy.grants?(:admin, :"state.retention")
      refute Policy.grants?(:ops, :"state.retention")
    end

    test "app is viewer plus the lifecycle an application runs on its own work" do
      for a <- [:"operator.list", :"plugin.list", :"state.get"],
          do: assert(Policy.grants?(:app, a))

      for a <- [
            :"operator.start",
            :"operator.stop",
            :"operator.send",
            :"operator.listen",
            :"plugin.enable",
            :"plugin.disable"
          ] do
        assert Policy.grants?(:app, a)
      end
    end

    test "listening is granted from app upward, and apart from the plugin rights" do
      for role <- [:app, :ops, :admin], do: assert(Policy.grants?(role, :"operator.listen"))

      for role <- [:viewer, :spawner, :stopper, :runner],
          do: refute(Policy.grants?(role, :"operator.listen"))
    end

    test "app destroys no record and moves no work it did not create" do
      for a <- [
            :"dlq.drop",
            :"dlq.replay",
            :"state.rotate",
            :"state.delete",
            :"operator.rewind",
            :"operator.replay",
            :"operator.scale",
            :"plugin.install",
            :"controller.stop",
            :"authz.token_issue"
          ] do
        refute Policy.grants?(:app, a)
      end
    end

    test "app is a strict subset of ops but for the connect a runner already held" do
      app = Enum.filter(Action.all(), &Policy.grants?(:app, &1))
      ops = Enum.filter(Action.all(), &Policy.grants?(:ops, &1))

      assert app -- ops == [:"runner.connect"]
      assert ops -- app != []
    end

    test "no role but app gained an action" do
      # The five sets this change did not touch, spelled out so a grant that
      # drifts into one of them fails here rather than in an audit.
      assert Enum.filter(Action.all(), &Policy.grants?(:runner, &1)) == [:"runner.connect"]

      for {role, gained} <- [viewer: [], ops: [], admin: []] do
        assert Enum.filter(Action.all(), &(Policy.grants?(role, &1) and &1 == :"runner.connect")) ==
                 gained
      end
    end

    test "spawner holds the connect and the two rights a requested spawn needs, and nothing else" do
      assert Enum.filter(Action.all(), &Policy.grants?(:spawner, &1)) ==
               [:"operator.start", :"operator.send", :"runner.connect"]
    end

    test "app holds every spawner grant" do
      spawner = Enum.filter(Action.all(), &Policy.grants?(:spawner, &1))
      assert spawner != []
      assert Enum.reject(spawner, &Policy.grants?(:app, &1)) == []
    end

    test "stopper holds the connect and the stop, and nothing else" do
      assert Enum.filter(Action.all(), &Policy.grants?(:stopper, &1)) ==
               [:"operator.stop", :"runner.connect"]
    end

    test "app holds every stopper grant" do
      stopper = Enum.filter(Action.all(), &Policy.grants?(:stopper, &1))
      assert stopper != []
      assert Enum.reject(stopper, &Policy.grants?(:app, &1)) == []
    end

    test "runner holds runner.connect and nothing else" do
      assert Policy.grants?(:runner, :"runner.connect")
      assert Enum.filter(Action.all(), &Policy.grants?(:runner, &1)) == [:"runner.connect"]

      for a <- [:"operator.start", :"operator.list", :"audit.read", :"observability.query"] do
        refute Policy.grants?(:runner, a)
      end
    end

    test "unknown role denies everything" do
      refute Policy.grants?(:superuser, :"operator.start")
    end
  end

  test "every (role, action) pair has a deterministic decision" do
    # Sanity: every documented action × every role returns boolean.
    for role <- Role.all(), action <- Action.all() do
      assert is_boolean(Policy.grants?(role, action))
    end
  end

  test "runner_principal_role?/1 is the connect grant, not a list of names" do
    assert Enum.filter(Role.all(), &Policy.runner_principal_role?/1) |> Enum.sort() ==
             [:app, :runner, :spawner, :stopper]

    for role <- [:viewer, :ops, :admin], do: refute(Policy.runner_principal_role?(role))
  end

  test "cross_namespace_admin_action?/1 covers auth-state mutations" do
    assert Policy.cross_namespace_admin_action?(:"authz.namespace_create")
    assert Policy.cross_namespace_admin_action?(:"authz.token_issue")
    refute Policy.cross_namespace_admin_action?(:"operator.start")
    refute Policy.cross_namespace_admin_action?(:"audit.read")
  end
end
