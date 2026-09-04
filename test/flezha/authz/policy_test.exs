defmodule Flezha.AuthZ.PolicyTest do
  use ExUnit.Case, async: true

  alias Flezha.AuthZ.{Action, Policy, Role}

  describe "grants?/2" do
    test "viewer has read-only verbs" do
      for a <- [:"operative.list", :"audit.read", :"observability.query"] do
        assert Policy.grants?(:viewer, a)
      end
    end

    test "viewer cannot start operatives or mutate" do
      for a <- [:"operative.start", :"plugin.install", :"state.retention", :"authz.token_issue"] do
        refute Policy.grants?(:viewer, a)
      end
    end

    test "operator inherits viewer plus operative/plugin lifecycle and observability mutate" do
      assert Policy.grants?(:operator, :"operative.list")
      assert Policy.grants?(:operator, :"operative.start")
      assert Policy.grants?(:operator, :"plugin.enable")
      assert Policy.grants?(:operator, :"observability.enable_source")
      refute Policy.grants?(:operator, :"plugin.install")
      refute Policy.grants?(:operator, :"authz.token_issue")
    end

    test "admin inherits operator plus auth-state mutation and the controller-wide settings" do
      assert Policy.grants?(:admin, :"operative.start")
      assert Policy.grants?(:admin, :"plugin.install")
      assert Policy.grants?(:admin, :"authz.token_issue")
      assert Policy.grants?(:admin, :"authz.namespace_create")
      assert Policy.grants?(:admin, :"state.retention")
      refute Policy.grants?(:operator, :"state.retention")
    end

    test "runner holds runner.connect and nothing else" do
      assert Policy.grants?(:runner, :"runner.connect")
      assert Enum.filter(Action.all(), &Policy.grants?(:runner, &1)) == [:"runner.connect"]

      for a <- [:"operative.start", :"operative.list", :"audit.read", :"observability.query"] do
        refute Policy.grants?(:runner, a)
      end
    end

    test "unknown role denies everything" do
      refute Policy.grants?(:superuser, :"operative.start")
    end
  end

  test "every (role, action) pair has a deterministic decision" do
    # Sanity: every documented action × every role returns boolean.
    for role <- Role.all(), action <- Action.all() do
      assert is_boolean(Policy.grants?(role, action))
    end
  end

  test "cross_namespace_admin_action?/1 covers auth-state mutations" do
    assert Policy.cross_namespace_admin_action?(:"authz.namespace_create")
    assert Policy.cross_namespace_admin_action?(:"authz.token_issue")
    refute Policy.cross_namespace_admin_action?(:"operative.start")
    refute Policy.cross_namespace_admin_action?(:"audit.read")
  end
end
