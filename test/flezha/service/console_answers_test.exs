defmodule Flezha.Service.ConsoleAnswersTest do
  @moduledoc """
  Two answers a console reads to decide what it offers: the operations
  `Flezha.Service.Namespace.whoami/1` says the caller's role grants, and the
  history retention `Flezha.Service.Steps.cells/2` says a relay class runs with.
  """
  # async: false: writes Application env; the auth stores are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_console_answers", claim: true}, :env],
    async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ
  alias Flezha.Config
  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Namespace
  alias Flezha.Service.Steps

  describe "whoami/1 operations" do
    test "each role answers exactly the operations it is granted" do
      for {role, namespace} <- [viewer: "team-orders", ops: "team-orders", admin: "_system"] do
        context = bound(role, namespace)
        assert {:ok, %{role: ^role, operations: operations}} = Namespace.whoami(context)
        assert operations == Enum.map(Admin.catalogue(context), & &1.name)
      end
    end

    test "the lists differ where the roles do" do
      operations = fn role, namespace ->
        {:ok, %{operations: operations}} = Namespace.whoami(bound(role, namespace))
        operations
      end

      viewer = operations.(:viewer, "team-orders")
      ops = operations.(:ops, "team-orders")
      admin = operations.(:admin, "_system")

      assert "Relay.list" in viewer
      refute "Relay.start" in viewer
      assert "Relay.replay" in ops
      refute "Steps.delete" in ops
      assert "Steps.delete" in admin
      assert "Namespace.issue_token" in admin
    end

    test "flz auth whoami prints the identity and nothing more" do
      identity = %{principal: "carol", namespace: "team-orders", role: :ops, operations: ["x"]}
      output = capture_io(fn -> Cli.Authz.whoami(identity, [], []) end)

      assert output == "principal: carol\nnamespace: team-orders\nrole:      ops\n"
    end
  end

  test "an unbound caller under :open answers every operation it reaches" do
    open()

    context = %{source: :cli}
    assert {:ok, %{principal: nil, operations: operations}} = Namespace.whoami(context)
    assert operations == Enum.map(Admin.reachable(context), & &1.name)
    assert "Steps.delete" in operations
  end

  describe "cells/2 history retention" do
    setup do
      open()
    end

    test "answers the default until set_retention/3 changes it" do
      context = %{source: :cli}

      assert {:ok, %{history_retention_hours: default}} = Steps.cells("console_rc", context)
      assert default == Config.get(:default_time_travel_retention_hours)

      assert {:ok, %{hours: 7}} = Steps.set_retention("console_rc", 7, context)
      assert {:ok, %{history_retention_hours: 7}} = Steps.cells("console_rc", context)
    end
  end

  defp open do
    config = Application.get_env(:flezha, :authn_authz, [])
    Application.put_env(:flezha, :authn_authz, Keyword.put(config, :enabled, false))
    assert AuthZ.enforcement_posture() == :open
    :ok
  end

  defp bound(role, namespace),
    do: %{source: :cli, principal: "#{role}-caller", namespace: namespace, role: role}
end
