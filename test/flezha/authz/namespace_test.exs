defmodule Flezha.AuthZ.NamespaceTest do
  use ExUnit.Case, async: true

  alias Flezha.AuthZ.Namespace

  test "system_name/0 returns `_system`" do
    assert Namespace.system_name() == "_system"
    assert Namespace.system?("_system")
    refute Namespace.system?("team-orders")
  end

  describe "validate_name/1" do
    test "accepts _system and lowercase names with hyphens or underscores" do
      assert :ok = Namespace.validate_name("_system")
      assert :ok = Namespace.validate_name("team-orders")
      assert :ok = Namespace.validate_name("team_orders")
      assert :ok = Namespace.validate_name("a")
      assert :ok = Namespace.validate_name(String.duplicate("a", 63))
    end

    test "rejects malformed names" do
      assert {:error, {:invalid_namespace_name, "Team-Orders"}} =
               Namespace.validate_name("Team-Orders")

      assert {:error, {:invalid_namespace_name, "-leading-hyphen"}} =
               Namespace.validate_name("-leading-hyphen")

      assert {:error, {:invalid_namespace_name, "1starts-with-digit"}} =
               Namespace.validate_name("1starts-with-digit")

      assert {:error, {:invalid_namespace_name, "_team-orders"}} =
               Namespace.validate_name("_team-orders")

      assert {:error, {:invalid_namespace_name, _}} =
               Namespace.validate_name(String.duplicate("a", 64))

      assert {:error, {:invalid_namespace_name, :atom}} = Namespace.validate_name(:atom)
    end
  end

  describe "build_quotas/1" do
    test "returns defaults when overrides are empty" do
      {:ok, q} = Namespace.build_quotas(%{})
      assert q.max_operators == 100
      assert q.max_state_bytes == 107_374_182_400
    end

    test "applies user overrides over defaults" do
      {:ok, q} = Namespace.build_quotas(%{max_operators: 5})
      assert q.max_operators == 5
      assert q.max_state_bytes == 107_374_182_400
    end

    test "rejects unknown fields and negative values" do
      assert {:error, {:unknown_quota_field, :nope}} = Namespace.build_quotas(%{nope: 1})

      assert {:error, {:invalid_quota_value, :max_operators, -1}} =
               Namespace.build_quotas(%{max_operators: -1})
    end
  end
end
