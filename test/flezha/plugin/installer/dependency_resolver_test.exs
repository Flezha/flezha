defmodule Flezha.Plugin.Installer.DependencyResolverTest do
  @moduledoc """
  Spec-driven tests for `Flezha.Plugin.Installer.DependencyResolver`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Installer.DependencyResolver

  test "satisfied dep passes" do
    manifest = %{id: "a", version: "1.0.0", dependencies: [{"common", ">= 1.0.0"}]}
    catalog = [{"common", %{version: "1.2.3", dependencies: []}}]

    assert :ok = DependencyResolver.validate_install(manifest, catalog)
  end

  test "missing dep rejected" do
    manifest = %{id: "a", version: "1.0.0", dependencies: [{"common", ">= 1.0.0"}]}

    assert {:error, {:dependency_missing, "common", ">= 1.0.0"}} =
             DependencyResolver.validate_install(manifest, [])
  end

  test "constraint failure rejected" do
    manifest = %{id: "a", version: "1.0.0", dependencies: [{"common", ">= 2.0.0"}]}
    catalog = [{"common", %{version: "1.2.3", dependencies: []}}]

    assert {:error, {:dependency_constraint, "common", ">= 2.0.0", "1.2.3"}} =
             DependencyResolver.validate_install(manifest, catalog)
  end

  test "simple A->B->A cycle rejected" do
    manifest = %{id: "a", version: "1.0.0", dependencies: [{"b", ">= 0.0.0"}]}

    catalog = [
      {"b", %{version: "1.0.0", dependencies: [{"a", ">= 0.0.0"}]}}
    ]

    assert {:error, {:dependency_cycle, _path}} =
             DependencyResolver.validate_install(manifest, catalog)
  end

  test "three-node cycle A->B->C->A rejected" do
    manifest = %{id: "a", version: "1.0.0", dependencies: [{"b", ">= 0.0.0"}]}

    catalog = [
      {"b", %{version: "1.0.0", dependencies: [{"c", ">= 0.0.0"}]}},
      {"c", %{version: "1.0.0", dependencies: [{"a", ">= 0.0.0"}]}}
    ]

    assert {:error, {:dependency_cycle, _path}} =
             DependencyResolver.validate_install(manifest, catalog)
  end

  test "upgrade that breaks a dependent rejected" do
    # common 1.2.3 installed, x depends on common ~> 1.2
    catalog = [
      {"common", %{version: "1.2.3", dependencies: []}},
      {"x", %{version: "1.0.0", dependencies: [{"common", "~> 1.2"}]}}
    ]

    new_manifest = %{id: "common", version: "2.0.0", dependencies: []}

    assert {:error, {:dependents_block, ["x ~> 1.2"]}} =
             DependencyResolver.validate_upgrade(new_manifest, "common", catalog)
  end

  test "upgrade that still satisfies dependents allowed" do
    catalog = [
      {"common", %{version: "1.2.3", dependencies: []}},
      {"x", %{version: "1.0.0", dependencies: [{"common", ">= 1.0.0"}]}}
    ]

    new_manifest = %{id: "common", version: "1.3.0", dependencies: []}

    assert :ok = DependencyResolver.validate_upgrade(new_manifest, "common", catalog)
  end

  test "find_dependents returns every plugin depending on id" do
    catalog = [
      {"common", %{version: "1.0.0", dependencies: []}},
      {"x", %{version: "1.0.0", dependencies: [{"common", ">= 1.0.0"}]}},
      {"y", %{version: "1.0.0", dependencies: []}},
      {"z", %{version: "1.0.0", dependencies: [{"common", ">= 1.0.0"}]}}
    ]

    assert DependencyResolver.find_dependents("common", catalog) |> Enum.sort() ==
             ["x", "z"]
  end
end
