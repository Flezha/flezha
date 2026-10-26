defmodule Flezha.Service.NamespaceQuotaTest do
  @moduledoc """
  A quota override names a quota field, as text or as an atom, resolved
  against the default quotas' own keys: any other name is refused naming it,
  and a ceiling that is not a whole number names its field. The migration
  setting takes `cold` or `hot` and refuses any other word, keeping the record.
  """
  # async: false: the auth stores are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_ns_quota", enabled: false}],
    async: false

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Service.Admin
  alias Flezha.Service.Namespace

  @context %{source: :cli}

  # Fails when: the setting takes a word outside cold and hot, or a refusal
  # still changes the record.
  test "set_migration takes cold or hot, refuses anything else, and show reports it" do
    assert {:ok, %{migration: :cold}} = Namespace.create("moves-ns", [], @context)

    assert {:ok, %{migration: :hot}} =
             Admin.run("Namespace.set_migration", ~s(["moves-ns", "hot"]), @context, :given)

    assert {:error, %Flezha.Error{code: "usage.unknown_word"}} =
             Admin.run("Namespace.set_migration", ~s(["moves-ns", "warm"]), @context, :given)

    assert {:ok, %{migration: :hot}} = Namespace.get("moves-ns", @context)

    assert {:ok, %{migration: :cold}} =
             Admin.run("Namespace.set_migration", ~s(["moves-ns", "cold"]), @context, :given)

    assert {:error, %Flezha.Error{code: "namespace.not_found"}} =
             Admin.run("Namespace.set_migration", ~s(["no-such-ns", "hot"]), @context, :given)
  end

  # Fails when: a quota field is any atom the node knows.
  test "a word that is no quota field is refused, as text and as an atom" do
    for field <- ["enabled", :enabled] do
      assert {:error, %Flezha.Error{code: "namespace.unknown_quota_field", message: message}} =
               Namespace.create("quota-ns", [quotas: %{field => "x"}], @context)

      assert message =~ "'enabled' is not a quota field"
    end
  end

  # Fails when: a field is resolved against anything but the default quotas' keys.
  test "a quota field is taken, as text and as an atom" do
    assert {:ok, _} = Namespace.create("quota-ns", [quotas: %{"max_relays" => 5}], @context)
    assert {:ok, %{quotas: %{max_relays: 5}}} = NamespaceStore.get("quota-ns")
    assert {:ok, _} = Namespace.set_quotas("quota-ns", %{max_runners: 3}, @context)
    assert {:ok, %{quotas: %{max_runners: 3}}} = NamespaceStore.get("quota-ns")
  end

  test "a ceiling that is not a whole number names its field" do
    assert {:error, %Flezha.Error{code: "usage.invalid_number", details: details}} =
             Namespace.create("quota-ns", [quotas: %{"max_relays" => "ten"}], @context)

    assert details == %{"option" => "max_relays", "value" => "ten"}
  end
end
