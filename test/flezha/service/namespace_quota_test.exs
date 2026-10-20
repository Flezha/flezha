defmodule Flezha.Service.NamespaceQuotaTest do
  @moduledoc """
  A quota override names a quota field, as text or as an atom, resolved
  against the default quotas' own keys: any other name is refused naming it,
  and a ceiling that is not a whole number names its field.
  """
  # async: false: the auth stores are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_ns_quota", enabled: false}],
    async: false

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Service.Namespace

  @context %{source: :cli}

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
