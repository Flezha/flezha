defmodule Flezha.AuthZ.RunnerNamespaceProofTest do
  @moduledoc """
  prove-the-namespace-and-recover-a-lost-admin: what a runner connection proves
  about the namespace it names, and what it costs when it proves nothing.

  The reproduction the change opened with is the first describe: one credential
  admitted to two namespaces, because the value the check compared came from the
  frame on both sides.
  """
  # async: false: writes Application env; a shared on-disk root; the live fleet.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_runner_proof", claim: true}],
    async: false

  alias Flezha.AuthZ
  alias Flezha.AuthZ.RunnerProof
  alias Flezha.AuthZ.Scope
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Runner.FleetManager
  alias Flezha.Test.CertFixtures
  alias FlezhaTest.Helper.AuthTestHelper

  @ns_a "team-orders"
  @ns_b "team-payments"

  setup do
    assert AuthZ.enabled?()
    {secret_a, hash_a} = AuthTestHelper.issue_runner_credential(@ns_a)
    {:ok, secret_a: secret_a, hash_a: hash_a}
  end

  defp frame(namespace, secret) do
    %{"namespace" => namespace, "namespace_secret" => secret}
  end

  describe "a claimed namespace is checked against a proven one" do
    test "the credential's namespace is the one the connection acts in", %{secret_a: secret} do
      assert {:ok, %{namespace: @ns_a}} =
               RunnerProof.prove(nil, @ns_a, frame(@ns_a, secret), :uds)
    end

    test "a frame naming a namespace the credential does not entitle is refused", %{
      secret_a: secret
    } do
      assert {:error, {:namespace_not_entitled, @ns_b, @ns_a}} =
               RunnerProof.prove(nil, @ns_b, frame(@ns_b, secret), :uds)
    end

    test "a connect presenting no credential at all is refused" do
      assert {:error, :namespace_not_proven} = RunnerProof.prove(nil, @ns_a, %{}, :uds)
    end

    test "a credential that is not a runner credential is refused" do
      {:ok, admin, _} = TokenStore.issue("someone", @ns_a, :admin, 3_600)

      assert {:error, :namespace_not_proven} =
               RunnerProof.prove(nil, @ns_a, frame(@ns_a, admin), :uds)
    end

    test "a revoked credential is refused", %{secret_a: secret, hash_a: hash} do
      {:ok, _} = TokenStore.revoke(String.slice(hash, 0, 12))

      assert {:error, :namespace_not_proven} =
               RunnerProof.prove(nil, @ns_a, frame(@ns_a, secret), :uds)
    end

    test "the connect path refuses the unentitled frame before any session is minted", %{
      secret_a: secret
    } do
      assert {:error, {:namespace_not_entitled, @ns_b, @ns_a}} =
               FleetManager.connect_handler(make_ref(), @ns_b, frame(@ns_b, secret), :uds)
    end
  end

  describe "a namespace holds more than one live secret" do
    test "both live secrets are admitted, and retiring one leaves the other", %{
      secret_a: first,
      hash_a: first_hash
    } do
      {second, _} = AuthTestHelper.issue_runner_credential(@ns_a)

      assert {:ok, %{namespace: @ns_a}} = RunnerProof.prove(nil, @ns_a, frame(@ns_a, first), :uds)

      assert {:ok, %{namespace: @ns_a}} =
               RunnerProof.prove(nil, @ns_a, frame(@ns_a, second), :uds)

      {:ok, _} = TokenStore.revoke(String.slice(first_hash, 0, 12))

      assert {:error, :namespace_not_proven} =
               RunnerProof.prove(nil, @ns_a, frame(@ns_a, first), :uds)

      assert {:ok, %{namespace: @ns_a}} =
               RunnerProof.prove(nil, @ns_a, frame(@ns_a, second), :uds)
    end
  end

  describe "a runner credential does not lapse" do
    test "a runner-role token refuses a ttl" do
      assert {:error, :runner_token_cannot_expire} =
               TokenStore.issue("runner@" <> @ns_b, @ns_b, :runner, 3_600)
    end
  end

  describe "_system is refused at runner connect" do
    test "a frame naming _system is refused whatever it presents", %{secret_a: secret} do
      assert {:error, :system_namespace_refused} =
               RunnerProof.prove(nil, "_system", frame("_system", secret), :uds)
    end

    test "no runner credential can be issued for _system" do
      assert {:error, :runner_namespace_cannot_be_system} =
               TokenStore.issue("runner@_system", "_system", :runner, :never)
    end

    test "no runner identity satisfies the cross-namespace admin arm" do
      # Two independent reasons. The arm needs the `admin` role and `runner` sits
      # outside the hierarchy, so even a context spelled `_system` is refused
      # anywhere else…
      spelled = FleetManager.runner_caller_context("_system")

      assert {:error, {:forbidden, :namespace_mismatch}} =
               AuthZ.authorize(spelled, :"runner.connect", Scope.build(@ns_a))

      # …and no connect can mint such a context in the first place.
      assert {:error, :system_namespace_refused} = RunnerProof.prove(nil, "_system", %{}, :uds)
    end
  end

  describe "what a real certificate settles (task 2.4)" do
    test "a certificate naming /ns/<name> proves that namespace" do
      der =
        CertFixtures.self_signed_der(
          cn: "runner-7",
          spiffe: "spiffe://flezha.local/ns/" <> @ns_a <> "/sa/runner-7"
        )

      assert {:ok, @ns_a} = RunnerProof.certificate_namespace(der)
    end

    test "a certificate with no /ns/ entity names no namespace, so the secret is asked" do
      assert :none =
               RunnerProof.certificate_namespace(
                 CertFixtures.self_signed_der(
                   cn: "runner-7",
                   spiffe: "spiffe://flezha.local/admins/alice"
                 )
               )

      assert :none = RunnerProof.certificate_namespace(CertFixtures.self_signed_der(cn: "x"))
      assert :none = RunnerProof.certificate_namespace(<<0, 1, 2>>)
      assert :none = RunnerProof.certificate_namespace(nil)
    end
  end
end
