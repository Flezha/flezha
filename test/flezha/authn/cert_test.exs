defmodule Flezha.AuthN.CertTest do
  @moduledoc """
  Round-trip tests for `Flezha.AuthN.Cert`. Builds X.509 certs at
  runtime via `Flezha.Test.CertFixtures` with controlled subject DNs
  and optional SPIFFE URI SANs.
  """

  use ExUnit.Case, async: true

  alias Flezha.AuthN.Cert
  alias Flezha.Test.CertFixtures

  describe "principal_from_peer/1" do
    test "SPIFFE SAN wins over subject DN" do
      san = "spiffe://flezha.local/ns/team-orders/sa/executor-7"
      der = CertFixtures.self_signed_der(cn: "executor-7", o: "team-orders", spiffe: san)
      assert {:ok, ^san} = Cert.principal_from_peer(der)
    end

    test "subject DN used when no SPIFFE SAN" do
      der = CertFixtures.self_signed_der(cn: "executor-7", o: "team-orders")
      assert {:ok, "CN=executor-7,O=team-orders"} = Cert.principal_from_peer(der)
    end

    test "returns :invalid_cert on garbage input" do
      assert {:error, :invalid_cert} = Cert.principal_from_peer(<<0, 1, 2, 3>>)
    end

    test "RFC 4514 escapes special characters" do
      der = CertFixtures.self_signed_der(cn: "alice, jr", o: "ACME+Co")
      assert {:ok, "CN=alice\\, jr,O=ACME\\+Co"} = Cert.principal_from_peer(der)
    end
  end

  describe "namespace_from_peer/1" do
    test "SPIFFE SAN with /ns/<name>/... binds to <name>" do
      der =
        CertFixtures.self_signed_der(
          cn: "x",
          spiffe: "spiffe://flezha.local/ns/team-orders/sa/exec-1"
        )

      assert {:ok, "team-orders"} = Cert.namespace_from_peer(der)
    end

    test "SPIFFE SAN with no /ns/ segment falls back to _system" do
      der = CertFixtures.self_signed_der(cn: "x", spiffe: "spiffe://flezha.local/admins/alice")
      assert {:ok, "_system"} = Cert.namespace_from_peer(der)
    end

    test "no SAN at all falls back to _system" do
      der = CertFixtures.self_signed_der(cn: "x")
      assert {:ok, "_system"} = Cert.namespace_from_peer(der)
    end

    test "garbage cert falls back to _system" do
      assert {:ok, "_system"} = Cert.namespace_from_peer(<<0, 1, 2>>)
    end
  end
end
