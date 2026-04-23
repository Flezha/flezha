defmodule Flezha.Plugin.Installer.SignatureTest do
  @moduledoc """
  Tests for Ed25519 signature verification and enforcement gating.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Installer.Signature

  setup do
    # Generate a test keypair and produce a signature over a sample CHECKSUMS blob.
    {pub, priv} = :crypto.generate_key(:eddsa, :ed25519)
    msg = "abc  manifest.exs\n"
    sig = :crypto.sign(:eddsa, :none, msg, [priv, :ed25519])
    {:ok, pub: pub, priv: priv, msg: msg, sig: sig}
  end

  test "valid signature accepted with raw key", %{pub: pub, msg: msg, sig: sig} do
    assert :ok = Signature.verify(msg, sig, [pub])
  end

  test "invalid signature rejected", %{pub: pub, msg: msg} do
    bad_sig = :crypto.strong_rand_bytes(64)
    assert {:error, :signature_invalid} = Signature.verify(msg, bad_sig, [pub])
  end

  test "wrong key rejected", %{msg: msg, sig: sig} do
    {other_pub, _} = :crypto.generate_key(:eddsa, :ed25519)
    assert {:error, :signature_invalid} = Signature.verify(msg, sig, [other_pub])
  end

  test "short signature rejected", %{msg: msg, pub: pub} do
    assert {:error, :signature_invalid} = Signature.verify(msg, "short", [pub])
  end

  test "empty trusted keys returns no_trusted_keys", %{msg: msg, sig: sig} do
    assert {:error, :no_trusted_keys} = Signature.verify(msg, sig, [])
  end

  test "multiple keys — any match accepts", %{pub: pub, msg: msg, sig: sig} do
    {other_pub, _} = :crypto.generate_key(:eddsa, :ed25519)
    assert :ok = Signature.verify(msg, sig, [other_pub, pub])
  end

  test "gate permissive path emits telemetry" do
    handler = "unsigned-#{System.unique_integer([:positive])}"
    me = self()

    :telemetry.attach(
      handler,
      [:flezha, :plugin_installer, :unsigned_install],
      fn _n, _m, meta, _ -> send(me, {:telemetry_meta, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert :ok =
             Signature.gate(
               signature_present?: false,
               require_signature: false,
               allow_unsigned: true,
               plugin_id: "x"
             )

    assert_receive {:telemetry_meta, %{plugin_id: "x"}}, 500
  end

  test "gate rejects missing signature when require_signature true" do
    assert {:error, :signature_required} =
             Signature.gate(
               signature_present?: false,
               require_signature: true,
               allow_unsigned: true
             )
  end

  test "gate rejects missing signature when allow_unsigned false" do
    assert {:error, :unsigned_not_allowed} =
             Signature.gate(
               signature_present?: false,
               require_signature: false,
               allow_unsigned: false
             )
  end

  test "gate accepts present signature regardless of policy" do
    assert :ok =
             Signature.gate(
               signature_present?: true,
               require_signature: true,
               allow_unsigned: false
             )
  end

  test "PEM-encoded key accepted", %{pub: pub, msg: msg, sig: sig} do
    # Build an Ed25519 SubjectPublicKeyInfo DER by hand:
    #   SEQUENCE {
    #     SEQUENCE { OID 1.3.101.112 }
    #     BIT STRING (0 unused bits, then the 32-byte public point)
    #   }
    oid_der = <<0x06, 0x03, 0x2B, 0x65, 0x70>>
    alg_seq = <<0x30, byte_size(oid_der)>> <> oid_der
    bit_string = <<0x03, byte_size(pub) + 1, 0x00>> <> pub
    inner = alg_seq <> bit_string
    spki_der = <<0x30, byte_size(inner)>> <> inner

    pem = :public_key.pem_encode([{:SubjectPublicKeyInfo, spki_der, :not_encrypted}])
    assert :ok = Signature.verify(msg, sig, [pem])
  end
end
