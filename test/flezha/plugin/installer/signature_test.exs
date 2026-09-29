defmodule Flezha.Plugin.Installer.SignatureTest do
  @moduledoc "\"Ed25519 signature verification\": the one policy, at install and at boot."

  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  alias Flezha.Plugin.Installer.Signature

  setup %{tmp: tmp} do
    File.write!(Path.join(tmp, "CHECKSUMS.sha256"), "abc  manifest.exs\n")
    {private_pem, public_pem} = keypair()
    {:ok, dir: tmp, private: private_pem, public: public_pem}
  end

  defp keypair() do
    {:ECPrivateKey, _, _, curve, public, _} =
      key = :public_key.generate_key({:namedCurve, :ed25519})

    {:public_key.pem_encode([:public_key.pem_entry_encode(:PrivateKeyInfo, key)]),
     :public_key.pem_encode([
       :public_key.pem_entry_encode(:SubjectPublicKeyInfo, {{:ECPoint, public}, curve})
     ])}
  end

  defp sign!(dir, pem) do
    {:ok, signature} = Signature.sign(File.read!(Path.join(dir, "CHECKSUMS.sha256")), pem)
    assert byte_size(signature) == 64
    File.write!(Path.join(dir, "signature.sig"), signature)
  end

  test "a signature a trusted key verifies is accepted", %{
    dir: dir,
    private: private,
    public: public
  } do
    sign!(dir, private)
    assert {:ok, true} = Signature.check(dir, false, [public])
    assert {:ok, true} = Signature.check(dir, true, [public])
  end

  test "a present signature no trusted key verifies is refused whatever the setting",
       %{dir: dir, public: public} do
    {other, _} = keypair()
    sign!(dir, other)
    assert {:error, :signature_invalid} = Signature.check(dir, false, [public])
    assert {:error, :signature_invalid} = Signature.check(dir, false, [])
  end

  test "an unsigned package is accepted in permissive mode and refused in required mode",
       %{dir: dir, public: public} do
    assert {:ok, false} = Signature.check(dir, false, [])
    assert {:error, :signature_required} = Signature.check(dir, true, [public])
  end

  test "required signatures with an empty trust store refuse every package", %{
    dir: dir,
    private: private
  } do
    assert {:error, :trust_store_empty} = Signature.check(dir, true, [])
    sign!(dir, private)
    assert {:error, :trust_store_empty} = Signature.check(dir, true, [])
  end

  test "sign/2 refuses a key that is not an Ed25519 private key", %{public: public} do
    assert {:error, :not_an_ed25519_private_key} = Signature.sign("msg", public)
    assert {:error, :not_an_ed25519_private_key} = Signature.sign("msg", "junk")
  end
end
