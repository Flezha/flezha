defmodule Flezha.Plugin.Installer.Signature do
  @moduledoc """
  Ed25519 signatures of plugin packages: a raw 64-byte signature over the bytes
  of `CHECKSUMS.sha256`.

  Trusted public keys may be a raw 32-byte binary or a PEM string
  (`-----BEGIN PUBLIC KEY-----`). `check/3` is the one signature policy, applied
  at install and at every boot; `sign/2` is what `mix flezha.plugin.package
  --sign` writes with.
  """

  @type key :: binary() | String.t()

  @ed25519 {1, 3, 101, 112}

  @doc """
  The signature policy for the package extracted in `dir`: `{:ok, true}` for a
  signature a trusted key verifies, `{:ok, false}` for an accepted unsigned
  package. A present signature no trusted key verifies is refused whatever the
  setting, and with signatures required and no trusted key nothing verifies.
  """
  @spec check(Path.t(), boolean(), [key()]) :: {:ok, boolean()} | {:error, atom()}
  def check(dir, require_signature, trusted_keys) do
    signature = Path.join(dir, "signature.sig")

    cond do
      require_signature and trusted_keys == [] ->
        {:error, :trust_store_empty}

      File.regular?(signature) ->
        msg = File.read!(Path.join(dir, "CHECKSUMS.sha256"))

        with :ok <- verify(msg, File.read!(signature), trusted_keys), do: {:ok, true}

      require_signature ->
        {:error, :signature_required}

      true ->
        {:ok, false}
    end
  end

  @doc "Verify `signature` over `msg` against each key until one validates."
  @spec verify(binary(), binary(), [key()]) :: :ok | {:error, :signature_invalid}
  def verify(msg, signature, trusted_keys)
      when is_binary(msg) and is_binary(signature) and is_list(trusted_keys) do
    verified? =
      byte_size(signature) == 64 and
        Enum.any?(trusted_keys, fn key ->
          case normalize_key(key) do
            {:ok, raw} -> verify_one(msg, signature, raw)
            {:error, _} -> false
          end
        end)

    if verified?, do: :ok, else: {:error, :signature_invalid}
  end

  @doc "Sign `msg` with the Ed25519 private key in `pem` (PKCS#8)."
  @spec sign(binary(), binary()) :: {:ok, binary()} | {:error, :not_an_ed25519_private_key}
  def sign(msg, pem) when is_binary(msg) and is_binary(pem) do
    with [entry] <- :public_key.pem_decode(pem),
         {:ECPrivateKey, _, private, {:namedCurve, @ed25519}, _, _} <-
           :public_key.pem_entry_decode(entry) do
      {:ok, :crypto.sign(:eddsa, :none, msg, [private, :ed25519])}
    else
      _ -> {:error, :not_an_ed25519_private_key}
    end
  rescue
    _ -> {:error, :not_an_ed25519_private_key}
  end

  ## Private

  defp verify_one(msg, signature, public_key) do
    :crypto.verify(:eddsa, :none, msg, signature, [public_key, :ed25519])
  rescue
    _ -> false
  catch
    _, _ -> false
  end

  # Raw 32-byte binary: accept as-is.
  defp normalize_key(raw) when is_binary(raw) and byte_size(raw) == 32,
    do: {:ok, raw}

  # Everything else: try to decode as PEM.
  defp normalize_key(pem) when is_binary(pem) do
    case :public_key.pem_decode(pem) do
      [] ->
        {:error, :not_pem}

      entries ->
        entries
        |> Enum.map(&decode_pem_entry/1)
        |> Enum.find(&match?({:ok, _}, &1))
        |> case do
          nil -> {:error, :unsupported_pem}
          {:ok, raw} -> {:ok, raw}
        end
    end
  rescue
    _ -> {:error, :pem_decode_failed}
  end

  defp normalize_key(_), do: {:error, :unsupported_key_format}

  defp decode_pem_entry({:SubjectPublicKeyInfo, _, _} = entry) do
    case :public_key.pem_entry_decode(entry) do
      {{:ECPoint, raw}, _params} when is_binary(raw) and byte_size(raw) == 32 ->
        {:ok, raw}

      {:ed_pub, :ed25519, raw} when is_binary(raw) and byte_size(raw) == 32 ->
        {:ok, raw}

      _ ->
        {:error, :unsupported_spki}
    end
  rescue
    _ -> {:error, :spki_decode_failed}
  end

  defp decode_pem_entry(_), do: {:error, :unsupported_pem_entry}
end
