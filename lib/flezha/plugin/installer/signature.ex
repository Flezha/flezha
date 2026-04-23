defmodule Flezha.Plugin.Installer.Signature do
  @moduledoc """
  Ed25519 signature verification for plugin packages.

  A signature is a raw 64-byte Ed25519 signature over the UTF-8 bytes of
  `CHECKSUMS.sha256`. Trusted public keys may be supplied in either form:

    * a raw 32-byte binary (the OpenSSL raw public key output), or
    * a PEM-encoded string (e.g. `-----BEGIN PUBLIC KEY-----`), which is
      decoded via `:public_key` and converted to its 32-byte curve point.

  `verify/3` returns `:ok` if any trusted key verifies; `gate/3` applies
  the spec's enforcement decision tree (`require_signature`,
  `allow_unsigned`) and emits telemetry for accepted unsigned installs.
  """

  require Logger

  @type key :: binary() | String.t()

  @doc """
  Verify `signature` (64 bytes) against `msg` using each key in
  `trusted_keys` until one validates. Returns `:ok` or
  `{:error, :signature_invalid}`.
  """
  @spec verify(binary(), binary(), [key()]) :: :ok | {:error, :signature_invalid | :no_trusted_keys}
  def verify(msg, signature, trusted_keys)
      when is_binary(msg) and is_binary(signature) and is_list(trusted_keys) do
    cond do
      byte_size(signature) != 64 ->
        {:error, :signature_invalid}

      trusted_keys == [] ->
        {:error, :no_trusted_keys}

      true ->
        trusted_keys
        |> Enum.map(&normalize_key/1)
        |> Enum.filter(&match?({:ok, _}, &1))
        |> Enum.any?(fn {:ok, raw} -> verify_one(msg, signature, raw) end)
        |> case do
          true -> :ok
          false -> {:error, :signature_invalid}
        end
    end
  end

  @doc """
  Apply the spec's signature-enforcement decision tree and return
  `:ok | {:error, reason}`. Emits `[:flezha, :plugin_installer, :unsigned_install]`
  when an unsigned package is accepted under permissive config.

  `opts` carries:
    * `:signature_present?` — boolean
    * `:require_signature` — boolean (from config)
    * `:allow_unsigned` — boolean (from config)
    * `:plugin_id` — binary (for telemetry)
  """
  @spec gate(keyword()) :: :ok | {:error, :signature_required | :unsigned_not_allowed}
  def gate(opts) do
    signature_present? = Keyword.fetch!(opts, :signature_present?)
    require_signature = Keyword.fetch!(opts, :require_signature)
    allow_unsigned = Keyword.fetch!(opts, :allow_unsigned)
    plugin_id = Keyword.get(opts, :plugin_id, nil)

    cond do
      signature_present? ->
        :ok

      require_signature ->
        {:error, :signature_required}

      not allow_unsigned ->
        {:error, :unsigned_not_allowed}

      true ->
        :telemetry.execute(
          [:flezha, :plugin_installer, :unsigned_install],
          %{count: 1},
          %{plugin_id: plugin_id}
        )

        :ok
    end
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
    try do
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
  end

  defp normalize_key(_), do: {:error, :unsupported_key_format}

  defp decode_pem_entry({:SubjectPublicKeyInfo, _, _} = entry) do
    try do
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
  end

  defp decode_pem_entry(_), do: {:error, :unsupported_pem_entry}
end
