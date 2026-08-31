defmodule Flezha.AuthN.Cert do
  @moduledoc """
  X.509 cert → Principal / Namespace extraction.

  Implements the SPIFFE-SAN-wins-over-subject-DN rule from the
  platform-authn-authz design. The Principal is computed once at
  connection handshake and attached to transport state; downstream
  services read it from the `caller_context`.

  Inputs are DER-encoded cert binaries (what the transport gets
  from the TLS handshake).
  """

  @type principal :: binary()
  @type namespace :: binary()

  @doc """
  Extract a Principal from a DER-encoded peer cert.

  Precedence:

    1. SPIFFE URI SAN (`spiffe://<trust-domain>/...`) — wins when present.
    2. Subject DN normalised to RFC 4514 form.

  Returns `{:ok, principal}` or `{:error, reason}`. Never raises.
  """
  @spec principal_from_peer(binary()) :: {:ok, principal()} | {:error, term()}
  def principal_from_peer(der) when is_binary(der) do
    with {:ok, cert} <- decode_cert(der) do
      case spiffe_san(cert) do
        {:ok, uri} -> {:ok, uri}
        :error -> subject_dn_string(cert)
      end
    end
  end

  def principal_from_peer(_), do: {:error, :invalid_cert}

  @doc """
  Extract a Namespace from a DER-encoded peer cert. Returns the
  entity after `/ns/` in the SPIFFE SAN, or `"_system"` when no
  SAN is present (or no `/ns/` entity).
  """
  @spec namespace_from_peer(binary()) :: {:ok, namespace()}
  def namespace_from_peer(der) when is_binary(der) do
    case decode_cert(der) do
      {:ok, cert} ->
        case spiffe_san(cert) do
          {:ok, uri} -> {:ok, namespace_from_spiffe_uri(uri)}
          :error -> {:ok, "_system"}
        end

      _ ->
        {:ok, "_system"}
    end
  end

  def namespace_from_peer(_), do: {:ok, "_system"}

  # ----- decoding -----

  defp decode_cert(der) do
    {:ok, :public_key.pkix_decode_cert(der, :otp)}
  rescue
    _ -> {:error, :invalid_cert}
  catch
    _, _ -> {:error, :invalid_cert}
  end

  defp spiffe_san(cert) do
    case extensions(cert) do
      [] ->
        :error

      exts ->
        Enum.find_value(exts, :error, fn ext ->
          case ext do
            {:Extension, {2, 5, 29, 17}, _critical, names} ->
              find_spiffe_uri(names)

            _ ->
              false
          end
        end)
    end
  end

  defp find_spiffe_uri(names) when is_list(names) do
    Enum.find_value(names, fn
      {:uniformResourceIdentifier, uri} ->
        uri_str = to_string(uri)
        if String.starts_with?(uri_str, "spiffe://"), do: {:ok, uri_str}, else: nil

      _ ->
        nil
    end)
  end

  defp find_spiffe_uri(_), do: nil

  defp extensions(cert) do
    {:OTPCertificate, tbs, _, _} = cert
    {:OTPTBSCertificate, _, _, _, _, _, _, _, _, _, exts} = tbs
    List.wrap(exts)
  rescue
    _ -> []
  catch
    _, _ -> []
  end

  defp subject_dn_string(cert) do
    {:OTPCertificate, tbs, _, _} = cert
    {:OTPTBSCertificate, _, _, _, _, _, subject, _, _, _, _} = tbs
    {:rdnSequence, rdns} = subject
    {:ok, rdn_sequence_to_rfc4514(rdns)}
  rescue
    _ -> {:error, :subject_decode_failed}
  catch
    _, _ -> {:error, :subject_decode_failed}
  end

  # ----- RFC 4514 normalisation -----

  # Subject is a sequence of RelativeDistinguishedNames (each itself a
  # set of AttributeTypeAndValue). RFC 4514 reverses the sequence so the
  # most-specific RDN (CN) is first.
  defp rdn_sequence_to_rfc4514(rdns) do
    rdns
    |> Enum.reverse()
    |> Enum.map_join(",", &rdn_set_to_string/1)
  end

  defp rdn_set_to_string(rdn_set) do
    rdn_set
    |> Enum.map_join("+", &attr_type_and_value/1)
  end

  defp attr_type_and_value({:AttributeTypeAndValue, oid, value}) do
    type = attr_oid_to_short(oid)
    str = attr_value_to_string(value)
    "#{type}=#{escape_rfc4514(str)}"
  end

  # The common OIDs as documented in RFC 4514 §3 short-names table.
  defp attr_oid_to_short({2, 5, 4, 3}), do: "CN"
  defp attr_oid_to_short({2, 5, 4, 6}), do: "C"
  defp attr_oid_to_short({2, 5, 4, 7}), do: "L"
  defp attr_oid_to_short({2, 5, 4, 8}), do: "ST"
  defp attr_oid_to_short({2, 5, 4, 10}), do: "O"
  defp attr_oid_to_short({2, 5, 4, 11}), do: "OU"
  defp attr_oid_to_short({2, 5, 4, 5}), do: "SN"
  defp attr_oid_to_short({0, 9, 2342, 19_200_300, 100, 1, 25}), do: "DC"
  defp attr_oid_to_short({0, 9, 2342, 19_200_300, 100, 1, 1}), do: "UID"

  defp attr_oid_to_short(oid) when is_tuple(oid) do
    oid |> Tuple.to_list() |> Enum.join(".")
  end

  defp attr_value_to_string({:printableString, s}), do: to_string(s)
  defp attr_value_to_string({:utf8String, s}), do: to_string(s)
  defp attr_value_to_string({:teletexString, s}), do: to_string(s)
  defp attr_value_to_string({:bmpString, s}), do: to_string(s)
  defp attr_value_to_string({:universalString, s}), do: to_string(s)
  defp attr_value_to_string({:ia5String, s}), do: to_string(s)
  defp attr_value_to_string(s) when is_binary(s), do: s
  defp attr_value_to_string(s) when is_list(s), do: to_string(s)
  defp attr_value_to_string(other), do: inspect(other)

  defp escape_rfc4514(""), do: ""

  defp escape_rfc4514(s) do
    s
    |> String.replace("\\", "\\\\")
    |> String.replace(",", "\\,")
    |> String.replace("+", "\\+")
    |> String.replace("\"", "\\\"")
    |> String.replace("<", "\\<")
    |> String.replace(">", "\\>")
    |> String.replace(";", "\\;")
    |> escape_leading_special()
    |> escape_trailing_space()
  end

  defp escape_leading_special(<<c::utf8, rest::binary>>) when c in [?\s, ?#] do
    <<"\\", c::utf8, rest::binary>>
  end

  defp escape_leading_special(s), do: s

  defp escape_trailing_space(s) do
    if String.ends_with?(s, " ") do
      String.slice(s, 0..-2//1) <> "\\ "
    else
      s
    end
  end

  # ----- SPIFFE namespace extraction -----

  defp namespace_from_spiffe_uri(uri) do
    case URI.parse(uri) do
      %URI{path: path} when is_binary(path) ->
        entities =
          path
          |> String.split("/", trim: true)

        case entities do
          ["ns", ns | _] -> ns
          _ -> "_system"
        end

      _ ->
        "_system"
    end
  end
end
