defmodule Flezha.Test.CertFixtures do
  @moduledoc """
  Test helpers for generating X.509 certificates with controlled
  subject DNs and optional SPIFFE URI SANs.

  Uses the same ASN.1 record shapes as `Flezha.Transport.CertGenerator`
  (which is the production pattern known to round-trip through OTP's
  PKIX codec without `:asn1_NOVALUE` / NULL-parameter pitfalls).
  """

  @doc """
  Build a self-signed RSA cert (DER-encoded) with the given options:

    * `:cn` — CommonName (required)
    * `:o` — Organization (optional)
    * `:spiffe` — SPIFFE URI to embed in SubjectAltName (optional)
  """
  @spec self_signed_der(keyword()) :: binary()
  def self_signed_der(opts) do
    key = :public_key.generate_key({:rsa, 2048, 65537})

    pub_key = extract_public_key(key)

    cn = Keyword.fetch!(opts, :cn)
    o = Keyword.get(opts, :o)
    spiffe = Keyword.get(opts, :spiffe)

    serial = :crypto.strong_rand_bytes(8) |> :binary.decode_unsigned()

    # X.509 DN encoding goes most-general → most-specific, so O
    # comes BEFORE CN in the rdnSequence. RFC 4514 then reverses to
    # produce the "CN=..,O=.." display order.
    subject_rdns =
      Enum.filter(
        [
          if(o, do: [{:AttributeTypeAndValue, {2, 5, 4, 10}, {:utf8String, o}}]),
          if(cn, do: [{:AttributeTypeAndValue, {2, 5, 4, 3}, {:utf8String, cn}}])
        ],
        & &1
      )

    subject = {:rdnSequence, subject_rdns}

    extensions =
      [
        basic_constraints_extension(),
        key_usage_extension()
      ]
      |> maybe_append(spiffe && spiffe_san_extension(spiffe))

    tbs = {
      :TBSCertificate,
      :v3,
      serial,
      sig_alg(),
      subject,
      validity_record(),
      subject,
      public_key_info(pub_key),
      :asn1_NOVALUE,
      :asn1_NOVALUE,
      extensions
    }

    tbs_der = :public_key.der_encode(:TBSCertificate, tbs)
    signature = :public_key.sign(tbs_der, :sha256, key)

    cert = {:Certificate, tbs, sig_alg(), signature}
    :public_key.der_encode(:Certificate, cert)
  end

  defp extract_public_key(rsa_private_key) do
    modulus = elem(rsa_private_key, 2)
    public_exponent = elem(rsa_private_key, 3)
    {:RSAPublicKey, modulus, public_exponent}
  end

  defp public_key_info(public_key) do
    der = :public_key.der_encode(:RSAPublicKey, public_key)
    algorithm = {:AlgorithmIdentifier, {1, 2, 840, 113_549, 1, 1, 1}, <<5, 0>>}
    {:SubjectPublicKeyInfo, algorithm, der}
  end

  defp sig_alg do
    {:AlgorithmIdentifier, {1, 2, 840, 113_549, 1, 1, 11}, :asn1_NOVALUE}
  end

  defp validity_record do
    now = :calendar.universal_time()
    not_after = add_years(now, 5)

    {:Validity, {:utcTime, format_utc_time(now)}, {:utcTime, format_utc_time(not_after)}}
  end

  defp add_years({{year, month, day}, time}, years) do
    {{year + years, month, day}, time}
  end

  defp format_utc_time({{year, month, day}, {hour, minute, second}}) do
    short_year = rem(year, 100)

    :io_lib.format(~c"~2..0B~2..0B~2..0B~2..0B~2..0B~2..0BZ", [
      short_year,
      month,
      day,
      hour,
      minute,
      second
    ])
    |> IO.iodata_to_binary()
    |> to_charlist()
  end

  defp basic_constraints_extension do
    {:Extension, {2, 5, 29, 19}, true,
     :public_key.der_encode(:BasicConstraints, {:BasicConstraints, false, :asn1_NOVALUE})}
  end

  defp key_usage_extension do
    {:Extension, {2, 5, 29, 15}, true,
     :public_key.der_encode(:KeyUsage, [:digitalSignature, :keyEncipherment])}
  end

  defp spiffe_san_extension(uri) when is_binary(uri) do
    entries = [{:uniformResourceIdentifier, to_charlist(uri)}]

    {:Extension, {2, 5, 29, 17}, false, :public_key.der_encode(:SubjectAltName, entries)}
  end

  defp maybe_append(list, nil), do: list
  defp maybe_append(list, false), do: list
  defp maybe_append(list, extra), do: list ++ [extra]
end
