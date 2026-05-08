defmodule Flezha.Transport.CertGenerator do
  @moduledoc """
  Generates self-signed TLS certificates for QUIC transport.

  Creates a CA certificate, server certificate, and server key
  in the configured cert directory on first run.
  """

  require Logger

  @validity_days 365 * 10

  def ensure_certs do
    ca_cert_path = Application.get_env(:flezha, :ca_cert_path, "app_data/certs/ca.pem")

    server_cert_path =
      Application.get_env(:flezha, :server_cert_path, "app_data/certs/server.pem")

    server_key_path = Application.get_env(:flezha, :server_key_path, "app_data/certs/server.key")

    if File.exists?(ca_cert_path) and File.exists?(server_cert_path) and
         File.exists?(server_key_path) do
      Logger.info("QUIC certificates already exist, skipping generation.")
      :ok
    else
      Logger.info("Generating self-signed certificates for QUIC transport...")
      generate_certs(ca_cert_path, server_cert_path, server_key_path)
    end
  end

  defp generate_certs(ca_cert_path, server_cert_path, server_key_path) do
    # Ensure directory exists
    cert_dir = Path.dirname(ca_cert_path)
    File.mkdir_p!(cert_dir)

    # Generate CA key pair
    ca_key = :public_key.generate_key({:rsa, 2048, 65537})

    ca_cert =
      create_self_signed_cert(ca_key, "Flezha CA", is_ca: true)

    # Generate server key pair
    server_key = :public_key.generate_key({:rsa, 2048, 65537})

    server_cert =
      create_signed_cert(server_key, ca_key, ca_cert, "localhost")

    # Write files
    File.write!(ca_cert_path, pem_encode_cert(ca_cert))
    File.write!(server_cert_path, pem_encode_cert(server_cert))
    File.write!(server_key_path, pem_encode_key(server_key))

    Logger.info("QUIC certificates generated at #{cert_dir}/")
    :ok
  end

  defp create_self_signed_cert(key, cn, opts) do
    public_key = extract_public_key(key)
    is_ca = Keyword.get(opts, :is_ca, false)

    serial = :crypto.strong_rand_bytes(8) |> :binary.decode_unsigned()

    validity = validity_record()

    subject = {:rdnSequence, [[attribute_record(:commonName, cn)]]}

    extensions =
      if is_ca do
        [
          basic_constraints_extension(true),
          key_usage_extension([:keyCertSign, :cRLSign])
        ]
      else
        [
          basic_constraints_extension(false),
          key_usage_extension([:digitalSignature, :keyEncipherment])
        ]
      end

    tbs =
      {:TBSCertificate, :v3, serial, signature_algorithm(), subject, validity, subject,
       public_key_info(public_key), :asn1_NOVALUE, :asn1_NOVALUE, extensions}

    sign_certificate(tbs, key)
  end

  defp create_signed_cert(server_key, ca_key, ca_cert, hostname) do
    public_key = extract_public_key(server_key)

    serial = :crypto.strong_rand_bytes(8) |> :binary.decode_unsigned()

    validity = validity_record()

    ca_tbs = elem(ca_cert, 1)
    issuer = elem(ca_tbs, 6)
    subject = {:rdnSequence, [[attribute_record(:commonName, hostname)]]}

    san_extension = san_extension(hostname)

    extensions = [
      basic_constraints_extension(false),
      key_usage_extension([:digitalSignature, :keyEncipherment]),
      san_extension
    ]

    tbs =
      {:TBSCertificate, :v3, serial, signature_algorithm(), issuer, validity, subject,
       public_key_info(public_key), :asn1_NOVALUE, :asn1_NOVALUE, extensions}

    sign_certificate(tbs, ca_key)
  end

  defp sign_certificate(tbs, key) do
    tbs_der = :public_key.der_encode(:TBSCertificate, tbs)
    signature = :public_key.sign(tbs_der, :sha256, key)

    {:Certificate, tbs, signature_algorithm(), signature}
  end

  defp extract_public_key(rsa_private_key) do
    # RSAPrivateKey record: {type, version, modulus, publicExponent, ...}
    modulus = elem(rsa_private_key, 2)
    public_exponent = elem(rsa_private_key, 3)
    {:RSAPublicKey, modulus, public_exponent}
  end

  defp public_key_info(public_key) do
    der = :public_key.der_encode(:RSAPublicKey, public_key)

    algorithm =
      {:AlgorithmIdentifier, {1, 2, 840, 113_549, 1, 1, 1}, :asn1_NOVALUE}

    {:SubjectPublicKeyInfo, algorithm, der}
  end

  defp signature_algorithm do
    # SHA256 with RSA. Parameters absent (NULL); OTP 28's PKIX1Explicit
    # encoder rejects pre-encoded NULL bytes here, so emit `:asn1_NOVALUE`
    # and let the ASN.1 codec encode the absent-parameter form itself.
    {:AlgorithmIdentifier, {1, 2, 840, 113_549, 1, 1, 11}, :asn1_NOVALUE}
  end

  defp validity_record do
    now = :calendar.universal_time()
    not_after = add_days(now, @validity_days)

    {:Validity, {:utcTime, format_utc_time(now)}, {:utcTime, format_utc_time(not_after)}}
  end

  defp add_days({{year, month, day}, time}, days) do
    gregorian_days = :calendar.date_to_gregorian_days(year, month, day) + days
    {date, _} = {:calendar.gregorian_days_to_date(gregorian_days), time}
    {date, time}
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

  defp attribute_record(:commonName, value) do
    # AttributeTypeAndValue's `value` field is an OPEN TYPE. OTP 28's
    # PKIX1Explicit-2009 encoder rejects pre-encoded DER bytes here; pass
    # the typed value and let the codec encode it.
    {:AttributeTypeAndValue, {2, 5, 4, 3}, {:utf8String, value}}
  end

  defp basic_constraints_extension(is_ca) do
    {:Extension, {2, 5, 29, 19}, true,
     :public_key.der_encode(:BasicConstraints, {:BasicConstraints, is_ca, :asn1_NOVALUE})}
  end

  defp key_usage_extension(usages) do
    {:Extension, {2, 5, 29, 15}, true, :public_key.der_encode(:KeyUsage, usages)}
  end

  defp san_extension(hostname) do
    # SubjectAltName with dNSName and IP address
    san_entries = [
      {:dNSName, to_charlist(hostname)},
      {:iPAddress, [127, 0, 0, 1]}
    ]

    {:Extension, {2, 5, 29, 17}, false, :public_key.der_encode(:SubjectAltName, san_entries)}
  end

  defp pem_encode_cert(cert) do
    der = :public_key.der_encode(:Certificate, cert)
    :public_key.pem_encode([{:Certificate, der, :not_encrypted}])
  end

  defp pem_encode_key(key) do
    der = :public_key.der_encode(:RSAPrivateKey, key)
    :public_key.pem_encode([{:RSAPrivateKey, der, :not_encrypted}])
  end
end
