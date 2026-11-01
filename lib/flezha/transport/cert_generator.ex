defmodule Flezha.Transport.CertGenerator do
  @moduledoc """
  Generates self-signed TLS certificates for QUIC transport.

  Creates a CA certificate, server certificate, and server key
  in the configured cert directory on first run.
  """

  alias Flezha.Config
  alias Flezha.Layout.Table, as: Layout

  require Logger

  @validity_days 365 * 10

  def ensure_certs do
    ca_cert_path = Config.get(:ca_cert_path)

    server_cert_path =
      Config.get(:server_cert_path)

    server_key_path = Config.get(:server_key_path)

    client_cert_path =
      Config.get(:client_cert_path)

    client_key_path = Config.get(:client_key_path)

    paths = [ca_cert_path, server_cert_path, server_key_path, client_cert_path, client_key_path]

    if Enum.all?(paths, &File.exists?/1) do
      Logger.info("QUIC certificates already exist, skipping generation.")
      :ok
    else
      # The CA private key is never persisted, so a set missing any member
      # (typically the client pair, on an upgrade from a server-only layout)
      # is regenerated whole — a partial set cannot be extended.
      Logger.info("Generating self-signed certificates for QUIC transport...")

      generate_certs(
        ca_cert_path,
        server_cert_path,
        server_key_path,
        client_cert_path,
        client_key_path
      )
    end
  end

  defp generate_certs(
         ca_cert_path,
         server_cert_path,
         server_key_path,
         client_cert_path,
         client_key_path
       ) do
    cert_dir = Path.dirname(ca_cert_path)
    :ok = Layout.claim(:certs)

    # Generate CA key pair
    ca_key = :public_key.generate_key({:rsa, 2048, 65_537})

    ca_cert =
      create_self_signed_cert(ca_key, "Flezha CA", is_ca: true)

    # Generate server key pair
    server_key = :public_key.generate_key({:rsa, 2048, 65_537})

    server_cert =
      create_signed_cert(server_key, ca_key, ca_cert, "localhost", :server)

    # Client identity presented by local runners for mutual TLS.
    client_key = :public_key.generate_key({:rsa, 2048, 65_537})
    client_cert = create_signed_cert(client_key, ca_key, ca_cert, "flezha-runner", :client)

    # Write files
    File.write!(ca_cert_path, pem_encode_cert(ca_cert))
    File.write!(server_cert_path, pem_encode_cert(server_cert))
    write_private!(server_key_path, &:file.write(&1, pem_encode_key(server_key)))
    File.write!(client_cert_path, pem_encode_cert(client_cert))
    write_private!(client_key_path, &:file.write(&1, pem_encode_key(client_key)))

    Logger.info("QUIC certificates generated at #{cert_dir}/")
    :ok
  end

  @doc false
  # A key file is made owner-only while it is still empty, so no key byte is
  # ever readable by others. Erlang's open takes no creation mode, so the mode
  # is set on the path just opened, before `write` gets the descriptor.
  @spec write_private!(Path.t(), (:file.io_device() -> :ok | {:error, term()})) :: :ok
  def write_private!(path, write) do
    {:ok, fd} = :file.open(path, [:write, :raw, :binary])

    try do
      :ok = :file.change_mode(path, 0o600)
      :ok = write.(fd)
    after
      :file.close(fd)
    end
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

  defp create_signed_cert(leaf_key, ca_key, ca_cert, hostname, role)
       when role in [:server, :client] do
    public_key = extract_public_key(leaf_key)

    serial = :crypto.strong_rand_bytes(8) |> :binary.decode_unsigned()

    validity = validity_record()

    ca_tbs = elem(ca_cert, 1)
    issuer = elem(ca_tbs, 6)
    subject = {:rdnSequence, [[attribute_record(:commonName, hostname)]]}

    role_extensions =
      case role do
        :server -> [san_extension(hostname), extended_key_usage_extension(:server)]
        :client -> [extended_key_usage_extension(:client)]
      end

    extensions =
      [
        basic_constraints_extension(false),
        key_usage_extension([:digitalSignature, :keyEncipherment])
      ] ++ role_extensions

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

    # PKIX (RFC 3279) requires `rsaEncryption`'s `parameters` field in
    # SubjectPublicKeyInfo's AlgorithmIdentifier to be encoded as ASN.1
    # NULL — not absent. OTP's `:asn1_NOVALUE` produces *absent* for the
    # ANY-typed parameters field, which is technically a valid ASN.1
    # encoding but rustls' `with_single_cert` derives the cert's expected
    # SPKI from the private key with NULL parameters and compares DER
    # bytes; the mismatch surfaces as `KeyMismatch`. Pass `<<5, 0>>` —
    # the DER bytes of ASN.1 NULL — as the open-typed parameters value
    # so OTP's PKIX1Explicit-2009 codec emits it verbatim.
    algorithm =
      {:AlgorithmIdentifier, {1, 2, 840, 113_549, 1, 1, 1}, <<5, 0>>}

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
    # AttributeTypeAndValue.value is an OPEN TYPE; OTP's PKIX1Explicit
    # codec dispatches on the type OID and expects the typed value
    # (X520CommonName CHOICE).
    {:AttributeTypeAndValue, {2, 5, 4, 3}, {:utf8String, value}}
  end

  defp basic_constraints_extension(is_ca) do
    {:Extension, {2, 5, 29, 19}, true,
     :public_key.der_encode(:BasicConstraints, {:BasicConstraints, is_ca, :asn1_NOVALUE})}
  end

  defp key_usage_extension(usages) do
    {:Extension, {2, 5, 29, 15}, true, :public_key.der_encode(:KeyUsage, usages)}
  end

  # webpki rejects a leaf presented for a role its EKU does not name, so the
  # server/client split is explicit rather than left absent.
  defp extended_key_usage_extension(role) do
    oid =
      case role do
        # id-kp-serverAuth / id-kp-clientAuth
        :server -> {1, 3, 6, 1, 5, 5, 7, 3, 1}
        :client -> {1, 3, 6, 1, 5, 5, 7, 3, 2}
      end

    {:Extension, {2, 5, 29, 37}, false, :public_key.der_encode(:ExtKeyUsageSyntax, [oid])}
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
    # PKCS#8 PrivateKeyInfo wrapping. rustls' `with_single_cert` (with the
    # `ring` provider) needs PKCS#8 with explicit NULL `parameters` on the
    # `rsaEncryption` algorithm; OTP's `der_encode/2` of `:PrivateKeyInfo`
    # emits parameters as *absent* when given `:asn1_NOVALUE` (and rejects
    # the atom `:NULL` and pre-encoded `<<5, 0>>` forms), which ring then
    # rejects with "failed to parse private key as RSA, ECDSA, or EdDSA".
    # Hand-encode the PKCS#8 wrapper as raw DER around OTP's PKCS#1 DER:
    # the wrapper is small and avoids the codec's missing
    # parameter-defaulting rule for `OneAsymmetricKey`.
    rsa_der = :public_key.der_encode(:RSAPrivateKey, key)
    der = encode_pkcs8_rsa(rsa_der)
    :public_key.pem_encode([{:PrivateKeyInfo, der, :not_encrypted}])
  end

  # PKCS#8 PrivateKeyInfo (RFC 5208) for an RSA key. Hand-rolled so we
  # can emit the algorithm's `parameters` as ASN.1 NULL (`05 00`), which
  # OTP's `:PrivateKeyInfo` codec does not on its own.
  defp encode_pkcs8_rsa(rsa_der) do
    # AlgorithmIdentifier ::= SEQUENCE { algorithm OID, parameters NULL }
    rsa_oid_der = <<6, 9, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x01>>
    null_der = <<5, 0>>
    alg_inner = rsa_oid_der <> null_der
    alg_seq = <<0x30>> <> der_length(byte_size(alg_inner)) <> alg_inner

    version_der = <<2, 1, 0>>
    octet_string_der = <<4>> <> der_length(byte_size(rsa_der)) <> rsa_der

    inner = version_der <> alg_seq <> octet_string_der
    <<0x30>> <> der_length(byte_size(inner)) <> inner
  end

  defp der_length(n) when n < 0x80, do: <<n>>

  defp der_length(n) when n < 0x100, do: <<0x81, n>>

  defp der_length(n) when n < 0x10000, do: <<0x82, n::16>>

  defp der_length(n) when n < 0x1000000, do: <<0x83, n::24>>
end
