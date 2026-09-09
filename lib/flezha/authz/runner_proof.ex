defmodule Flezha.AuthZ.RunnerProof do
  @moduledoc """
  What a runner connection proves about the namespace it names.

  The connect frame keeps its namespace field, because a peer entitled to more
  than one namespace still has to say which it wants. What this module adds is
  that the field is checked rather than believed: the namespace the connection's
  own credential entitles is resolved here, and a frame naming another one is
  refused before any session is minted.

  A certificate can carry a namespace and a socket cannot, so each transport
  proves it differently and the answer is taken from the rule that applies:

      QUIC, cert carrying /ns/<name>  ->  the certificate's namespace
      QUIC, cert carrying no /ns/     ->  falls back to the secret, as UDS does
      UDS                             ->  the namespace of the presented secret
      native (in-node)                ->  co-residence, and the frame is taken

  The native row is the in-BEAM SDK, whose runner is code inside the
  controller's own release: it holds the distribution cookie and the whole node
  with it, `Flezha.Transport.Server.connect/2` refuses a handle owned by another
  BEAM, and asking such a peer for a secret proves nothing it could not have
  read off the disk the secret lives on. It is the transport reading of the
  `:internal` caller source, and like that source it does not extend to
  `_system`.

  The certificate reading is `Flezha.AuthN.Cert.namespace_from_peer/1`, which
  answers `_system` for a certificate with no SPIFFE `/ns/` entity — and
  `_system` is exactly what a runner may not be, so that answer is read as "this
  certificate names no namespace" and the secret is asked instead. The secret is
  a `:runner` entry of `Flezha.AuthZ.TokenStore`: the registry that already
  hashes, lists, rotates and revokes a credential, holding as many live runner
  entries per namespace as an operator issues.

  Nothing is proven while enforcement is off. The posture is the one gate:
  under `:open` the frame's namespace is taken as it always was, so a
  deployment that has not turned authorization on is not asked for a credential
  it was never issued.
  """

  alias Flezha.AuthN.Cert
  alias Flezha.AuthZ
  alias Flezha.AuthZ.Namespace
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Transport.Stream, as: TStream

  @secret_key "namespace_secret"

  @typedoc "The transport a connect arrived on."
  @type transport :: :uds | :quic | :native

  @typedoc """
  The namespace the connection may act in, and the credential that entitled it
  — the runner entry's hash, so revoking that entry can close this session, or
  `nil` when a certificate proved the namespace and no secret was presented.
  """
  @type t :: %{namespace: binary(), credential: binary() | nil}

  @doc """
  The namespace this connection is entitled to, or a refusal naming why.

  `claimed` is the frame's namespace field and `init_payload` the frame's own
  decoded JSON, which carries the secret under `"namespace_secret"`.
  """
  @spec prove(TStream.handle(), binary() | nil, map(), transport()) ::
          {:ok, t()} | {:error, term()}
  def prove(handle, claimed, init_payload, transport) do
    with :ok <- refuse_system(claimed) do
      resolve(handle, claimed, init_payload, transport)
    end
  end

  # A runner in `_system` would satisfy the `Namespace.system?(bound_ns)` arm of
  # the cross-namespace admin gate, so it is refused whatever it presents and
  # whatever the posture is. The credentialed, non-runner path is the only route
  # to a `_system` identity.
  defp refuse_system(claimed) do
    if Namespace.system?(claimed), do: {:error, :system_namespace_refused}, else: :ok
  end

  defp resolve(handle, claimed, init_payload, transport) do
    if enforced?(transport) do
      with {:ok, proof} <- proven_namespace(handle, init_payload, transport),
           do: agree(claimed, proof)
    else
      {:ok, %{namespace: claimed, credential: nil}}
    end
  end

  # Two ways a connect is not asked for a credential: the posture is not
  # enforcing at all, or the peer is in this BEAM and its co-residence is the
  # proof.
  defp enforced?(:native), do: false
  defp enforced?(_socket_transport), do: AuthZ.enabled?()

  # The certificate first, the secret second, and only QUIC is asked for a
  # certificate — a UDS peer completed no handshake, so there is nothing to
  # read. A deployment with SPIFFE naming distributes nothing; one with a CA but
  # no `/ns/` entity falls back rather than being refused for a naming
  # convention it never adopted.
  defp proven_namespace(handle, init_payload, :quic) do
    case certificate_namespace(TStream.peer_certificate(handle)) do
      {:ok, namespace} -> {:ok, %{namespace: namespace, credential: nil}}
      :none -> secret_namespace(init_payload)
    end
  end

  defp proven_namespace(_handle, init_payload, _no_handshake) do
    secret_namespace(init_payload)
  end

  @doc """
  The namespace a peer certificate names, or `:none` when it names none.

  `Flezha.AuthN.Cert.namespace_from_peer/1` answers `_system` for a certificate
  with no SPIFFE `/ns/` entity, for one with no SAN at all, and for input it
  cannot decode. A runner may not be `_system`, so every one of those answers
  reads here as "this certificate proves no namespace" and the secret is asked
  instead — the settled reading of the design's first open question, which is
  friendlier to a deployment that has a CA and no SPIFFE naming and gives up
  nothing, because such a certificate never carried a namespace to enforce.
  """
  @spec certificate_namespace(binary() | nil) :: {:ok, binary()} | :none
  def certificate_namespace(der) when is_binary(der) do
    with {:ok, namespace} <- Cert.namespace_from_peer(der),
         false <- Namespace.system?(namespace) do
      {:ok, namespace}
    else
      _no_namespace_in_cert -> :none
    end
  end

  def certificate_namespace(nil), do: :none

  defp secret_namespace(init_payload) do
    with secret when is_binary(secret) <- Map.get(init_payload, @secret_key),
         {:ok, %{role: :runner, namespace: namespace, token_hash: hash}} <-
           TokenStore.verify(secret) do
      {:ok, %{namespace: namespace, credential: hash}}
    else
      _no_runner_credential -> {:error, :namespace_not_proven}
    end
  end

  # The refusal names both namespaces, so an operator reading it knows which
  # credential was presented and which namespace was asked for.
  defp agree(claimed, %{namespace: proven} = proof) do
    if claimed == proven do
      {:ok, proof}
    else
      {:error, {:namespace_not_entitled, claimed, proven}}
    end
  end
end
