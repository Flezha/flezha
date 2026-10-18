defmodule FlezhaTest.Transport.QuicNamespaceProofTest do
  @moduledoc """
  prove-the-namespace-and-recover-a-lost-admin §8.1: the namespace proof over a
  real QUIC handshake, with the certificates the controller generates.

  Two things only a live handshake can show. The peer's DER certificate reaches
  the connect path, which is the input `Flezha.AuthN.Cert` was written for and
  never had; and the certificate this deployment issues carries no SPIFFE `/ns/`
  cell, so the namespace comes from the presented credential and a runner
  holding another namespace's credential is refused on QUIC exactly as on UDS.
  """
  # async: false: writes Application env; Mimic global mode; the live transport.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_quic_proof", claim: true}],
    async: false

  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  @moduletag :quic
  @moduletag :capture_log

  setup :set_mimic_global_until_exit

  alias Flezha.AuthN.Cert
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.RunnerProof
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.PortPilot
  alias Flezha.Transport.Stream, as: TStream
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  @ns_a "quic-cred-orders"
  @ns_b "quic-cred-payments"

  setup do
    {:ok, _} = NamespaceStore.create(@ns_a)
    {:ok, _} = NamespaceStore.create(@ns_b)
    on_exit(fn -> FleetManager.stop_namespace(@ns_a) end)
    :ok
  end

  defp secret(namespace) do
    {raw, _hash} = AuthTestHelper.issue_runner_credential(namespace)
    raw
  end

  # A QUIC runner process, dialled at the controller's own listener with the
  # generated client pair. The caller owns closing the port.
  defp dial_quic(namespace, env) do
    command = PortPilot.launch_command(:rust, :test, namespace)
    {port, _cmd} = NodeTestHelper.open_runner_port(command, transport: :quic, env: env)
    port
  end

  test "the peer certificate reaches the connect path" do
    test_pid = self()

    stub(FleetManager, :connect_handler, fn stream_ref, _ns, _payload, transport ->
      send(test_pid, {:connect, transport, TStream.peer_certificate(stream_ref)})
      {:error, :not_admitted}
    end)

    port = dial_quic(@ns_a, [{"FLEZHA_NAMESPACE_SECRET", secret(@ns_a)}])

    try do
      assert_receive {:connect, :quic, der}, 10_000
      assert is_binary(der), "a QUIC control stream must answer the peer's DER certificate"

      # The certificate is the one this deployment issues: a real principal, and
      # no `/ns/` cell, which is why the credential is what proves the
      # namespace here.
      assert {:ok, principal} = Cert.principal_from_peer(der)
      assert principal =~ "flezha-runner"
      assert :none = RunnerProof.certificate_namespace(der)
    after
      NodeTestHelper.ensure_port_dead(port)
    end
  end

  test "a QUIC runner holding another namespace's credential is refused" do
    Telemetry.capture([:flezha, :transport, :connect_rejected], as: :rejected)

    port = dial_quic(@ns_a, [{"FLEZHA_NAMESPACE_SECRET", secret(@ns_b)}])

    try do
      assert_receive {:rejected, %{count: 1},
                      %{reason: {:namespace_not_entitled, @ns_a, @ns_b}, transport: :quic}},
                     10_000

      assert FleetManager.active_runners(@ns_a) == []
    after
      NodeTestHelper.ensure_port_dead(port)
    end
  end
end
