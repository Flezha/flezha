defmodule FlezhaTest.Runner.RunnerNamespaceCredentialTest do
  @moduledoc """
  prove-the-namespace-and-recover-a-lost-admin §8: the namespace proof driven
  through real runner processes rather than through the connect function.

  A runner presenting a namespace's credential is admitted to that namespace and
  refused every other; a namespace holds more than one live credential, so a
  rotation is issue, roll, retire with nothing dropped; and revoking one closes
  the sessions it authenticated, because a credential with no expiry leaves
  nothing that would.
  """
  # async: false: writes Application env; enforcement on; the live controller and its fleet.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_runner_credential", claim: true}],
    async: false

  @moduletag :capture_log

  alias Flezha.AuthZ
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.TokenStore
  alias Flezha.Runner.FleetManager
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @ns_a "cred-orders"
  @ns_b "cred-payments"

  setup do
    assert AuthZ.enabled?()
    {:ok, _} = NamespaceStore.create(@ns_a)
    {:ok, _} = NamespaceStore.create(@ns_b)

    on_exit(fn ->
      FleetManager.stop_namespace(@ns_a)
      FleetManager.stop_namespace(@ns_b)
    end)

    {:ok,
     secret_a: AuthTestHelper.issue_runner_credential(@ns_a),
     secret_b: AuthTestHelper.issue_runner_credential(@ns_b)}
  end

  defp start_on(namespace, {secret, _hash}) do
    NodeTestHelper.start_runner(namespace, env: [{"FLEZHA_NAMESPACE_SECRET", secret}])
  end

  test "a runner is admitted to the namespace its credential entitles", %{secret_a: secret_a} do
    runner = start_on(@ns_a, secret_a)
    assert runner in FleetManager.active_runners(@ns_a)
  end

  test "a runner presenting another namespace's credential is refused", %{secret_b: secret_b} do
    MessageTestHelper.forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)

    # The runner names `@ns_a` and holds `@ns_b`'s credential: it never connects,
    # so the refusal is read off the counted signal rather than off a session.
    assert_raise RuntimeError, fn ->
      NodeTestHelper.start_runner(@ns_a,
        env: [{"FLEZHA_NAMESPACE_SECRET", elem(secret_b, 0)}],
        connect_timeout: 4_000
      )
    end

    assert_receive {:rejected, %{count: 1},
                    %{reason: {:namespace_not_entitled, @ns_a, @ns_b}, transport: :uds}},
                   1_000

    assert FleetManager.active_runners(@ns_a) == []
  end

  test "a rolling rotation refuses nothing and retires the old credential", %{
    secret_a: {_, first_hash} = first
  } do
    running = start_on(@ns_a, first)
    assert running in FleetManager.active_runners(@ns_a)

    # Issue the second, roll one runner onto it: both credentials are live, so
    # nothing had to be disconnected to make room for the new one.
    second = AuthTestHelper.issue_runner_credential(@ns_a)
    rolled = start_on(@ns_a, second)

    assert running in FleetManager.active_runners(@ns_a)
    assert rolled in FleetManager.active_runners(@ns_a)

    # Retire the first. Its own session goes with it (that is the revocation
    # rule); the session on the second credential is untouched.
    :ok = NodeTestHelper.stop_runner(@ns_a, running)
    {:ok, _} = TokenStore.revoke(String.slice(first_hash, 0, 12))

    assert {:error, :namespace_not_proven} =
             Flezha.AuthZ.RunnerProof.prove(
               nil,
               @ns_a,
               %{"namespace_secret" => elem(first, 0)},
               :uds
             )

    assert rolled in FleetManager.active_runners(@ns_a)
  end

  test "revoking a credential closes the session it authenticated", %{
    secret_a: {_, hash} = secret_a
  } do
    runner = start_on(@ns_a, secret_a)
    assert runner in FleetManager.active_runners(@ns_a)

    {:ok, _} = TokenStore.revoke(String.slice(hash, 0, 12))

    assert NodeTestHelper.wait_until(
             fn -> runner not in FleetManager.active_runners(@ns_a) end,
             5_000,
             25
           ),
           "the revoked credential's session must be closed without waiting for a reconnect"
  end

  test "sessions on a namespace's other live credential are untouched", %{
    secret_a: {_, first_hash} = first
  } do
    second = AuthTestHelper.issue_runner_credential(@ns_a)
    on_first = start_on(@ns_a, first)
    on_second = start_on(@ns_a, second)

    {:ok, _} = TokenStore.revoke(String.slice(first_hash, 0, 12))

    assert NodeTestHelper.wait_until(
             fn -> on_first not in FleetManager.active_runners(@ns_a) end,
             5_000,
             25
           )

    assert on_second in FleetManager.active_runners(@ns_a)
  end
end
