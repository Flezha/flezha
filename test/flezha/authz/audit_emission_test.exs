defmodule Flezha.AuthZ.AuditEmissionTest do
  @moduledoc """
  Verifies that the mutating authn/authz primitives each emit exactly one
  `:platform_authz` event on the observability bus, carrying the documented
  `name:` atom and a `metadata.principal` / related identity field, and that a
  namespace create through the wire leaves exactly one record on the verb's own
  source. An entry crossing its expiry warning threshold is announced on the
  same source, so a lapse is observable before it happens.

  The foundation `:ring_buffer` sink is already registered in the
  test config; the shared auth harness registers the `:platform_authz`
  source and owns the ring-buffer polling.
  """

  # async: false: a shared on-disk root.

  use FlezhaTest.Case,
    fixtures: [
      {:auth_stores,
       prefix: "flz_authz_audit",
       stores: [
         Flezha.AuthZ.AuthState,
         Flezha.AuthZ.RoleStore,
         Flezha.AuthZ.NamespaceStore,
         Flezha.AuthZ.TokenStore
       ],
       register_source: true}
    ],
    async: false

  alias Flezha.AuthZ.{AuthState, Bootstrap, RoleStore, TokenStore}
  alias Flezha.Observability.{RingBufferSink, TestSinks}
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.AuthTestHelper

  @source :platform_authz

  test "TokenStore.issue/5 emits exactly one :token_issued event" do
    boundary = now_us()

    {:ok, _raw, _entry} =
      TokenStore.issue("alice", "_system", :admin, 60, created_by_principal: "bootstrap")

    [signal] = wait_for_events(:token_issued, boundary, 1)
    assert signal.source == @source
    assert signal.name == :token_issued
    assert signal.metadata.principal == "alice"
    assert signal.metadata.namespace == "_system"
    assert signal.metadata.role == :admin
    assert signal.metadata.created_by_principal == "bootstrap"
  end

  test "TokenStore.revoke/2 emits exactly one :token_revoked event" do
    {:ok, _raw, entry} = TokenStore.issue("alice", "_system", :admin, 60)

    boundary = now_us()
    {:ok, _} = TokenStore.revoke(entry.token_hash, revoked_by_principal: "alice")

    [signal] = wait_for_events(:token_revoked, boundary, 1)
    assert signal.source == @source
    assert signal.metadata.principal == "alice"
    assert signal.metadata.revoked_by_principal == "alice"
  end

  test "an entry inside its expiry window emits exactly one :token_expiring event" do
    {:ok, _raw, entry} = TokenStore.issue("alice", "_system", :admin, 60)

    boundary = now_us()
    assert TokenStore.check_expiring_now() == [entry.token_hash]

    [signal] = wait_for_events(:token_expiring, boundary, 1)
    assert signal.source == @source
    assert signal.metadata.principal == "alice"
    assert signal.metadata.expires_at == entry.expires_at

    # Announced once: a second sweep says nothing about an entry already named.
    second = now_us()
    assert TokenStore.check_expiring_now() == []

    assert RingBufferSink.query(@source, second) |> Enum.filter(&(&1.name == :token_expiring)) ==
             []
  end

  test "Bootstrap.claim/2 emits exactly one :bootstrap_completed event" do
    :ok = Bootstrap.check_and_write()
    {:ok, raw} = File.read(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])

    boundary = now_us()
    {:ok, %{principal: "alice"}} = Bootstrap.claim(raw, principal: "alice")

    [signal] = wait_for_events(:bootstrap_completed, boundary, 1)
    assert signal.source == @source
    assert signal.metadata.principal == "alice"
  end

  test "a namespace create through the wire leaves exactly one :namespace_service record" do
    # Enforced posture: the row authorizes for real, so the caller needs a
    # claimed auth state, a binding, and a verified token — a caller-supplied
    # principal would be ignored (§3.2).
    :ok = AuthState.set_enabled()
    :ok = RoleStore.bind("alice", "_system", :admin)
    {:ok, raw, _entry} = TokenStore.issue("alice", "_system", :admin, 60)

    boundary = now_us()

    output =
      ExUnit.CaptureIO.capture_io(fn ->
        CliManager.authz_render_namespace_create("tenant-a", "{}", token: raw)
      end)

    assert output =~ "namespace tenant-a created"
    TestSinks.settle(RingBufferSink)

    assert [signal] = RingBufferSink.query(:namespace_service, boundary)
    assert signal.name == :namespace_create
    assert signal.metadata.action == :"authz.namespace_create"
    assert signal.metadata.namespace == "tenant-a"
    assert signal.metadata.principal == "alice"
    assert signal.metadata.outcome == :ok
  end

  # ----- helpers -----

  defp now_us(), do: System.monotonic_time(:microsecond)

  defp wait_for_events(name, since_us, expected_count) do
    matches = AuthTestHelper.await_platform_authz_events(name, since_us)

    assert length(matches) == expected_count,
           "expected exactly #{expected_count} #{inspect(name)} event(s), got #{length(matches)}"

    matches
  end
end
