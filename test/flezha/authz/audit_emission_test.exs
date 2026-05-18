defmodule Flezha.AuthZ.AuditEmissionTest do
  @moduledoc """
  Verifies that the four mutating authn/authz operations each emit
  exactly one `:platform_authz` event on the observability bus,
  carrying the documented `name:` atom and a `metadata.principal` /
  related identity field.

  The foundation `:ring_buffer` sink is already registered in the
  test config, so we subscribe by querying it directly after each
  emit. The `:platform_authz` source is registered explicitly here
  because the `Flezha.AuthZ.Supervisor` (which normally registers
  it via `SourceRegistration`) is gated off in tests by
  `application_managed: false`.
  """

  use ExUnit.Case, async: false

  alias Flezha.AuthZ.{AuthState, Bootstrap, NamespaceStore, RoleStore, TokenStore}
  alias Flezha.Observability.{Registry, RingBufferSink}
  alias Flezha.Service.CliManager

  @source :platform_authz

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_authz_audit_#{System.unique_integer([:positive])}")
    auth_dir = Path.join(tmp, "auth")
    File.mkdir_p!(auth_dir)
    File.chmod!(tmp, 0o700)
    File.chmod!(auth_dir, 0o700)

    prev = Application.get_env(:flezha, :authn_authz, [])

    Application.put_env(
      :flezha,
      :authn_authz,
      prev
      |> Keyword.put(:auth_dir, auth_dir)
      |> Keyword.put(:bootstrap_token_path, Path.join(auth_dir, "bootstrap.token"))
      |> Keyword.put(:enabled, true)
    )

    stop_if_running([AuthState, RoleStore, NamespaceStore, TokenStore])
    {:ok, _} = AuthState.start_link([])
    {:ok, _} = RoleStore.start_link([])
    {:ok, _} = NamespaceStore.start_link([])
    {:ok, _} = TokenStore.start_link([])

    # Register :platform_authz so the RingBufferSink creates a
    # dedicated table for it and we can query by source.
    _ =
      Registry.register_source(%{
        name: @source,
        kinds: [:event, :metric],
        flavour: :declarative,
        cardinality_budget: 256
      })

    # Give the RingBufferSink a tick to observe the registration.
    Process.sleep(50)

    on_exit(fn ->
      stop_if_running([AuthState, RoleStore, NamespaceStore, TokenStore])
      _ = Registry.unregister_source(@source)
      Application.put_env(:flezha, :authn_authz, prev)
      File.rm_rf!(tmp)
    end)

    {:ok, auth_dir: auth_dir}
  end

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

  test "Bootstrap.claim/2 emits exactly one :bootstrap_completed event" do
    :ok = Bootstrap.check_and_write()
    {:ok, raw} = File.read(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])

    boundary = now_us()
    {:ok, %{principal: "alice"}} = Bootstrap.claim(raw, principal: "alice")

    [signal] = wait_for_events(:bootstrap_completed, boundary, 1)
    assert signal.source == @source
    assert signal.metadata.principal == "alice"
  end

  test "CliManager.namespace_create/3 emits exactly one :namespace_created event" do
    :ok = RoleStore.bind("alice", "_system", :admin)

    boundary = now_us()

    {:ok, _entry} =
      CliManager.namespace_create("tenant-a", %{},
        principal: "alice",
        namespace: "_system",
        role: :admin
      )

    [signal] = wait_for_events(:namespace_created, boundary, 1)
    assert signal.source == @source
    assert signal.metadata.namespace == "tenant-a"
    assert signal.metadata.principal == "alice"
  end

  # ----- helpers -----

  defp now_us(), do: System.monotonic_time(:microsecond)

  defp wait_for_events(name, since_us, expected_count, attempts \\ 20) do
    Enum.reduce_while(1..attempts, [], fn _i, _acc ->
      matches =
        RingBufferSink.query(@source, since_us)
        |> Enum.filter(&(&1.name == name))

      if length(matches) >= expected_count do
        {:halt, matches}
      else
        Process.sleep(25)
        {:cont, matches}
      end
    end)
    |> case do
      matches when is_list(matches) ->
        assert length(matches) == expected_count,
               "expected exactly #{expected_count} #{inspect(name)} event(s), got #{length(matches)}"

        matches
    end
  end

  defp stop_if_running(mods) when is_list(mods), do: Enum.each(mods, &stop_if_running/1)

  defp stop_if_running(mod) do
    case Process.whereis(mod) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
    end
  end
end
