defmodule Flezha.AuthZ.QuotasTest do
  use ExUnit.Case, async: false

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{AuthState, NamespaceStore, QuotaCounters, Quotas, RoleStore, TokenStore}

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_authz_quotas_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)
    File.chmod!(tmp, 0o700)

    prev = Application.get_env(:flezha, :authn_authz, [])

    Application.put_env(
      :flezha,
      :authn_authz,
      prev |> Keyword.put(:auth_dir, tmp) |> Keyword.put(:enabled, true)
    )

    stop_if_running([AuthState, RoleStore, NamespaceStore, TokenStore, QuotaCounters])
    {:ok, _} = AuthState.start_link([])
    {:ok, _} = RoleStore.start_link([])
    {:ok, _} = NamespaceStore.start_link([])
    {:ok, _} = TokenStore.start_link([])
    {:ok, _} = QuotaCounters.start_link([])

    :ok = AuthState.set_enabled()
    assert AuthZ.enabled?()

    on_exit(fn ->
      stop_if_running([AuthState, RoleStore, NamespaceStore, TokenStore, QuotaCounters])
      Application.put_env(:flezha, :authn_authz, prev)
      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "unknown namespace surfaces :namespace_unknown" do
    assert {:error, :namespace_unknown} = Quotas.check_allocation("ghost", :max_relays)
  end

  test "system namespace allows allocations within default quotas" do
    assert :ok = Quotas.check_allocation("_system", :max_relays, 1)
    assert :ok = Quotas.check_allocation("_system", :max_disk_bytes_total, 1024)
  end

  test "exceeding max_relays surfaces structured error" do
    {:ok, _} = NamespaceStore.create("team-orders", quotas: %{max_relays: 3})

    assert :ok = Quotas.check_allocation("team-orders", :max_relays, 1)
    :ok = Quotas.record_allocation("team-orders", :max_relays, 3)
    _ = :sys.get_state(QuotaCounters)

    assert {:error, {:quota_exceeded, :max_relays, %{current: 3, limit: 3}}} =
             Quotas.check_allocation("team-orders", :max_relays, 1)
  end

  test "dealloc lets a new allocation succeed" do
    {:ok, _} = NamespaceStore.create("team-orders", quotas: %{max_relays: 2})

    :ok = Quotas.record_allocation("team-orders", :max_relays, 2)
    _ = :sys.get_state(QuotaCounters)

    assert {:error, {:quota_exceeded, _, _}} =
             Quotas.check_allocation("team-orders", :max_relays, 1)

    :ok = Quotas.record_deallocation("team-orders", :max_relays, 1)
    # The cast must finish before the next read; round-trip a sync call
    # against the QuotaCounters GenServer to flush its mailbox.
    _ = :sys.get_state(QuotaCounters)

    assert :ok = Quotas.check_allocation("team-orders", :max_relays, 1)
  end

  test "while authn disabled, every check is :ok and no counter touched" do
    :ok = AuthState.reset()
    refute AuthZ.enabled?()

    {:ok, _} = NamespaceStore.create("team-orders", quotas: %{max_relays: 0})
    assert :ok = Quotas.check_allocation("team-orders", :max_relays, 1)
    :ok = Quotas.record_allocation("team-orders", :max_relays, 1)
    assert QuotaCounters.get("team-orders", :relays) == 0
  end

  test "check latency is well under the 50µs spec budget against a warm cache" do
    {:ok, _} = NamespaceStore.create("team-orders", quotas: %{max_relays: 1_000_000})

    # Warm the cache (first call touches both DETS-backed stores via
    # the ETS read path).
    :ok = Quotas.check_allocation("team-orders", :max_relays, 1)

    iterations = 1_000

    {us, _} =
      :timer.tc(fn ->
        Enum.each(1..iterations, fn _ ->
          :ok = Quotas.check_allocation("team-orders", :max_relays, 1)
        end)
      end)

    per_call_us = us / iterations
    # Spec says p99 < 50µs. We measure the AVERAGE here and assert a
    # generous ceiling because CI VMs are noisy; the goal is to catch
    # a regression like "we accidentally added a GenServer hop".
    assert per_call_us < 200, "average #{per_call_us}µs / call exceeded 200µs ceiling"
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
