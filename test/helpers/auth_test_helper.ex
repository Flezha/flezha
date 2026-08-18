defmodule FlezhaTest.Helper.AuthTestHelper do
  @moduledoc """
  Per-test authn/authz store harness — the ONE definition of "isolated auth
  stores against a tmp `auth_dir`" shared by every authz-facing suite.

  The auth dir is nested one level inside the tmp dir so
  `Flezha.AuthZ.Bootstrap`'s parent-mode check sees a 0700 parent (the tmp
  itself), never /tmp (1777 on Linux, which would abort every bootstrap
  write).
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.AuthZ.{AuthState, NamespaceStore, QuotaCounters, RoleStore, TokenStore}

  @all_stores [AuthState, RoleStore, NamespaceStore, TokenStore, QuotaCounters]

  @doc """
  Prepare an isolated auth dir, point the app config at it, (re)start the
  requested stores, and register cleanup. Options:

    * `:stores` — modules to start, `:all` (default) for the full five, or
      `[]` to only prepare dir + config (suites that start stores per test).
    * `:enabled` — value for `:authn_authz, :enabled` (default `true`);
      `:unchanged` leaves the key as configured.
    * `:claim` — flip the durable `AuthState` flag after start (default
      `false`, i.e. the `:bootstrap_pending` posture under `enabled: true`).
    * `:register_source` — register `:platform_authz` on the observability
      registry so the RingBufferSink keeps a table for it (default `false`).

  Returns `%{tmp: tmp, auth_dir: auth_dir}` for the test context.
  """
  def setup_auth_stores(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "flz_auth")
    stores = resolve_stores(Keyword.get(opts, :stores, :all))

    tmp = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    auth_dir = Path.join(tmp, "auth")
    File.mkdir_p!(auth_dir)
    File.chmod!(tmp, 0o700)
    File.chmod!(auth_dir, 0o700)

    prev = Application.get_env(:flezha, :authn_authz, [])

    config =
      prev
      |> Keyword.put(:auth_dir, auth_dir)
      |> Keyword.put(:bootstrap_token_path, Path.join(auth_dir, "bootstrap.token"))
      |> put_enabled(Keyword.get(opts, :enabled, true))

    Application.put_env(:flezha, :authn_authz, config)

    stop_if_running(@all_stores)
    Enum.each(stores, fn mod -> {:ok, _} = mod.start_link([]) end)

    if Keyword.get(opts, :claim, false), do: :ok = AuthState.set_enabled()

    registered_source? = Keyword.get(opts, :register_source, false)
    if registered_source?, do: register_platform_authz_source()

    on_exit(fn ->
      stop_if_running(@all_stores)
      if registered_source?, do: Flezha.Observability.Registry.unregister_source(:platform_authz)
      Application.put_env(:flezha, :authn_authz, prev)
      File.rm_rf!(tmp)
    end)

    %{tmp: tmp, auth_dir: auth_dir}
  end

  @doc "Stop each named auth-store GenServer if it is running."
  def stop_if_running(mods) when is_list(mods), do: Enum.each(mods, &stop_if_running_one/1)

  @doc """
  Poll the `:platform_authz` ring buffer for events named `name` emitted
  after `since_us` (monotonic microseconds); returns the matches, `[]` after
  `attempts × 25 ms`.
  """
  def await_platform_authz_events(name, since_us, attempts \\ 20) do
    Enum.reduce_while(1..attempts, [], fn _i, _acc ->
      matches =
        Flezha.Observability.RingBufferSink.query(:platform_authz, since_us)
        |> Enum.filter(&(&1.name == name))

      if matches == [] do
        Process.sleep(25)
        {:cont, []}
      else
        {:halt, matches}
      end
    end)
  end

  @doc """
  `ExUnit.CaptureLog.capture_log/1` with the primary Logger level lowered
  to `level` for the capture window — the test env's primary level is
  `:error`, which filters warnings BEFORE any capture handler sees them.
  """
  def capture_log_at(level, fun) do
    prev = Logger.level()
    Logger.configure(level: level)

    try do
      ExUnit.CaptureLog.capture_log(fun)
    after
      Logger.configure(level: prev)
    end
  end

  defp resolve_stores(:all), do: @all_stores
  defp resolve_stores(stores) when is_list(stores), do: stores

  defp put_enabled(config, :unchanged), do: config
  defp put_enabled(config, enabled), do: Keyword.put(config, :enabled, enabled)

  # The Flezha.AuthZ.Supervisor that normally registers the source is gated
  # off in tests by `application_managed: false`.
  defp register_platform_authz_source() do
    _ =
      Flezha.Observability.Registry.register_source(%{
        name: :platform_authz,
        kinds: [:event, :metric],
        flavour: :declarative,
        cardinality_budget: 256
      })

    # Give the RingBufferSink a tick to observe the registration.
    Process.sleep(50)
  end

  defp stop_if_running_one(mod) do
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
