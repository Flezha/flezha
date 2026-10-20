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

  alias Flezha.AuthZ.{AuthState, NamespaceStore, QuotaCounters, Replica, RoleStore, TokenStore}
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Runner.DropMarks
  alias Flezha.Observability.TestSinks
  alias FlezhaTest.Helper.NodeTestHelper

  @all_stores [
    Replica,
    AuthState,
    RoleStore,
    NamespaceStore,
    TokenStore,
    DropMarks,
    QuotaCounters
  ]

  @doc """
  Prepare an isolated auth dir, point the app config at it, (re)start the
  requested stores, and register cleanup. Options:

    * `:stores` — modules to start, `:all` (default) for every one, or
      `[]` to only prepare dir + config (suites that start stores per test).
    * `:enabled` — value for `:authn_authz, :enabled` (default `true`);
      `:unchanged` leaves the key as configured.
    * `:claim` — flip the durable `AuthState` flag after start (default
      `false`, i.e. the `:bootstrap_pending` posture under `enabled: true`).
    * `:register_source` — register `:platform_authz` again and wait until
      the RingBufferSink keeps a table for it (default `false`).

  Returns `%{tmp: tmp, auth_dir: auth_dir}` for the test context.
  """
  def setup_auth_stores(opts \\ []) do
    prefix = Keyword.get(opts, :prefix, "flz_auth")
    stores = resolve_stores(Keyword.get(opts, :stores, :all))

    tmp = NodeTestHelper.claim_tmp_dir(prefix)
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

    if Keyword.get(opts, :register_source, false), do: register_platform_authz_source()

    on_exit(fn ->
      stop_if_running(@all_stores)
      Application.put_env(:flezha, :authn_authz, prev)
      File.rm_rf!(tmp)
    end)

    %{tmp: tmp, auth_dir: auth_dir}
  end

  @doc """
  A runner credential for `namespace`, as `{raw, hash}`.

  The one spelling of "the credential a runner proves its namespace with": a
  `:runner` entry of the token registry, principal `runner@<namespace>`, no
  expiry — the shape `Flezha.AuthZ.RunnerProof` verifies and the shape the
  registry refuses to give an expiry.
  """
  @spec issue_runner_credential(binary()) :: {binary(), binary()}
  def issue_runner_credential(namespace) when is_binary(namespace) do
    {:ok, raw, entry} =
      TokenStore.issue("runner@" <> namespace, namespace, :runner, :never)

    {raw, entry.token_hash}
  end

  @doc "Stop each named auth-store GenServer if it is running."
  def stop_if_running(mods) when is_list(mods), do: Enum.each(mods, &stop_if_running_one/1)

  @doc """
  The `:platform_authz` events named `name` emitted after `since_us`, read once
  the ring buffer has handled every signal already sent to it: an empty answer
  means none was emitted, not none yet.
  """
  def platform_authz_events(name, since_us) do
    TestSinks.settle(RingBufferSink)
    Enum.filter(RingBufferSink.query(:platform_authz, since_us), &(&1.name == name))
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

  # The registry registers the source when it starts; registering it again
  # gives the ring buffer a registration the test can wait on.
  defp register_platform_authz_source() do
    _ =
      Flezha.Observability.Registry.register_source(Flezha.AuthZ.SourceRegistration.__source__())

    # The registration reaches the RingBufferSink as a message; a call to the
    # sink returns once that message has been handled.
    TestSinks.settle(Flezha.Observability.RingBufferSink)
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
