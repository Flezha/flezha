defmodule Flezha.AuthZ.TokenStore do
  @moduledoc """
  Durable scoped-token registry backed by `<auth_dir>/tokens.dets`.

  Wire form: 256-bit random value, base32-encoded, prefixed with
  `flzt_`. Storage form: SHA-256 of the bare token. The raw token is
  returned to the caller exactly once at `issue/5`; the DETS row
  carries only the hash so a stolen file is not a credential.

  Verification: hash the presented token, look it up, reject when
  `revoked_at` is non-null or `expires_at` is in the past, otherwise
  bind `principal`/`namespace`/`role` to the caller.

  A `:runner` entry is the credential a runner connection proves its namespace
  with (`Flezha.AuthZ.RunnerProof`), and it is the one kind this registry
  refuses to give an expiry: the holder is an unattended process, and a
  credential that lapses fails it at a restart rather than at a convenient
  moment. A namespace may hold as many live runner entries as a caller
  issues, which is what makes a rotation "issue, roll the fleet, retire the
  old one" rather than a cutover. Revoking one closes the sessions it
  authenticated, because nothing else would ever evict them.

  Every entry that does have an expiry is announced before it reaches it: the
  registry sweeps on `:token_expiry_check_interval_ms` and emits
  `:token_expiring` once for each entry inside `:token_expiry_warning_ms` of
  lapsing, so a lapse is a monitoring alert rather than the moment a caller
  discovers they can no longer issue a credential.
  """

  use GenServer
  use Flezha.State.Store
  use Flezha.Process.Timer, keys: [:expiry_check]

  require Logger

  alias Flezha.AuthZ.{Namespace, Role}
  alias Flezha.Config.Durations
  alias Flezha.DetsStore
  alias Flezha.Identificator
  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Runner.FleetManager

  @table :flz_authz_tokens
  @filename "tokens.dets"
  @prefix "flzt_"
  @raw_bytes 32

  @type entry :: %{
          required(:token_hash) => binary(),
          required(:principal) => binary(),
          required(:namespace) => binary(),
          required(:role) => Role.t(),
          required(:created_at) => integer(),
          required(:expires_at) => integer() | :never,
          required(:revoked_at) => integer() | nil,
          required(:created_by_principal) => binary() | nil,
          required(:label) => binary() | nil
        }

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Issue a new token. Returns `{:ok, raw_token, entry}` on success;
  the raw token is the value to hand to the caller. Subsequent
  reads of the store SHALL NOT recover the raw value.
  """
  @spec issue(binary(), binary(), Role.t(), pos_integer() | :never, keyword()) ::
          {:ok, binary(), entry()} | {:error, term()}
  def issue(principal, namespace, role, ttl_seconds, opts \\ [])
      when is_binary(principal) and is_binary(namespace) and is_atom(role) do
    now = System.system_time(:second)

    with :ok <- Role.validate(role),
         :ok <- Namespace.validate_name(namespace),
         :ok <- validate_runner_entry(role, namespace, ttl_seconds),
         {:ok, expires_at} <- expiry_at(ttl_seconds, now) do
      raw = generate_token()

      entry = %{
        token_hash: hash_token(raw),
        principal: principal,
        namespace: namespace,
        role: role,
        created_at: now,
        expires_at: expires_at,
        revoked_at: nil,
        created_by_principal: Keyword.get(opts, :created_by_principal),
        label: Keyword.get(opts, :label)
      }

      with :ok <- GenServer.call(__MODULE__, {:insert, entry.token_hash, entry}) do
        emit_issued(entry)
        {:ok, raw, entry}
      end
    end
  end

  @doc """
  Verify a presented raw token. On success returns
  `{:ok, %{principal, namespace, role, expires_at, token_hash}}`; a refusal is
  one of the `authz.token_*` codes, or `authz.registry_unavailable`.

  A registry that is not open is refused rather than raising: both readers —
  the CLI's identity binding and the runner connect's namespace proof — are on
  paths where an exception would cost more than a refusal, and neither should
  carry its own guard.
  """
  @spec verify(binary()) ::
          {:ok,
           %{
             principal: binary(),
             namespace: binary(),
             role: Role.t(),
             expires_at: integer() | :never,
             token_hash: binary()
           }}
          | {:error, Flezha.Error.t()}
  def verify(token) when is_binary(token) do
    if String.starts_with?(token, @prefix) do
      case lookup_by_hash(hash_token(token)) do
        {:ok, %{revoked_at: revoked}} when is_integer(revoked) ->
          refused("authz.token_revoked")

        {:ok, %{expires_at: :never} = e} ->
          {:ok, project(e)}

        {:ok, %{expires_at: exp} = e} ->
          if exp <= System.system_time(:second) do
            refused("authz.token_expired")
          else
            {:ok, project(e)}
          end

        :error ->
          refused("authz.token_unknown")

        :unavailable ->
          refused("authz.registry_unavailable")
      end
    else
      refused("authz.token_malformed")
    end
  end

  def verify(_), do: refused("authz.token_malformed")

  defp refused(code), do: {:error, Flezha.Error.new(code)}

  @doc """
  Revoke the token whose `token_hash` matches `id` (a SHA-256 hex
  digest prefix — at least 8 chars). Returns `{:ok, entry}` or
  `{:error, {:token_not_found | :token_ambiguous, id}}`.
  """
  @spec revoke(binary(), keyword()) :: {:ok, entry()} | {:error, term()}
  def revoke(id, opts \\ []) when is_binary(id) do
    case Identificator.validate(:token, id) do
      :ok -> close_authenticated_sessions(GenServer.call(__MODULE__, {:revoke, id, opts}))
      {:error, _refusal} -> {:error, {:token_not_found, id}}
    end
  end

  @doc """
  Revoke by the raw token value itself. Useful for self-revoke flows
  where the bearer can present the token they want to invalidate.
  """
  @spec revoke_by_token(binary()) :: {:ok, entry()} | {:error, term()}
  def revoke_by_token(token) when is_binary(token) do
    hash = hash_token(token)
    close_authenticated_sessions(GenServer.call(__MODULE__, {:revoke_by_hash, hash}))
  end

  @doc """
  List tokens. When `namespace` is provided, only tokens scoped to
  that namespace are returned. Pass `:all` to list every namespace
  (admin@_system only — the caller's role/namespace filtering is
  enforced by the service layer).
  """
  @spec list(binary() | :all) :: [entry()]
  def list(:all), do: all_entries()

  def list(namespace) when is_binary(namespace) do
    all_entries() |> Enum.filter(&(&1.namespace == namespace))
  end

  @doc """
  Run the expiry sweep now and answer the hashes it announced.

  The sweep the timer drives, called synchronously — the same shape
  `Flezha.State.Retention.sweep_now/0` gives its own tick, so what the timer does
  can be asserted without waiting for it.
  """
  @spec check_expiring_now() :: [binary()]
  def check_expiring_now(), do: GenServer.call(__MODULE__, :check_expiring_now)

  ## ----- store declaration -----

  @impl Flezha.State.Store
  def subtree(), do: :auth

  @impl Flezha.State.Store
  def filename(), do: @filename

  @impl Flezha.State.Store
  def table(), do: @table

  # The first sweep is owed as soon as the table is warm: an entry already
  # inside its warning window at boot is exactly the one nobody has noticed.
  @impl Flezha.State.Store
  def after_open(_state) do
    send(self(), :expiry_check)
    :ok
  end

  ## ----- GenServer -----

  @impl true
  def handle_info(:expiry_check, state) do
    {_named, state} = announce_expiring(timer_fired(state, :expiry_check))
    {:noreply, arm_expiry_check(state)}
  end

  @impl true
  def handle_call(:check_expiring_now, _from, state) do
    {named, state} = announce_expiring(state)
    {:reply, named, state}
  end

  def handle_call({:insert, hash, entry}, _from, state) do
    :ok = DetsStore.put(@table, hash, entry)
    {:reply, :ok, state}
  end

  def handle_call({:revoke, id, opts}, _from, state) do
    matches =
      DetsStore.all(@table)
      |> Enum.filter(fn {hash, _entry} -> String.starts_with?(hash, id) end)

    reply =
      case matches do
        [] ->
          {:error, {:token_not_found, id}}

        [{hash, entry}] ->
          revoke_entry(hash, entry, Keyword.get(opts, :revoked_by_principal))

        _ ->
          {:error, {:token_ambiguous, id}}
      end

    {:reply, reply, state}
  end

  def handle_call({:revoke_by_hash, hash}, _from, state) do
    reply =
      case DetsStore.fetch(@table, hash) do
        {:ok, entry} -> revoke_entry(hash, entry, nil)
        :error -> {:error, {:token_not_found, hash}}
      end

    {:reply, reply, state}
  end

  ## ----- private -----

  # The expiry an entry records: a positive number of seconds from now, or none
  # at all. `:never` is a real answer rather than a missing one, which is why it
  # is a clause here and not a nil check at the call.
  defp expiry_at(:never, _now), do: {:ok, :never}
  defp expiry_at(seconds, now) when is_integer(seconds) and seconds > 0, do: {:ok, now + seconds}
  defp expiry_at(invalid, _now), do: {:error, {:invalid_ttl, invalid}}

  # The two rules a runner credential carries, refused here rather than at the
  # connect path: an expiring one is the trap this change exists to close, and a
  # `_system` one would satisfy the cross-namespace admin gate the moment any
  # authority were attached to a runner identity.
  defp validate_runner_entry(:runner, namespace, ttl_seconds) do
    cond do
      Namespace.system?(namespace) -> {:error, :runner_namespace_cannot_be_system}
      ttl_seconds != :never -> {:error, :runner_token_cannot_expire}
      true -> :ok
    end
  end

  defp validate_runner_entry(_role, _namespace, _ttl_seconds), do: :ok

  # A revoked runner credential leaves live control channels behind it, and a
  # credential with no expiry has nothing that would otherwise evict them. Only
  # a runner entry can have authenticated a session, so no caller revoke pays
  # for a fleet call. Driven from the client function rather than the store's
  # own `handle_call`, so the eviction never runs inside this GenServer.
  defp close_authenticated_sessions({:ok, %{role: :runner, token_hash: hash}} = revoked) do
    case FleetManager.evict_credential(hash) do
      0 ->
        :ok

      closed ->
        Logger.warning(
          "[authz.tokens] revoking runner credential #{String.slice(hash, 0, 12)} closed " <>
            "#{closed} live session(s)"
        )
    end

    revoked
  end

  defp close_authenticated_sessions(other), do: other

  @spec hash_token(binary()) :: binary()
  defp hash_token(token) when is_binary(token) do
    :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
  end

  # Mark the row revoked, sync, and emit the audit event.
  defp revoke_entry(hash, entry, revoked_by) do
    updated = %{entry | revoked_at: System.system_time(:second)}
    :ok = DetsStore.put(@table, hash, updated)
    emit_revoked(updated, revoked_by)
    {:ok, updated}
  end

  defp generate_token() do
    raw = :crypto.strong_rand_bytes(@raw_bytes)
    @prefix <> Base.encode32(raw, padding: false, case: :lower)
  end

  defp project(entry) do
    Map.take(entry, [:principal, :namespace, :role, :expires_at, :token_hash])
  end

  defp lookup_by_hash(hash) do
    DetsStore.fetch(@table, hash)
  rescue
    _ -> :unavailable
  end

  # Every entry inside the warning window that has not been announced yet, named
  # once each. The announced set is held in memory rather than written back to
  # the row: a restart re-announcing an entry still inside its window is the
  # right answer, and the row's field list is the published one.
  defp arm_expiry_check(state) do
    arm_timer(state, :expiry_check, Durations.get(:token_expiry_check_interval_ms), :expiry_check)
  end

  defp announce_expiring(state) do
    announced = Map.get(state, :announced, MapSet.new())
    horizon = System.system_time(:second) + div(Durations.get(:token_expiry_warning_ms), 1_000)
    now = System.system_time(:second)

    expiring =
      for entry <- all_entries(),
          is_nil(entry.revoked_at),
          is_integer(entry.expires_at),
          entry.expires_at > now,
          entry.expires_at <= horizon,
          not MapSet.member?(announced, entry.token_hash) do
        emit_expiring(entry)
        entry.token_hash
      end

    {expiring, Map.put(state, :announced, MapSet.union(announced, MapSet.new(expiring)))}
  end

  defp all_entries() do
    DetsStore.all(@table)
    |> Enum.map(fn {_hash, entry} -> entry end)
  rescue
    _ -> []
  end

  defp emit_issued(entry) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: :platform_authz,
        name: :token_issued,
        metadata: %{
          principal: entry.principal,
          namespace: entry.namespace,
          role: entry.role,
          created_by_principal: entry.created_by_principal,
          token_hash_prefix: String.slice(entry.token_hash, 0, 12),
          expires_at: entry.expires_at,
          label: entry.label
        }
      })
    )

    :ok
  rescue
    _ -> :ok
  end

  defp emit_expiring(entry) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: :platform_authz,
        name: :token_expiring,
        metadata: %{
          principal: entry.principal,
          namespace: entry.namespace,
          role: entry.role,
          token_hash_prefix: String.slice(entry.token_hash, 0, 12),
          expires_at: entry.expires_at,
          label: entry.label
        }
      })
    )

    :ok
  rescue
    _ -> :ok
  end

  defp emit_revoked(entry, revoked_by) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: :platform_authz,
        name: :token_revoked,
        metadata: %{
          principal: entry.principal,
          namespace: entry.namespace,
          role: entry.role,
          revoked_by_principal: revoked_by,
          token_hash_prefix: String.slice(entry.token_hash, 0, 12)
        }
      })
    )

    :ok
  rescue
    _ -> :ok
  end
end
