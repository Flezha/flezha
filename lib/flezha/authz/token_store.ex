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
  """

  use GenServer

  alias Flezha.AuthZ.{Config, Namespace, Role, Store}
  alias Flezha.Observability.{Bus, Signal}

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
  the raw token is the value to hand to the operator. Subsequent
  reads of the store SHALL NOT recover the raw value.
  """
  @spec issue(binary(), binary(), Role.t(), pos_integer() | :never, keyword()) ::
          {:ok, binary(), entry()} | {:error, term()}
  def issue(principal, namespace, role, ttl_seconds, opts \\ [])
      when is_binary(principal) and is_binary(namespace) and is_atom(role) do
    with :ok <- Role.validate(role),
         :ok <- Namespace.validate_name(namespace) do
      raw = generate_token()
      hash = hash_token(raw)
      now = System.system_time(:second)

      expires_at =
        case ttl_seconds do
          :never -> :never
          n when is_integer(n) and n > 0 -> now + n
          _ -> {:error, :invalid_ttl}
        end

      case expires_at do
        {:error, _} = err ->
          err

        _ ->
          entry = %{
            token_hash: hash,
            principal: principal,
            namespace: namespace,
            role: role,
            created_at: now,
            expires_at: expires_at,
            revoked_at: nil,
            created_by_principal: Keyword.get(opts, :created_by_principal),
            label: Keyword.get(opts, :label)
          }

          case GenServer.call(__MODULE__, {:insert, hash, entry}) do
            :ok ->
              emit_issued(entry)
              {:ok, raw, entry}

            other ->
              other
          end
      end
    end
  end

  @doc """
  Verify a presented raw token. On success returns
  `{:ok, %{principal, namespace, role, expires_at}}`.
  """
  @spec verify(binary()) ::
          {:ok,
           %{
             principal: binary(),
             namespace: binary(),
             role: Role.t(),
             expires_at: integer() | :never
           }}
          | {:error, :malformed | :unknown | :token_revoked | :token_expired}
  def verify(token) when is_binary(token) do
    cond do
      not String.starts_with?(token, @prefix) ->
        {:error, :malformed}

      true ->
        case lookup_by_hash(hash_token(token)) do
          {:ok, %{revoked_at: revoked}} when is_integer(revoked) ->
            {:error, :token_revoked}

          {:ok, %{expires_at: :never} = e} ->
            {:ok, project(e)}

          {:ok, %{expires_at: exp} = e} ->
            if exp <= System.system_time(:second) do
              {:error, :token_expired}
            else
              {:ok, project(e)}
            end

          :error ->
            {:error, :unknown}
        end
    end
  end

  def verify(_), do: {:error, :malformed}

  @doc """
  Revoke the token whose `token_hash` matches `id` (a SHA-256 hex
  digest prefix — at least 8 chars). Returns `{:ok, entry}` or
  `{:error, :not_found | :ambiguous}`.
  """
  @spec revoke(binary(), keyword()) :: {:ok, entry()} | {:error, term()}
  def revoke(id, opts \\ []) when is_binary(id) and byte_size(id) >= 8 do
    GenServer.call(__MODULE__, {:revoke, id, opts})
  end

  @doc """
  Revoke by the raw token value itself. Useful for self-revoke flows
  where the bearer can present the token they want to invalidate.
  """
  @spec revoke_by_token(binary()) :: {:ok, entry()} | {:error, term()}
  def revoke_by_token(token) when is_binary(token) do
    hash = hash_token(token)
    GenServer.call(__MODULE__, {:revoke_by_hash, hash})
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

  @doc "Absolute path of the backing DETS file."
  @spec path() :: String.t()
  def path(), do: Path.join(Config.auth_dir(), @filename)

  @doc "SHA-256 hex digest of the bare token value (32 lowercase chars)."
  @spec hash_token(binary()) :: binary()
  def hash_token(token) when is_binary(token) do
    :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
  end

  @doc """
  Token wire-form prefix; exposed for the CLI handshake so it can
  cheap-reject non-token frames before any hashing happens.
  """
  @spec wire_prefix() :: binary()
  def wire_prefix(), do: @prefix

  ## ----- GenServer -----

  @impl true
  def init(_opts) do
    Store.init_table(@table, path())
  end

  @impl true
  def handle_call({:insert, hash, entry}, _from, state) do
    :ok = :dets.insert(@table, {hash, entry})
    :ok = :dets.sync(@table)
    {:reply, :ok, state}
  end

  def handle_call({:revoke, id, opts}, _from, state) do
    matches =
      :dets.match_object(@table, {:_, :_})
      |> Enum.filter(fn {hash, _entry} -> String.starts_with?(hash, id) end)

    reply =
      case matches do
        [] ->
          {:error, :not_found}

        [{hash, entry}] ->
          revoke_entry(hash, entry, Keyword.get(opts, :revoked_by_principal))

        _ ->
          {:error, :ambiguous}
      end

    {:reply, reply, state}
  end

  def handle_call({:revoke_by_hash, hash}, _from, state) do
    reply =
      case :dets.lookup(@table, hash) do
        [{^hash, entry}] ->
          revoke_entry(hash, entry, nil)

        _ ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end

  ## ----- private -----

  # Mark the row revoked, sync, and emit the audit event.
  defp revoke_entry(hash, entry, revoked_by) do
    updated = %{entry | revoked_at: System.system_time(:second)}
    :ok = :dets.insert(@table, {hash, updated})
    :ok = :dets.sync(@table)
    emit_revoked(updated, revoked_by)
    {:ok, updated}
  end

  defp generate_token() do
    raw = :crypto.strong_rand_bytes(@raw_bytes)
    @prefix <> Base.encode32(raw, padding: false, case: :lower)
  end

  defp project(entry) do
    Map.take(entry, [:principal, :namespace, :role, :expires_at])
  end

  defp lookup_by_hash(hash) do
    case :dets.lookup(@table, hash) do
      [{^hash, entry}] -> {:ok, entry}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp all_entries() do
    :dets.match_object(@table, {:_, :_})
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
