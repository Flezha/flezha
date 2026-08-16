defmodule Flezha.State.DurabilityTier.Backend.Postgres do
  @moduledoc """
  Remote durability backend over PostgreSQL: one table of `bytea` blobs keyed by
  artifact key, with an epoch column that makes the conditional manifest write a
  single statement.

  `postgrex` is already a tree dependency (the Postgres output sink) and
  Postgres is already present in the reference deployment, which is why this is
  the first backend rather than object storage. It stores blobs and nothing
  else — no WAL framing, no seq arithmetic, no supersession.

  Credential hygiene follows the sink precedent: the password travels only
  inside the `:configure` closure, so it never lands in the connection child
  args, the process state, or a log line.

  Options (from `:flezha, :durability_tier, :backend_opts`):

    * `:hostname` / `:port` / `:database` / `:username` — connection
    * `:password` / `:password_env` — prefer the env-var form
    * `:ssl` — default `false`
    * `:pool_size` — default `2`
    * `:table` — default `flezha_durability_artifacts`
    * `:auto_create` — default `true`; creates the table on connect
  """

  @behaviour Flezha.State.DurabilityTier.Backend

  @conn __MODULE__.Conn
  @default_table "flezha_durability_artifacts"
  @query_timeout_ms 30_000

  @impl true
  def child_spec(opts) do
    table = table(opts)

    conn_opts =
      [
        name: @conn,
        hostname: Keyword.get(opts, :hostname, "localhost"),
        port: Keyword.get(opts, :port, 5432),
        database: Keyword.fetch!(opts, :database),
        username: Keyword.fetch!(opts, :username),
        ssl: Keyword.get(opts, :ssl, false),
        pool_size: Keyword.get(opts, :pool_size, 2),
        configure: configure_fun(opts)
      ] ++ after_connect(opts, table)

    Postgrex.child_spec(conn_opts)
  end

  @impl true
  def put(key, blob, opts) do
    sql = """
    INSERT INTO #{table(opts)} (key, epoch, blob, updated_at)
    VALUES ($1, 0, $2, now())
    ON CONFLICT (key) DO UPDATE SET blob = EXCLUDED.blob, updated_at = now()
    """

    case query(sql, [key, blob]) do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  @impl true
  def get(key, opts) do
    case query("SELECT blob FROM #{table(opts)} WHERE key = $1", [key]) do
      {:ok, %Postgrex.Result{rows: [[blob]]}} -> {:ok, blob}
      {:ok, %Postgrex.Result{rows: []}} -> {:error, :not_found}
      {:error, _} = err -> err
    end
  end

  @impl true
  def list(prefix, opts) do
    # `left(key, length($1)) = $1` rather than `LIKE $1 || '%'`: artifact keys
    # carry `_` (the reserved-identity and base64 escape), which LIKE would read
    # as a single-character wildcard.
    sql = "SELECT key FROM #{table(opts)} WHERE left(key, length($1)) = $1"

    case query(sql, [prefix]) do
      {:ok, %Postgrex.Result{rows: rows}} -> {:ok, Enum.map(rows, &hd/1)}
      {:error, _} = err -> err
    end
  end

  @impl true
  def put_if(key, blob, epoch, opts) do
    # One statement: the row is written when it is absent or its epoch is not
    # ahead of ours, and the `RETURNING` tells the two apart without a race.
    # Equal epochs overwrite — a shipping controller updates its own manifest
    # many times inside one epoch.
    sql = """
    INSERT INTO #{table(opts)} (key, epoch, blob, updated_at)
    VALUES ($1, $2, $3, now())
    ON CONFLICT (key) DO UPDATE SET blob = EXCLUDED.blob, epoch = EXCLUDED.epoch, updated_at = now()
    WHERE #{table(opts)}.epoch <= EXCLUDED.epoch
    RETURNING epoch
    """

    case query(sql, [key, epoch, blob]) do
      {:ok, %Postgrex.Result{rows: [[_epoch]]}} -> :ok
      {:ok, %Postgrex.Result{rows: []}} -> {:error, {:stale_epoch, held_epoch(key, opts)}}
      {:error, _} = err -> err
    end
  end

  @impl true
  def delete(key, opts) do
    case query("DELETE FROM #{table(opts)} WHERE key = $1", [key]) do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  @doc """
  DDL for the artifact table. Public so the integration test and an operator
  provisioning the schema by hand share one definition with `auto_create`.

  Serialized on an advisory lock: `auto_create` runs per connection, so a pool
  opening two connections at once would otherwise race inside
  `CREATE TABLE IF NOT EXISTS` and lose one of them to a `pg_type` unique
  violation — which kills that connection at boot.
  """
  @spec create_table_sql(binary()) :: binary()
  def create_table_sql(table) do
    """
    DO $flezha_durability$
    BEGIN
      PERFORM pg_advisory_xact_lock(hashtext('flezha_durability_tier:#{table}'));
      CREATE TABLE IF NOT EXISTS #{table} (
        key TEXT PRIMARY KEY,
        epoch BIGINT NOT NULL DEFAULT 0,
        blob BYTEA NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    END
    $flezha_durability$;
    """
  end

  @doc "The configured table name, validated as a bare identifier."
  @spec table(keyword()) :: binary()
  def table(opts) do
    name = Keyword.get(opts, :table) || @default_table

    if Regex.match?(~r/^[a-zA-Z_][a-zA-Z0-9_]*$/, name) do
      name
    else
      raise ArgumentError,
            "durability_tier table must be a bare SQL identifier, got #{inspect(name)}"
    end
  end

  defp held_epoch(key, opts) do
    case query("SELECT epoch FROM #{table(opts)} WHERE key = $1", [key]) do
      {:ok, %Postgrex.Result{rows: [[epoch]]}} -> epoch
      _ -> :unknown
    end
  end

  defp query(sql, params) do
    Postgrex.query(@conn, sql, params, timeout: @query_timeout_ms)
  catch
    :exit, reason -> {:error, {:no_connection, reason}}
  end

  defp after_connect(opts, table) do
    if Keyword.get(opts, :auto_create, true) do
      [after_connect: {Postgrex, :query!, [create_table_sql(table), []]}]
    else
      []
    end
  end

  defp configure_fun(opts) do
    password_fun =
      case {Keyword.get(opts, :password), Keyword.get(opts, :password_env)} do
        {password, _} when is_binary(password) and password != "" -> fn -> password end
        {_, env} when is_binary(env) and env != "" -> fn -> System.get_env(env) end
        _ -> fn -> nil end
      end

    fn conn_opts ->
      case password_fun.() do
        nil -> conn_opts
        password -> Keyword.put(conn_opts, :password, password)
      end
    end
  end
end
