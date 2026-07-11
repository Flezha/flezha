defmodule FlezhaTest.Helper.PostgresSink do
  @moduledoc """
  Shared fixtures for the Postgres sink suites: connection to the
  `postgres-sink` docker service (localhost:5433 by default) and the
  sink's fixed default schema, as an operator would pre-create it.
  """

  @doc "Provider / `Postgrex` connection opts from the standard env vars."
  def conn_opts do
    [
      hostname: System.get_env("POSTGRES_HOST", "localhost"),
      port: String.to_integer(System.get_env("POSTGRES_PORT", "5433")),
      database: System.get_env("POSTGRES_DB", "flezha_test"),
      username: System.get_env("POSTGRES_USER", "flezha"),
      password: System.get_env("POSTGRES_PASSWORD", "flezha_test")
    ]
  end

  def connect! do
    {:ok, db} = Postgrex.start_link(conn_opts())
    db
  end

  def drop_tables!(db) do
    Postgrex.query!(db, "DROP TABLE IF EXISTS flezha_records", [])
    Postgrex.query!(db, "DROP TABLE IF EXISTS flezha_sink_progress", [])
    :ok
  end

  def create_tables!(db) do
    Postgrex.query!(
      db,
      "CREATE TABLE flezha_records (key text PRIMARY KEY, segment text, seq text, " <>
        "epoch bigint, payload bytea, updated_at timestamptz)",
      []
    )

    Postgrex.query!(
      db,
      "CREATE TABLE flezha_sink_progress (segment text PRIMARY KEY, last_seq text, epoch bigint)",
      []
    )

    :ok
  end

  @doc "All `flezha_records` rows ordered by key: `[key, segment, seq, epoch, payload, updated_at]`."
  def rows(db) do
    Postgrex.query!(
      db,
      "SELECT key, segment, seq, epoch, payload, updated_at FROM flezha_records ORDER BY key",
      []
    ).rows
  end

  @doc "All `flezha_sink_progress` rows ordered by segment: `[segment, last_seq, epoch]`."
  def progress(db) do
    Postgrex.query!(
      db,
      "SELECT segment, last_seq, epoch FROM flezha_sink_progress ORDER BY segment",
      []
    ).rows
  end

  def table_exists?(db, name) do
    %{rows: [[reg]]} = Postgrex.query!(db, "SELECT to_regclass($1)::text", [name])
    reg != nil
  end
end
