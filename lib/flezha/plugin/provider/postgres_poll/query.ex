defmodule Flezha.Plugin.Provider.PostgresPoll.Query do
  @moduledoc """
  What the Postgres poll source sends to the database and what it makes of the
  answer: the names it accepts, the catalog read that resolves the table and its
  column types, the queries built from them, the row encoding and the message
  id. The reader runs one `read/4` per poll, in a child of its own, from the
  `spec/1` it was started with.

  No value a user wrote reaches the SQL text: names are validated and quoted, the
  types interpolated into a cast are the ones the catalog answered, and every
  value is a parameter. The cursor travels as Postgres's text form and is cast
  back on the server, because a text value cannot bind to an `int8` or a
  `timestamptz` parameter, and its text is the same on every controller because
  the session runs in UTC. The name checks, the catalog read, the select list
  and the row encoding are `Flezha.Plugin.Provider.Postgres.Connection`'s.
  """

  alias Flezha.Plugin.Provider.Postgres.Connection

  @session "SELECT set_config('transaction_read_only', 'on', true), " <>
             "set_config('statement_timeout', $1, true)"

  @doc """
  Refuse a `table`, `id_column`, `timestamp_column` or `columns` entry that is
  not an identifier, naming the option, before anything is opened.
  """
  @spec validate_names(map()) :: :ok | {:error, {:invalid_option_value, atom(), term()}}
  def validate_names(opts) do
    Connection.check_names([
      {:table, opts.table, Connection.table_names(opts.table)},
      {:id_column, opts.id_column, [opts.id_column]},
      {:timestamp_column, opts.timestamp_column, List.wrap(opts.timestamp_column)},
      {:columns, opts.columns, List.wrap(opts.columns)}
    ])
  end

  @doc """
  What the reader reads and how, from the plugin's prepared options: the table
  and its cursor columns, where to start, the batch, the delay, the timeouts,
  and the plugin id and entry channel each row is injected with.
  """
  @spec spec(map()) :: map()
  def spec(opts) do
    opts
    |> Map.take([
      :table,
      :columns,
      :plugin_id,
      :entry_channel,
      :start_at,
      :batch_size,
      :interval_ms,
      :timestamp_delay_ms,
      :query_timeout_ms
    ])
    |> Map.merge(%{id: opts.id_column, ts: opts.timestamp_column, key: key(opts)})
  end

  # What a cursor was read under, stored with it.
  defp key(opts), do: [opts.table, opts.id_column, opts.timestamp_column]

  @doc "`cursor` when it was read under `opts`' table and cursor columns, else none."
  @spec own_cursor(map() | nil, map()) :: map() | nil
  def own_cursor(%{key: key} = cursor, opts), do: if(key == key(opts), do: cursor)
  def own_cursor(_cursor, _opts), do: nil

  # ---------- the catalog ----------

  @doc """
  The plan `spec` reads by, from the catalog's rows for its table: the relation
  it resolved to, each read column with its encoding, and the three queries.
  `{:table_missing, table}` when there are no rows, and `{:column_missing, name}`
  for a named column the table does not have.
  """
  @spec plan(map(), [list()]) :: {:ok, map()} | {:error, term()}
  def plan(spec, []), do: {:error, {:table_missing, spec.table}}

  def plan(spec, [[schema, table | _] | _] = rows) do
    kinds = Map.new(Connection.kinds(rows))
    types = Map.new(rows, fn [_, _, name, type | _kind] -> {name, type} end)
    wanted = spec.columns || Enum.map(rows, &Enum.at(&1, 2))

    case Enum.find([spec.id, spec.ts | wanted], &(&1 != nil and not Map.has_key?(types, &1))) do
      nil ->
        columns = for name <- wanted, do: {name, kinds[name]}

        cursor =
          for name <- [spec.ts, spec.id],
              name,
              do: {"r.#{Connection.quote_name(name)}", types[name]}

        {:ok, queries(%{relation: "#{schema}.#{table}", columns: columns}, schema, table, cursor)}

      missing ->
        {:error, {:column_missing, missing}}
    end
  end

  # ---------- the queries ----------

  # The cursor's parameters come first, then the delay in timestamp mode, then
  # the limit.
  defp queries(plan, schema, table, cursor) do
    # The cursor columns are named through the alias `r`, so ORDER BY reads the
    # table's column, never a selected column of the same name.
    from = "#{Connection.relation(schema, table)} r"
    names = Enum.map(cursor, &elem(&1, 0))
    order = Enum.join(names, ", ")
    select = Connection.select_list(plan.columns) <> ", " <> texts(names)
    not_null = Enum.map_join(names, " AND ", &"#{&1} IS NOT NULL")
    n = length(cursor)
    delay? = n == 2
    limit = if delay?, do: 2, else: 1

    Map.merge(plan, %{
      delay?: delay?,
      first:
        "SELECT #{select} FROM #{from} WHERE #{not_null}#{delay(names, 1)} " <>
          "ORDER BY #{order} LIMIT $#{limit}",
      next:
        "SELECT #{select} FROM #{from} WHERE (#{order}) > (#{casts(cursor)})" <>
          "#{delay(names, n + 1)} ORDER BY #{order} LIMIT $#{n + limit}",
      last:
        "SELECT #{texts(names)} FROM #{from} WHERE #{not_null} " <>
          "ORDER BY #{Enum.map_join(names, ", ", &"#{&1} DESC")} LIMIT 1"
    })
  end

  defp delay([ts, _id], at), do: " AND #{ts} < now() - $#{at}::bigint * interval '1 millisecond'"
  defp delay([_id], _at), do: ""

  defp texts(names), do: Enum.map_join(names, ", ", &"#{&1}::text")

  defp casts(cursor) do
    cursor
    |> Enum.with_index(1)
    |> Enum.map_join(", ", fn {{_name, type}, i} -> "($#{i}::text)::#{type}" end)
  end

  # ---------- one read ----------

  @doc """
  One read in a read-only transaction: the catalog when `plan` is nil, the
  starting cursor when `cursor` is nil, then one batch. Answers the plan, the
  cursor the batch starts after and each row as `{payload, metadata, at}`, `at`
  being the cursor past it.
  """
  @spec read(pid(), map(), map() | nil, map() | nil) ::
          {:ok, map(), map(), [{binary(), map(), [binary()]}]} | {:error, term()}
  def read(conn, spec, plan, cursor) do
    # The server cancels a statement at `query_timeout_ms`; the client gives up
    # one interval later, when the read would have been due again anyway.
    budget = spec.query_timeout_ms + spec.interval_ms

    conn
    |> Postgrex.transaction(&read_in(&1, spec, plan, cursor), timeout: budget)
    |> case do
      {:ok, {plan, cursor, rows}} -> {:ok, plan, cursor, records(rows, plan, spec)}
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, e}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp read_in(conn, spec, plan, cursor) do
    run!(conn, @session, [Integer.to_string(spec.query_timeout_ms)])
    plan = plan || catalog!(conn, spec)
    cursor = cursor || %{key: spec.key, at: start(conn, plan, spec.start_at)}
    {plan, cursor, run!(conn, batch(plan, cursor.at), params(plan, cursor.at, spec)).rows}
  end

  defp catalog!(conn, spec) do
    {schema, table} = Connection.split_table(spec.table)

    with {:ok, %{rows: rows}} <- Connection.catalog(conn, schema, table),
         {:ok, plan} <- plan(spec, rows) do
      plan
    else
      {:error, reason} -> DBConnection.rollback(conn, reason)
    end
  end

  defp start(_conn, _plan, "beginning"), do: nil

  defp start(conn, plan, "end") do
    case run!(conn, plan.last, []).rows do
      [at] -> at
      [] -> nil
    end
  end

  defp batch(plan, nil), do: plan.first
  defp batch(plan, _at), do: plan.next

  defp params(plan, at, spec) do
    delay = if plan.delay?, do: [spec.timestamp_delay_ms], else: []
    List.wrap(at) ++ delay ++ [spec.batch_size]
  end

  defp run!(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, result} -> result
      {:error, reason} -> DBConnection.rollback(conn, reason)
    end
  end

  # ---------- rows ----------

  defp records(rows, plan, spec) do
    width = length(plan.columns)

    for row <- rows do
      {values, at} = Enum.split(row, width)
      {Connection.encode(plan.columns, values), metadata(plan, at, spec), at}
    end
  end

  defp metadata(plan, at, spec) do
    %{
      "channel" => spec.entry_channel,
      "flz.msg_id" => Enum.join([spec.plugin_id, plan.relation | at], "/")
    }
  end
end
