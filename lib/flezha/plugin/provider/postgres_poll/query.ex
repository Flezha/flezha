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
  the session runs in UTC.
  """

  @identifier ~r/\A[A-Za-z_][A-Za-z0-9_$]{0,62}\z/

  # The types whose decoded value is written as it comes, as an ISO 8601 string,
  # or as base64. Every other type is selected as its text form.
  @as_decoded ~w(int2 int4 int8 float4 float8 bool text varchar bpchar json jsonb)
  @temporal ~w(date time timestamp timestamptz)

  @catalog """
  SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, NULL),
         t.typtype, t.typname, t.typcategory = 'A', e.typname
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
  JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
  LEFT JOIN pg_catalog.pg_type e ON e.oid = t.typelem AND e.typtype = 'b'
  WHERE c.oid = to_regclass($1::text)
  ORDER BY a.attnum
  """

  @session "SELECT set_config('transaction_read_only', 'on', true), " <>
             "set_config('statement_timeout', $1, true)"

  @doc """
  Refuse a `table`, `id_column`, `timestamp_column` or `columns` entry that is
  not an identifier, naming the option, before anything is opened.
  """
  @spec validate_names(map()) :: :ok | {:error, {:invalid_option_value, atom(), term()}}
  def validate_names(opts) do
    checks = [
      {:table, opts.table, table_names(opts.table)},
      {:id_column, opts.id_column, [opts.id_column]},
      {:timestamp_column, opts.timestamp_column, List.wrap(opts.timestamp_column)},
      {:columns, opts.columns, List.wrap(opts.columns)}
    ]

    case Enum.find(checks, fn {_field, _value, names} -> not Enum.all?(names, &identifier?/1) end) do
      nil -> :ok
      {field, value, _names} -> {:error, {:invalid_option_value, field, value}}
    end
  end

  defp table_names(table) when is_binary(table) do
    case String.split(table, ".") do
      [_name] = names -> names
      [_schema, _name] = names -> names
      _more -> [nil]
    end
  end

  defp identifier?(name), do: is_binary(name) and Regex.match?(@identifier, name)

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
    types = Map.new(rows, fn [_, _, name, type | kind] -> {name, {type, kind(kind)}} end)
    wanted = spec.columns || Enum.map(rows, &Enum.at(&1, 2))

    case Enum.find([spec.id, spec.ts | wanted], &(&1 != nil and not Map.has_key?(types, &1))) do
      nil ->
        columns = for name <- wanted, do: {name, elem(types[name], 1)}

        cursor =
          for name <- [spec.ts, spec.id],
              name,
              do: {"r.#{quote_name(name)}", elem(types[name], 0)}

        {:ok, queries(%{relation: "#{schema}.#{table}", columns: columns}, schema, table, cursor)}

      missing ->
        {:error, {:column_missing, missing}}
    end
  end

  defp kind([_typtype, _typname, true, element]), do: {:array, base_kind(element)}
  defp kind(["b", typname, false, _element]), do: base_kind(typname)
  defp kind(_other), do: :text

  defp base_kind(typname) when typname in @as_decoded, do: :value
  defp base_kind(typname) when typname in @temporal, do: :temporal
  defp base_kind("bytea"), do: :bytea
  defp base_kind(_other), do: :text

  # ---------- the queries ----------

  # The cursor's parameters come first, then the delay in timestamp mode, then
  # the limit.
  defp queries(plan, schema, table, cursor) do
    # The cursor columns are named through the alias `r`, so ORDER BY reads the
    # table's column, never a selected column of the same name.
    from = "#{quote_name(schema)}.#{quote_name(table)} r"
    names = Enum.map(cursor, &elem(&1, 0))
    order = Enum.join(names, ", ")
    select = Enum.map_join(plan.columns, ", ", &select/1) <> ", " <> texts(names)
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

  defp select({name, :text}), do: "#{quote_name(name)}::text"
  defp select({name, {:array, :text}}), do: "#{quote_name(name)}::text[]"
  defp select({name, _kind}), do: quote_name(name)

  defp texts(names), do: Enum.map_join(names, ", ", &"#{&1}::text")

  defp casts(cursor) do
    cursor
    |> Enum.with_index(1)
    |> Enum.map_join(", ", fn {{_name, type}, i} -> "($#{i}::text)::#{type}" end)
  end

  defp quote_name(name), do: ~s("#{name}")

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
    case plan(spec, run!(conn, @catalog, [relation(spec.table)]).rows) do
      {:ok, plan} -> plan
      {:error, reason} -> DBConnection.rollback(conn, reason)
    end
  end

  # The table as `to_regclass` reads it: quoted, so its case is kept.
  defp relation(table), do: table |> String.split(".") |> Enum.map_join(".", &quote_name/1)

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
      {encode(plan.columns, values), metadata(plan, at, spec), at}
    end
  end

  defp metadata(plan, at, spec) do
    %{
      "channel" => spec.entry_channel,
      "flz.msg_id" => Enum.join([spec.plugin_id, plan.relation | at], "/")
    }
  end

  @doc "A row's values, read as `columns` say, as one JSON object in column order."
  @spec encode([{binary(), term()}], list()) :: binary()
  def encode(columns, values) do
    columns
    |> Enum.zip(values)
    |> Enum.map(fn {{name, kind}, value} -> {name, value(kind, value)} end)
    |> Jason.OrderedObject.new()
    |> Jason.encode!()
  end

  defp value(_kind, nil), do: nil

  defp value({:array, kind}, list) when is_list(list),
    do: Enum.map(list, &value({:array, kind}, &1))

  defp value({:array, kind}, value), do: value(kind, value)
  defp value(:temporal, %module{} = value), do: module.to_iso8601(value)
  defp value(:bytea, value), do: Base.encode64(value)
  defp value(_kind, :NaN), do: "NaN"
  defp value(_kind, :inf), do: "Infinity"
  defp value(_kind, :"-inf"), do: "-Infinity"
  defp value(_kind, value), do: value
end
