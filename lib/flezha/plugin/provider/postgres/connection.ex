defmodule Flezha.Plugin.Provider.Postgres.Connection do
  @moduledoc """
  What every Postgres provider shares about its connection, and the package
  surface for Postgres: a plugin package reaches connection handling and SQL
  helpers through this module only. It holds the option rows that describe a
  connection, the `Postgrex` options a worker connects with, the only text about
  a connection or its errors a log may carry, and the SQL every reader of a
  table shares: the name checks and quoting, the catalog read and the kind each
  column is read as, the select list and the row encoding.

  The password is the shared credential row (`ProviderBehaviour.Options.secret/2`):
  a literal one is held in a transient closure and the prepared `password` is a
  placeholder. The `configure` closure resolves it, or `password_env`, at every
  connect, so the password never lands in a worker's state, a supervisor's child
  arguments, a log line or a telemetry label.

  No value a user wrote reaches the SQL text: names are validated and quoted,
  and every value is a parameter.
  """

  alias Flezha.Plugin.ProviderBehaviour.Options

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

  @rows [
          database: [type: :string, required: true, doc: "Database name"],
          username: [type: :string, required: true, doc: "Role the plugin connects as"]
        ] ++
          Options.secret(:password, "password") ++
          [
            hostname: [type: :string, default: "localhost", doc: "Database host"],
            port: [type: :int, default: 5432, min: 1, max: 65_535, doc: "TCP port"],
            ssl: [type: :bool, default: false, doc: "TLS to the server"]
          ]

  @doc "The connection's option rows, for a provider's declaration."
  @spec rows() :: keyword()
  def rows(), do: @rows

  @doc "`opts` with a literal password held in its transient row and replaced by the placeholder."
  @spec prepare(map()) :: map()
  def prepare(opts), do: Options.hold_secret(opts, :password)

  @doc """
  The `Postgrex` options for `opts`, with `extra` (a pool size, session
  parameters) on top. The password is resolved inside `configure`, at connect.
  """
  @spec conn_opts(map(), keyword()) :: keyword()
  def conn_opts(opts, extra \\ []) do
    password_fun = Options.secret_fun(opts, :password)

    [
      hostname: opts.hostname,
      port: opts.port,
      database: opts.database,
      username: opts.username,
      ssl: opts.ssl,
      configure: fn conn_opts ->
        case password_fun.() do
          nil -> conn_opts
          password -> Keyword.put(conn_opts, :password, password)
        end
      end
    ] ++ extra
  end

  @doc "Where `opts` connects, for a log line: the host, the port and the database."
  @spec label(map()) :: %{hostname: binary(), port: pos_integer(), database: binary()}
  def label(opts), do: Map.take(opts, [:hostname, :port, :database])

  @doc "A connection label as `host:port/database`."
  @spec describe(map()) :: binary()
  def describe(%{hostname: h, port: p, database: d}), do: "#{h}:#{p}/#{d}"

  @doc """
  An error as the text a log line or a health verdict may carry: the server's
  code and message, never the connection options, which hold the password.
  """
  @spec sanitize_error(term()) :: binary()
  def sanitize_error(%Postgrex.Error{postgres: %{code: code, message: m}}), do: "#{code}: #{m}"
  def sanitize_error(%DBConnection.ConnectionError{message: m}), do: m
  def sanitize_error(%{__exception__: true} = e), do: "#{e.__struct__}: #{Exception.message(e)}"
  def sanitize_error(other), do: inspect(other)

  # ---------- names ----------

  @doc """
  Refuse the first `{field, value, names}` of `checks` whose names are not all
  identifiers, naming the field and the value as written.
  """
  @spec check_names([{atom(), term(), list()}]) ::
          :ok | {:error, {:invalid_option_value, atom(), term()}}
  def check_names(checks) do
    case Enum.find(checks, fn {_field, _value, names} -> not Enum.all?(names, &identifier?/1) end) do
      nil -> :ok
      {field, value, _names} -> {:error, {:invalid_option_value, field, value}}
    end
  end

  @doc "The names in a table written `name` or `schema.name`; a `nil` in place of any other."
  @spec table_names(binary()) :: [binary() | nil]
  def table_names(table) when is_binary(table) do
    case String.split(table, ".") do
      [_name] = names -> names
      [_schema, _name] = names -> names
      _more -> [nil]
    end
  end

  defp identifier?(name), do: is_binary(name) and Regex.match?(@identifier, name)

  @doc "A table written `name` or `schema.name`, as `{schema or nil, name}`."
  @spec split_table(binary()) :: {binary() | nil, binary()}
  def split_table(table) do
    case table_names(table) do
      [name] -> {nil, name}
      [schema, name] -> {schema, name}
    end
  end

  @doc """
  A name as an identifier in a statement: quoted, so its case is kept, and any
  `"` in it doubled, so a name the catalog answered cannot end the identifier.
  """
  @spec quote_name(binary()) :: binary()
  def quote_name(name), do: ~s(") <> String.replace(name, ~s("), ~s("")) <> ~s(")

  @doc "A table as `to_regclass` and a statement read it, each name quoted."
  @spec relation(binary() | nil, binary()) :: binary()
  def relation(nil, table), do: quote_name(table)
  def relation(schema, table), do: quote_name(schema) <> "." <> quote_name(table)

  # ---------- the catalog ----------

  @doc """
  The catalog's rows for `table` in `schema` (`nil`: the search path): one per
  column, in column order, as `kinds/1` reads them: schema, table, column, type,
  typtype, typname, whether an array, and the array's element type. None for a
  missing table.
  """
  @spec catalog(DBConnection.conn(), binary() | nil, binary()) ::
          {:ok, Postgrex.Result.t()} | {:error, term()}
  def catalog(conn, schema, table), do: Postgrex.query(conn, @catalog, [relation(schema, table)])

  @doc """
  Each column of the catalog's rows for one table, in column order, with the
  kind its value is read and written as.
  """
  @spec kinds([list()]) :: [{binary(), term()}]
  def kinds(rows), do: for([_schema, _table, name, _type | kind] <- rows, do: {name, kind(kind)})

  defp kind([_typtype, _typname, true, element]), do: {:array, base_kind(element)}
  defp kind(["b", typname, false, _element]), do: base_kind(typname)
  defp kind(_other), do: :text

  defp base_kind(typname) when typname in @as_decoded, do: :value
  defp base_kind(typname) when typname in @temporal, do: :temporal
  defp base_kind("bytea"), do: :bytea
  defp base_kind(_other), do: :text

  # ---------- rows ----------

  @doc """
  The select list reading `columns` as their kinds say: a type with no decoded
  form as its text, every other as it is.
  """
  @spec select_list([{binary(), term()}]) :: binary()
  def select_list(columns), do: Enum.map_join(columns, ", ", &select/1)

  defp select({name, :text}), do: "#{quote_name(name)}::text"
  defp select({name, {:array, :text}}), do: "#{quote_name(name)}::text[]"
  defp select({name, _kind}), do: quote_name(name)

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
