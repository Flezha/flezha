defmodule Flezha.Plugin.Provider.Postgres.Connection do
  @moduledoc """
  What every Postgres provider shares about its connection: the option rows that
  describe it, the password handling, the `Postgrex` options a worker connects
  with, and the only text about a connection or its errors a log may carry.

  A literal `password` is held in the transient row `password_secret`, inside a
  closure, which is kept in memory, never written down and never printed by a
  crash report, and the prepared `password` is a placeholder. The `configure`
  closure resolves the secret, or `password_env`, at every connect, so the
  password never lands in a worker's state, a supervisor's child arguments, a
  log line or a telemetry label.
  """

  @redacted "**redacted**"

  @rows [
    database: [type: :string, required: true, doc: "Database name"],
    username: [type: :string, required: true, doc: "Role the plugin connects as"],
    password: [type: :string, doc: "Password, redacted once the plugin is prepared"],
    password_env: [type: :string, doc: "Environment variable holding the password instead"],
    # The literal password inside a closure, held in memory so a connect
    # authenticates with it while the prepared `password` is the placeholder.
    password_secret: [type: :any, internal: true, transient: true],
    hostname: [type: :string, default: "localhost", doc: "Database host"],
    port: [type: :int, default: 5432, min: 1, max: 65_535, doc: "TCP port"],
    ssl: [type: :bool, default: false, doc: "TLS to the server"]
  ]

  @doc "The connection's option rows, for a provider's declaration."
  @spec rows() :: keyword()
  def rows(), do: @rows

  @doc """
  `opts` with a literal password moved into `password_secret`, as a closure that
  answers it, and replaced by the placeholder. A plugin prepared again from these
  opts keeps the secret it holds.
  """
  @spec prepare(map()) :: map()
  def prepare(opts) do
    case Map.get(opts, :password) do
      password when is_binary(password) and password not in ["", @redacted] ->
        %{opts | password: @redacted} |> Map.put(:password_secret, fn -> password end)

      _none_or_placeholder ->
        opts
    end
  end

  @doc """
  The `Postgrex` options for `opts`, with `extra` (a pool size, session
  parameters) on top. The password is resolved inside `configure`, at connect.
  """
  @spec conn_opts(map(), keyword()) :: keyword()
  def conn_opts(opts, extra \\ []) do
    password_fun = password_fun(opts)

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

  defp password_fun(opts) do
    case {Map.get(opts, :password_secret), Map.get(opts, :password_env)} do
      {secret, _} when is_function(secret, 0) -> secret
      {_, env} when is_binary(env) and env != "" -> fn -> System.get_env(env) end
      _ -> fn -> nil end
    end
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
end
