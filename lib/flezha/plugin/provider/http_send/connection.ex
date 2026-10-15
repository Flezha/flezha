defmodule Flezha.Plugin.Provider.HttpSend.Connection do
  @moduledoc """
  One connection of a `Flezha.Plugin.Provider.HttpSend` worker: a process
  linked to it that owns one kept-alive `Flezha.Http.Client` connection and runs
  one request at a time, answering `{:response, self(), result}`.

  It is a process of its own because a request blocks for up to its timeout,
  and the worker must stay answerable meanwhile. It opens the connection on the
  first request, again after a failed exchange (a response that arrives after
  its timeout would be read as the next one's), and after a response whose body
  it stopped reading. What it holds between
  requests is the endpoint and the connection, never a header; it is a bare
  process, so no status of it shows the request target either.
  """

  alias Flezha.Http.Client

  # Only a response's status is used: past this much body the connection is
  # closed rather than read on.
  @max_body_bytes 8 * 1024

  @type request :: {binary(), [Client.header()], binary()}

  @doc """
  Start one, linked to the caller, which it answers. `endpoint` is the scheme,
  host, port and request target every request goes to.
  """
  @spec start_link(map(), pos_integer()) :: pid()
  def start_link(endpoint, timeout) do
    worker = self()
    spawn_link(fn -> loop(%{endpoint: endpoint, timeout: timeout, worker: worker, conn: nil}) end)
  end

  @doc "Run `request`; the answer comes to the worker as a message."
  @spec request(pid(), request()) :: :ok
  def request(connection, request) do
    send(connection, {:request, request})
    :ok
  end

  @doc "Stop it, closing its connection."
  @spec stop(pid()) :: :ok
  def stop(connection) do
    Process.unlink(connection)
    Process.exit(connection, :shutdown)
    :ok
  end

  defp loop(state) do
    receive do
      {:request, request} ->
        {state, result} = exchange(state, request)
        send(state.worker, {:response, self(), result})
        loop(state)
    end
  end

  defp exchange(%{endpoint: endpoint} = state, {method, headers, body}) do
    with {:ok, conn} <-
           Client.ensure_open(state.conn, endpoint.scheme, endpoint.host, endpoint.port,
             timeout: state.timeout
           ),
         {:ok, conn, status, response_headers, _body} <-
           Client.request(conn, method, endpoint.target, headers, body, state.timeout,
             max_body_bytes: {@max_body_bytes, :close}
           ) do
      {%{state | conn: conn}, {:ok, status, response_headers}}
    else
      {:error, reason, conn} -> {close(%{state | conn: conn}), {:error, reason}}
      {:error, reason} -> {close(state), {:error, reason}}
    end
  end

  defp close(state) do
    Client.close(state.conn)
    %{state | conn: nil}
  end
end
