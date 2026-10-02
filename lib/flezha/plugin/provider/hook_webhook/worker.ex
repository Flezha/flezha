defmodule Flezha.Plugin.Provider.HookWebhook.Worker do
  @moduledoc """
  One `hook_webhook` plugin's connection. It sends each event as one JSON POST,
  in the order its hooks hand them over, on one connection it keeps open
  between them and opens again after a failure (`Flezha.Http.Client`).

  A drop is counted and warned once per plugin and status, naming the plugin
  and the URL's host: the path, the query and the headers may hold a token, so
  none of them is logged, and the state a crash report would print leaves the
  headers out.
  """

  use GenServer

  require Logger

  alias Flezha.Config.Backoff
  alias Flezha.Http.Client
  alias Flezha.Plugin
  alias Flezha.Plugin.ProviderBehaviour

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts),
    do: GenServer.start_link(__MODULE__, opts, Enum.to_list(Map.take(opts, [:name])))

  @doc "Send `event`, retried as the plugin's options say; answers once it is sent or dropped."
  @spec post(GenServer.server(), map()) :: :ok
  def post(worker, event) do
    GenServer.call(worker, {:post, event}, :infinity)
  catch
    # The plugin stopped with this event in flight, which is lost.
    :exit, _gone -> :ok
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       conn: nil,
       uri: URI.parse(opts.url),
       headers: [{"content-type", "application/json"} | Map.to_list(opts.headers)],
       timeout: opts.timeout_ms,
       retries: opts.retries,
       plugin: opts[:source_plugin_id],
       warned: MapSet.new()
     }}
  end

  @impl true
  def handle_call({:post, event}, _from, state),
    do: {:reply, :ok, send_event(state, Jason.encode!(event), 0)}

  @impl true
  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{conn: conn}) do
    if conn, do: Client.close(conn)
    :ok
  end

  @impl true
  def format_status(status), do: Map.update(status, :state, nil, &Plugin.redact/1)

  defp send_event(state, body, attempt) do
    case exchange(state, body) do
      {:ok, state, status} when status in 200..299 ->
        emit(state, :sent, %{})
        state

      {:ok, state, status} when status == 429 or status >= 500 ->
        retry_or_drop(state, body, attempt, status)

      {:ok, state, status} ->
        drop(state, status)

      {:error, state, reason} ->
        retry_or_drop(close(state), body, attempt, transport(reason))
    end
  end

  defp retry_or_drop(state, body, attempt, why) when attempt < state.retries do
    emit(state, :failed, %{reason: why})
    Backoff.wait(:hook_webhook_post, attempt + 1, state.plugin)
    send_event(state, body, attempt + 1)
  end

  defp retry_or_drop(state, _body, _attempt, why), do: drop(state, why)

  defp drop(state, why) do
    unless MapSet.member?(state.warned, why) do
      Logger.warning(
        "[hook_webhook] #{state.plugin} dropped an event: #{state.uri.host} answered #{inspect(why)}"
      )
    end

    emit(state, :dropped, %{reason: why})
    %{state | warned: MapSet.put(state.warned, why)}
  end

  defp exchange(state, body) do
    with {:ok, state} <- connect(state) do
      case Client.request(
             state.conn,
             "POST",
             target(state.uri),
             state.headers,
             body,
             state.timeout
           ) do
        {:ok, conn, status, _headers, _body} -> {:ok, %{state | conn: conn}, status}
        {:error, reason, conn} -> {:error, %{state | conn: conn}, reason}
      end
    end
  end

  defp connect(%{conn: conn, uri: uri} = state) do
    if Client.open?(conn) do
      {:ok, state}
    else
      scheme = String.to_existing_atom(uri.scheme)

      case Client.connect(scheme, uri.host, uri.port, timeout: state.timeout) do
        {:ok, conn} -> {:ok, %{state | conn: conn}}
        {:error, reason} -> {:error, state, reason}
      end
    end
  end

  # A response that arrives after its timeout would be read as the next one's.
  defp close(%{conn: nil} = state), do: state

  defp close(%{conn: conn} = state) do
    Client.close(conn)
    %{state | conn: nil}
  end

  defp target(%URI{path: path, query: query}) do
    path = if path in [nil, ""], do: "/", else: path
    if query, do: path <> "?" <> query, else: path
  end

  defp transport(%{reason: reason}), do: reason
  defp transport(reason), do: reason

  defp emit(state, verb, metadata),
    do:
      ProviderBehaviour.emit(
        :hook_webhook,
        verb,
        %{count: 1},
        Map.put(metadata, :plugin, state.plugin)
      )
end
