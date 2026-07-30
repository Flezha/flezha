defmodule Flezha.Plugin.Provider.HttpPoll.PollWorker do
  @moduledoc """
  The polling half of `Flezha.Plugin.Provider.HttpPoll`: one process per
  enabled plugin owning the interval timer and the injection state, so the host
  operative's dispatch path is never inside an upstream wait.

  Each request runs in a short-lived monitored child rather than in this
  process, which keeps the worker's mailbox answerable while a slow or hanging
  endpoint is outstanding — the health probe reports on exactly the outage that
  would otherwise block it. The child streams the response and cancels past
  `:max_body_bytes`, so the cap bounds what the controller allocates, not only
  what the operative receives.

  Injection is content-gated: the body's SHA-256 is compared with the last
  *delivered* one and a frame is sent only when they differ. Delivery uses the
  confirming ingest clause, and the hash advances only once the operative has
  taken the payload — a refused or not-ready ingest leaves the old hash so the
  next poll re-offers the same content instead of losing a change.

  A fresh worker holds no hash, so its first success always injects. That is
  the seed, and it is why a handover or a plugin restart converges the
  rejoining operative onto current content within one interval.

  Started unlinked and monitored by the plugin host (`Extension.Table` tracks
  the returned pid and kills it on unsubscribe). It monitors the host in turn
  so a poller cannot outlive the operative it feeds.
  """

  alias Flezha.Plugin.ProviderBehaviour

  use GenServer

  require Logger

  @telemetry [:flezha, :plugin, :http_poll]

  @doc "Start a poller from the provider's normalised opts. Performs no I/O."
  @spec start(map()) :: {:ok, pid()} | {:error, term()}
  def start(opts) when is_map(opts), do: GenServer.start(__MODULE__, opts)

  @doc """
  Current `{failure run, last error}`, for the provider's `health/1`. Returns
  `:timeout` if the worker does not answer — which, since requests run off this
  process, means the worker itself is wedged rather than merely busy.
  """
  @spec probe(pid()) :: {:ok, {non_neg_integer(), term()}} | :timeout
  def probe(pid), do: ProviderBehaviour.ask(pid, :health_probe)

  @impl true
  def init(opts) do
    host = Map.fetch!(opts, :overwatcher_pid)

    state =
      opts
      |> Map.take([
        :url,
        :channel,
        :headers,
        :plugin_id,
        :interval_ms,
        :timeout_ms,
        :max_body_bytes,
        :overwatcher_pid
      ])
      |> Map.merge(%{
        host_ref: Process.monitor(host),
        fetch: nil,
        last_hash: nil,
        etag: nil,
        consecutive_failures: 0,
        last_error: nil
      })

    # The first poll is scheduled, never executed here: `activate/1` is
    # synchronous in the host, and an upstream wait there would block the bind.
    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, %{fetch: nil} = state), do: {:noreply, start_fetch(state)}

  # A tick that lands on an outstanding request is dropped rather than queued:
  # the running fetch schedules the next one when it settles, so a slow
  # endpoint stretches the cadence instead of stacking concurrent requests.
  def handle_info(:poll, state), do: {:noreply, state}

  def handle_info({:fetch_done, pid, result}, %{fetch: {pid, ref, started}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, settle(%{state | fetch: nil}, result, elapsed(started))}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{host_ref: ref} = state) do
    # The operative this poller feeds is gone; injecting into a dead mailbox is
    # nothing but wasted requests against the upstream.
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, :process, _dead, reason}, %{fetch: {_pid, ref, started}} = state) do
    {:noreply,
     settle(%{state | fetch: nil}, {:error, {:fetch_crashed, reason}}, elapsed(started))}
  end

  def handle_info({:health_probe, from, ref}, state) do
    send(from, {ref, {state.consecutive_failures, state.last_error}})
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # ---------- the poll cycle ----------

  defp start_fetch(state) do
    worker = self()
    request = Map.take(state, [:url, :headers, :etag, :timeout_ms, :max_body_bytes])

    {pid, ref} = spawn_monitor(fn -> send(worker, {:fetch_done, self(), fetch(request)}) end)

    %{state | fetch: {pid, ref, :erlang.monotonic_time(:millisecond)}}
  end

  defp settle(state, result, latency_ms) do
    state =
      case result do
        {:ok, :not_modified} -> unchanged(state, latency_ms)
        {:ok, body, etag} -> classify_body(state, body, etag, latency_ms)
        {:error, reason} -> failed(state, reason, latency_ms)
      end

    Process.send_after(self(), :poll, state.interval_ms)
    state
  end

  # A non-UTF-8 body is a failure rather than a frame: it cannot be carried by
  # an entry channel (the SDKs seal entry payloads to strings), so injecting it
  # would only move the error to the runner. The oversize refusal happens
  # earlier, in the fetch child, where it can still abort the transfer.
  #
  # The validator is committed only on a NON-failure outcome. Storing it here
  # would make the next request conditional on a body this poller rejected: the
  # server would answer 304, that reads as an unchanged poll, the failure run
  # resets, and the plugin sits healthy and silent forever with no hash.
  defp classify_body(state, body, etag, latency_ms) do
    cond do
      not String.valid?(body) ->
        failed(state, {:invalid_utf8, byte_size(body)}, latency_ms)

      :crypto.hash(:sha256, body) == state.last_hash ->
        unchanged(%{state | etag: etag}, latency_ms)

      true ->
        inject(state, body, etag, latency_ms)
    end
  end

  defp inject(state, body, etag, latency_ms) do
    seed? = is_nil(state.last_hash)

    case confirm(state, body) do
      :ok ->
        :telemetry.execute(
          @telemetry ++ [:injected],
          %{bytes: byte_size(body), latency_ms: latency_ms},
          Map.put(labels(state), :seed, seed?)
        )

        recovered(%{state | last_hash: :crypto.hash(:sha256, body), etag: etag})

      {:error, reason} ->
        # Neither the hash nor the validator is advanced: the operative did not
        # take this content, so the next poll must re-offer it rather than read
        # it as already delivered and go quiet forever.
        failed(state, {:ingest, reason}, latency_ms)
    end
  end

  # The confirming ingest clause, not the fire-and-forget cast: a parked
  # operative drops the cast silently, and a dropped config push that has
  # already advanced the hash is lost until the endpoint happens to change
  # again. `:absorbed` (duplicate) and `:suspended` (journaled for wake) both
  # mean the operative owns the payload.
  defp confirm(state, body) do
    case GenServer.call(
           state.overwatcher_pid,
           {:ingest, body, %{"channel" => state.channel}},
           state.timeout_ms
         ) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, reason -> {:error, {:unreachable, reason}}
  end

  defp unchanged(state, latency_ms) do
    :telemetry.execute(@telemetry ++ [:unchanged], %{latency_ms: latency_ms}, labels(state))
    recovered(state)
  end

  # A success ends the failure run, so the health contract flips back on the
  # same poll that delivered the change the outage hid.
  defp recovered(state), do: %{state | consecutive_failures: 0, last_error: nil}

  defp failed(state, reason, latency_ms) do
    failures = state.consecutive_failures + 1

    Logger.warning(
      "[Plugin.Provider.HttpPoll] poll failed plugin_id=#{inspect(state.plugin_id)} " <>
        "consecutive=#{failures} reason=#{inspect(reason)}"
    )

    :telemetry.execute(
      @telemetry ++ [:failed],
      %{consecutive_failures: failures, latency_ms: latency_ms},
      Map.put(labels(state), :reason, reason)
    )

    # The last delivered hash is retained: being unable to re-read the resource
    # does not invalidate the content the operative already holds.
    %{state | consecutive_failures: failures, last_error: reason}
  end

  defp labels(state), do: %{plugin_id: state.plugin_id, channel: state.channel}

  defp elapsed(started), do: :erlang.monotonic_time(:millisecond) - started

  # ---------- one request, in the fetch child ----------

  @doc false
  @spec fetch(map()) :: {:ok, binary(), binary() | nil} | {:ok, :not_modified} | {:error, term()}
  def fetch(request) do
    with {:ok, ssl_opts} <- ssl_opts(request.url) do
      http_opts = [
        timeout: request.timeout_ms,
        connect_timeout: request.timeout_ms,
        ssl: ssl_opts,
        # Static headers may carry a bearer token, and `:httpc` replays them to
        # whatever a redirect names — a host this provider never validated. A
        # 3xx is reported as the ordinary status failure it is.
        autoredirect: false
      ]

      case :httpc.request(
             :get,
             {String.to_charlist(request.url), request_headers(request)},
             http_opts,
             sync: false,
             stream: :self,
             body_format: :binary
           ) do
        {:ok, id} -> collect(id, request)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  # `stream: :self` streams only 2xx-with-a-body; every other response arrives
  # whole, which is what makes 304 and the error statuses cheap.
  defp collect(id, request) do
    receive do
      {:http, {^id, :stream_start, headers}} ->
        collect_body(id, request, etag(headers), [], 0)

      {:http, {^id, {{_v, 304, _}, _headers, _body}}} ->
        {:ok, :not_modified}

      {:http, {^id, {{_v, status, _}, headers, body}}} when status >= 200 and status < 300 ->
        {:ok, body, etag(headers)}

      {:http, {^id, {{_v, status, _}, _headers, _body}}} ->
        {:error, {:http_status, status}}

      {:http, {^id, {:error, reason}}} ->
        {:error, reason}
    after
      request.timeout_ms -> cancel(id, :timeout)
    end
  end

  # The cap is enforced as the body arrives and the request is cancelled at the
  # boundary, so an endpoint returning gigabytes costs the controller the cap,
  # not the response.
  defp collect_body(id, request, etag, acc, size) do
    receive do
      {:http, {^id, :stream, chunk}} ->
        size = size + byte_size(chunk)

        if size > request.max_body_bytes do
          cancel(id, {:oversized_body, size, request.max_body_bytes})
        else
          collect_body(id, request, etag, [acc, chunk], size)
        end

      {:http, {^id, :stream_end, _headers}} ->
        {:ok, IO.iodata_to_binary(acc), etag}

      {:http, {^id, {:error, reason}}} ->
        {:error, reason}
    after
      request.timeout_ms -> cancel(id, :timeout)
    end
  end

  defp cancel(id, reason) do
    :httpc.cancel_request(id)
    {:error, reason}
  end

  defp request_headers(request) do
    configured =
      Enum.map(request.headers, fn {k, v} ->
        {String.to_charlist(to_string(k)), String.to_charlist(to_string(v))}
      end)

    # Conditional GET saves the transfer when the server offers an ETag; the
    # local hash stays the injection gate, so a server that omits or ignores it
    # changes nothing but bandwidth.
    case request.etag do
      nil -> configured
      etag -> [{~c"if-none-match", String.to_charlist(etag)} | configured]
    end
  end

  defp etag(headers) do
    Enum.find_value(headers, fn {name, value} ->
      if String.downcase(to_string(name)) == "etag", do: to_string(value)
    end)
  end

  # Plain HTTP needs no TLS options; HTTPS is verified against the system CA
  # store, and a host with no usable store fails the poll rather than silently
  # dropping to an unverified connection.
  defp ssl_opts("http://" <> _), do: {:ok, []}

  defp ssl_opts(_https) do
    {:ok,
     [
       verify: :verify_peer,
       cacerts: :public_key.cacerts_get(),
       depth: 3,
       customize_hostname_check: [
         match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
       ]
     ]}
  rescue
    err -> {:error, {:no_ca_store, Exception.message(err)}}
  end
end
