defmodule Flezha.Plugin.Provider.HttpPush do
  @moduledoc """
  The first-party HTTP sink (a relay plugin): each payload on a channel it
  sends, from `on_output` or `on_input`, as one request to `url`.

  The dispatch holds a delivery-guaranteed output and hands the payload to the
  plugin's worker (`Flezha.Plugin.Provider.HttpSend`), which sends it, retries
  it and confirms or parks it; a plain output holds nothing and is dropped once
  its retries are spent. Every relay bound to the plugin on a controller
  sends through one worker, keyed like `hook_webhook`'s so a namespace and a
  partition member never share another's.

  Options are checked with the HTTP poll provider's rules
  (`Flezha.Plugin.Provider.HttpFetch`), plus the headers a request sets itself
  and the ones it could not carry, and open no connection. Rest:
  `docs/public/plugins/http_push.md`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.HttpSend
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour

  # Resolved once: the dispatch looks its worker up here for every payload.
  @registry WorkerSup.registry_name(WorkerSup.family(__MODULE__))

  # Headers the request sets from its own options and its output's identity.
  @reserved ~w(content-type content-length host transfer-encoding connection idempotency-key)

  use ProviderBehaviour,
    shareable: true,
    options: [
      url: [
        type: :string,
        required: true,
        doc: "Where each payload is sent: http or https, with a host and no credentials"
      ],
      method: [type: :enum, values: ["POST", "PUT"], default: "POST", doc: "`POST` or `PUT`"],
      headers: [type: :json, default: %{}, doc: "Request headers, as a JSON object; never logged"],
      content_type: [
        type: :string,
        default: "application/octet-stream",
        doc: "The request's `content-type`"
      ],
      channel: [
        type: :names,
        doc:
          "The channel this plugin is attached to: only payloads naming it are sent. Several names, comma-separated, send them all"
      ],
      channel_out: [
        type: :names,
        doc: "Send only payloads whose metadata channel matches; the explicit spelling"
      ],
      timeout_ms: [
        type: :int,
        default: 10_000,
        min: 1,
        max: 300_000,
        doc: "Budget for one request, connect to last byte"
      ],
      max_in_flight: [
        type: :int,
        default: 1,
        min: 1,
        max: 64,
        doc: "Requests outstanding at once, one connection each; above 1 order is not kept"
      ],
      retries: [
        type: :int,
        default: 5,
        min: 0,
        max: 100,
        doc: "Retries of a 408, a 429, a 5xx, a timeout or a transport error"
      ],
      retry_backoff_ms: [type: :int, default: 250, min: 1, doc: "First delay between retries"],
      retry_max_backoff_ms: [
        type: :int,
        default: 10_000,
        min: 1,
        doc: "Ceiling for that delay, and for a `retry-after`"
      ],
      delivery_timeout_ms: [
        type: :int,
        default: 30_000,
        min: 1,
        doc: "Time from a payload's first attempt after which it is not retried again"
      ],
      queue_max: [type: :int, default: 10_000, min: 1, doc: "Payloads waiting to be sent"],
      queue_max_bytes: [
        type: :int,
        default: 67_108_864,
        min: 1,
        doc: "Payload bytes waiting to be sent"
      ],
      queue_overflow: [
        type: :enum,
        values: [:drop_nothing, :drop_oldest, :drop_newest],
        default: :drop_nothing,
        doc: "What a full queue does: make the dispatch wait, drop the oldest, or drop the newest"
      ],
      queue_block_timeout_ms: [
        type: :int,
        default: 5_000,
        min: 1,
        doc: "How long a dispatch waits on a full queue before the payload overflows"
      ],
      failure_threshold: [
        type: :int,
        default: 3,
        min: 1,
        doc: "Consecutive failed requests before the plugin reports unhealthy"
      ]
    ],
    worker: [module: HttpSend, restart: :transient]

  @impl true
  def init_provider(opts) do
    with {:ok, url} <- HttpFetch.validate_url(opts.url, "http_push"),
         {:ok, headers} <- HttpFetch.validate_headers(opts.headers, "http_push"),
         :ok <- sendable_headers(headers) do
      if sendable_value?(opts.content_type),
        do: {:ok, %{opts | url: url, headers: headers}},
        else: {:error, {:headers_invalid, "http_push", :content_type, "content-type"}}
    end
  end

  # Mint refuses a name that is no token and a value outside visible ASCII on
  # every request, so both are refused here, once, naming the header.
  defp sendable_headers(headers) do
    case Enum.find(headers, fn {name, value} -> not sendable?(name, value) end) do
      nil -> :ok
      {name, _value} -> {:error, {:headers_invalid, "http_push", :headers, name}}
    end
  end

  defp sendable?(name, value) do
    down = String.downcase(name)

    down not in @reserved and not String.starts_with?(down, "flz-") and
      name =~ ~r/\A[!#$%&'*+\-.^_`|~0-9A-Za-z]+\z/ and sendable_value?(value)
  end

  defp sendable_value?(value), do: value =~ ~r/\A[\x20-\x7E\t]*\z/

  @impl true
  def worker_specs(opts) do
    [
      {WorkerSup.key(opts),
       opts
       |> Map.take([
         :url,
         :method,
         :headers,
         :content_type,
         :timeout_ms,
         :retries,
         :max_in_flight,
         :delivery_timeout_ms,
         :queue_max,
         :queue_max_bytes,
         :queue_overflow,
         :queue_block_timeout_ms,
         :source_plugin_id
       ])
       |> Map.merge(%{
         provider: :http_push,
         retry_statuses: [408, 429],
         ladder: :http_push,
         ladder_opts: [base: opts.retry_backoff_ms, cap: opts.retry_max_backoff_ms]
       })}
    ]
  end

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: send_payload(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: send_payload(ctx, opts)

  defp send_payload(%Ctx.IO{message: message, metadata: metadata} = ctx, opts) do
    if ProviderBehaviour.in_channel?(metadata, opts.filter_channel),
      do: HttpSend.push(@registry, opts.worker_key, message, metadata)

    {:ok, ctx}
  end

  @doc """
  `{:unhealthy, :send_stalled}` once `failure_threshold` requests in a row have
  failed, healthy again on the next 2xx.
  """
  @impl true
  def health(opts) do
    ProviderBehaviour.pid_health(
      WorkerSup.worker_pid(__MODULE__, opts.worker_key),
      [absent: :not_running, dead: :worker_dead],
      fn pid ->
        case HttpSend.stats(pid) do
          %{failures: n} when n >= opts.failure_threshold -> {:unhealthy, :send_stalled}
          _ -> :healthy
        end
      end
    )
  end
end
