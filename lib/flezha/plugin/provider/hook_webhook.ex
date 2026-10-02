defmodule Flezha.Plugin.Provider.HookWebhook do
  @moduledoc """
  The first-party controller plugin: every hook event it is enabled for, POSTed
  as one JSON object to `url`. The event's top-level `text` makes it work with a
  Slack-style incoming webhook as it is.

  Each hook hands the event to the plugin's worker
  (`Flezha.Plugin.Provider.HttpSend`) and waits while it is sent, so the events
  wait in the copy's bounded queue, in order, and a slow receiver holds back
  only this plugin. A 2xx completes an event; a 429, a 5xx, a timeout or a
  transport error is retried after 1 s and then 5 s, up to `retries` times;
  anything else is dropped at once.

  Options are checked with the HTTP poll provider's rules
  (`Flezha.Plugin.Provider.HttpFetch`) and open no connection. Rest:
  `docs/plugins/hook_webhook.md`.
  """

  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.HttpSend
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour,
    type: :controller,
    options: [
      url: [
        type: :string,
        required: true,
        doc: "Where each event is POSTed: http or https, with a host and no credentials"
      ],
      headers: [type: :json, default: %{}, doc: "Request headers, as a JSON object; never logged"],
      timeout_ms: [type: :int, default: 5_000, min: 1, max: 60_000, doc: "Budget for one request"],
      retries: [
        type: :int,
        default: 2,
        min: 0,
        max: 5,
        doc: "Retries of a 429, a 5xx, a timeout or a transport error, after 1 s and then 5 s"
      ]
    ],
    worker: [module: HttpSend, restart: :transient]

  @impl true
  def init_provider(opts) do
    with {:ok, url} <- HttpFetch.validate_url(opts.url, "hook_webhook"),
         {:ok, headers} <- HttpFetch.validate_headers(opts.headers, "hook_webhook"),
         do: {:ok, %{opts | url: url, headers: headers}}
  end

  # One worker per plugin, which a name in another namespace does not share.
  @impl true
  def worker_specs(opts) do
    [
      {key(opts),
       Map.merge(opts, %{
         provider: :hook_webhook,
         content_type: "application/json",
         retry_statuses: [429],
         ladder: :hook_webhook_post
       })}
    ]
  end

  for hook <- ProviderBehaviour.controller_callbacks() do
    @impl true
    def unquote(hook)(event, opts),
      do: HttpSend.call(WorkerSup.worker(__MODULE__, key(opts)), Jason.encode!(event))
  end

  defp key(opts), do: {opts[:source_namespace], opts[:source_plugin_id]}
end
