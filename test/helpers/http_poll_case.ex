defmodule FlezhaTest.HttpPollCase do
  @moduledoc """
  The fixture an `HttpPoll` test drives: a real `:gen_tcp` origin, a poller
  pointed at it, and the telemetry forward that reports what each poll did.

  The host relay is stood in for by the test process, so injection is a
  `{:ingest, body, metadata}` **call** that `take_ingest/2` answers with any
  verdict a real Overwatch could return — including the refusals that must
  leave the content undelivered and re-offered.
  """

  use ExUnit.CaseTemplate

  alias Flezha.Plugin.Provider.HttpPoll
  alias Flezha.Plugin.Provider.HttpPoll.PollWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias FlezhaTest.Helper.ProviderPrepare
  alias FlezhaTest.Helper.WebhookServer

  @channel "config_in"

  using do
    quote do
      import FlezhaTest.HttpPollCase

      import FlezhaTest.Helper.ProviderPrepare,
        only: [take_ingest: 0, take_ingest: 1, take_ingest: 2, refute_ingest: 0]

      @channel FlezhaTest.HttpPollCase.channel()
    end
  end

  setup do
    events = for verb <- [:shipped, :unchanged, :failed], do: [:flezha, :plugin, :http_poll, verb]

    FlezhaTest.Helper.Telemetry.capture(events,
      as: fn event, measurements, metadata ->
        {:poll_event, List.last(event), measurements, metadata}
      end
    )

    :ok
  end

  @doc "The entry channel every fixture poller injects into."
  def channel, do: @channel

  def base_opts(origin, extra \\ %{}) do
    Map.merge(
      %{
        url: "http://127.0.0.1:#{origin.port}/config",
        channel: @channel,
        interval_ms: 60_000,
        timeout_ms: 2_000
      },
      extra
    )
  end

  @doc """
  Prepare and start one poller the way the extension table does: the declaration
  normalises the options, the provider interprets them, and the declared worker
  is started under the provider's own family.
  """
  def start_poller(origin, extra \\ %{}) do
    {:ok, opts} = prepare(origin, extra)
    {:ok, poller} = start_worker(opts)
    stop_on_exit(poller)
    poller
  end

  @doc """
  The prepared options, refusals and all, without starting anything. The caller
  stands in for the host relay, exactly as the extension table does.
  """
  def prepare(origin, extra \\ %{}) do
    opts = Map.put(base_opts(origin, extra), :overwatcher_pid, self())

    with {:ok, opts} <- ProviderOptions.normalise_opts(HttpPoll, opts),
         do: HttpPoll.init_provider(opts)
  end

  @doc "Stop a poller the way the table does on disable, through its supervisor."
  def stop_worker(poller), do: ProviderPrepare.stop(HttpPoll, poller)

  @doc "Start the declared worker for `opts`, the way the table does at activation."
  def start_worker(opts) do
    [{key, worker_opts}] = HttpPoll.worker_specs(opts)

    WorkerSup.ensure_worker(
      WorkerSup.family(HttpPoll),
      key,
      {PollWorker, worker_opts},
      &Process.alive?/1
    )
  end

  def probe(poller), do: PollWorker.probe(poller)

  @doc "Settle the poller's own exit when the test ends; it stops with its host."
  def stop_on_exit(poller), do: ProviderPrepare.await_stop_on_exit(poller)

  # ---------- fake HTTP origin ----------

  @doc """
  An origin answering every request from `config` (`:status`, `:body`, `:etag`,
  `:location`, `:hold`): a held origin keeps each request in flight until
  `WebhookServer.release/0`, and a conditional request to a 200 with an ETag is
  answered 304, which is what makes the ETag pass-through observable.
  """
  def start_origin(config), do: WebhookServer.start([answer(config)])

  @doc "Answer from `config` from now on; what it leaves out takes its default."
  def set(origin, config), do: WebhookServer.set(origin, [answer(config)])

  def requests(origin), do: length(WebhookServer.requests(origin))

  def conditional_requests(origin),
    do: Enum.count(WebhookServer.requests(origin), &WebhookServer.conditional?/1)

  def last_request_headers(origin) do
    {_line, headers, _body} = List.last(WebhookServer.requests(origin), {"", "", ""})
    headers
  end

  defp answer(config) do
    config =
      Map.merge(%{status: 200, body: "", etag: nil, location: nil, hold: nil}, Map.new(config))

    headers =
      [{"etag", config.etag}, {"location", config.location}]
      |> Enum.reject(fn {_name, value} -> is_nil(value) end)
      |> Kernel.++([{"content-type", "application/octet-stream"}, {"connection", "close"}])

    answer = {config.status, headers, config.body}
    answer = if config.etag && config.status == 200, do: {:conditional, answer}, else: answer
    if config.hold, do: {:hold, answer}, else: answer
  end
end
