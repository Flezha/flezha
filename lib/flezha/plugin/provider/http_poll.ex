defmodule Flezha.Plugin.Provider.HttpPoll do
  @moduledoc """
  Generic HTTP polling input source (a relay plugin): the member of the source
  set whose upstream is a slowly-changing HTTP resource rather than a socket.

  It GETs `:url` every `:interval_ms` and injects the body on `:channel` as an
  ordinary entry frame — but ONLY when the body's SHA-256 differs from the last
  delivered one, so a steady endpoint costs one request per interval and zero
  frames. The first successful poll always injects, which is what converges a
  just-started or handed-over relay within one interval; consumers must
  treat that seed idempotently.

  `:idle_after_ms` is the age of the last SETTLED poll past which the worker
  reports `{:unhealthy, {:poll_idle, age_ms}}`; `0` means three `:interval_ms`.

  Required opts `:url` and an entry channel, written `:channel` or `:channel_in`;
  the rest: `docs/public/plugins/http_poll.md`.
  """

  alias Flezha.Config.Durations
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.HttpPoll.PollWorker
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  @max_body_ceiling_bytes 64 * 1_048_576

  use ProviderBehaviour,
    feeds: true,
    shareable: true,
    options:
      [
        url: [type: :string, required: true, doc: "The resource to GET each interval"],
        headers: [type: :headers, default: %{}, doc: "Request headers, as a JSON object"],
        interval_ms: [type: :int, default: 60_000, doc: "Milliseconds between polls"],
        timeout_ms: [type: :int, default: 10_000, min: 1, doc: "Budget for one request"],
        max_body_bytes: [
          type: :int,
          default: 1_048_576,
          min: 1,
          max: @max_body_ceiling_bytes,
          doc: "Largest body accepted before the poll is refused"
        ]
      ] ++
        ProviderOptions.channel(:in, "response body") ++
        ProviderOptions.plugin_id("the id it was enabled under") ++
        ProviderOptions.failure_threshold() ++ ProviderOptions.idle_after(),
    worker: [module: PollWorker, restart: :temporary]

  @doc """
  Validate and normalise. Deliberately performs no upstream I/O — the first
  poll is the worker's, and the worker is started by the extension table at
  activation, so a slow or unreachable endpoint cannot stall the relay's
  plugin preparation.
  """
  @impl true
  def init_provider(opts) do
    with :ok <- ProviderBehaviour.require_entry_channel(opts, __MODULE__),
         {:ok, raw_url} <- render_partition_placeholders(opts.url, opts),
         {:ok, url} <- HttpFetch.validate_url(raw_url, "http_poll"),
         :ok <- validate_interval(opts.interval_ms),
         :ok <- validate_budget(opts) do
      {:ok,
       Map.merge(opts, %{
         url: url,
         plugin_id: opts.plugin_id || opts[:source_plugin_id] || redact(url)
       })}
    end
  end

  @doc """
  Teardown is the host's: it kills the pid `activate/1` returned, on both
  unsubscribe and full teardown. This callback exists so the provider can be
  hooked at all — every hook requires the matching export, and a spec with no
  hooks is refused — and so a `:on_terminate` spec reads
  as the intent it is.
  """
  @impl true
  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  @doc """
  Health verdict from the poller's failure run and the age of its last settled
  poll. A single failed poll is not unhealthiness; `:failure_threshold`
  consecutive ones are, and the next success clears the run on the poll that
  reports it.

  The failure run alone is not enough: a poller that has stopped scheduling
  polls at all has a run of zero and a clean last error, so it read healthy
  while delivering nothing — and a source delivering nothing is
  indistinguishable downstream from a quiet stream, because every producing
  verdict is gated on input actually arriving. The idle window closes that.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.worker_health(
      opts,
      # The outer budget must exceed the inner `PollWorker.probe/1` timeout, or
      # the two race and this provider's precise `:probe_timeout` is replaced by
      # the generic budget one.
      [
        absent: :no_poller,
        dead: :poller_dead,
        budget_ms: 2 * ProviderBehaviour.probe_budget_ms()
      ],
      &poller_health(&1, opts)
    )
  end

  # The failure run outranks the idle age when both apply: it names the actual
  # error, and the age only says nothing has happened.
  defp poller_health(pid, opts) do
    case PollWorker.probe(pid) do
      {:ok, {failures, last_error, _last_settled_at}} when failures >= opts.failure_threshold ->
        {:unhealthy, {:poll_failing, last_error}}

      {:ok, {_failures, _last_error, last_settled_at}} ->
        ProviderBehaviour.idle_verdict(last_settled_at, opts.idle_after_ms, opts.interval_ms)

      # The worker is alive but did not answer inside the probe budget: an
      # UNCERTAIN probe, not a verdict. Reported upward rather than swallowed —
      # `Extension.Table` is the layer that can name the plugin.
      :timeout ->
        {:fail_open, :probe_timeout}
    end
  end

  @doc "See `Flezha.Plugin.Provider.HttpFetch.redact/1`."
  @spec redact(binary()) :: binary()
  defdelegate redact(url), to: HttpFetch

  # ---------- attach-time refusals ----------

  @partition_placeholders %{
    "{{partition_index}}" => :partition_index,
    "{{partition_count}}" => :partition_count
  }

  # Substitute the partition placeholders exactly once, here, so the poll
  # worker needs no partition knowledge at all and validation sees the URL that
  # will actually be requested — a substitution landing in the host position
  # must not slip past it.
  defp render_partition_placeholders(url, opts) when is_binary(url) do
    rendered =
      Enum.reduce(@partition_placeholders, url, fn {placeholder, key}, acc ->
        case Map.get(opts, key) do
          value when is_integer(value) ->
            String.replace(acc, placeholder, Integer.to_string(value))

          _ ->
            acc
        end
      end)

    case Enum.find(Map.keys(@partition_placeholders), &String.contains?(rendered, &1)) do
      nil ->
        {:ok, rendered}

      unresolved ->
        {:error, {:partition_placeholder_unfilled, :url, unresolved}}
    end
  end

  defp render_partition_placeholders(url, _opts), do: {:ok, url}

  # A sub-second interval is a hammer, not a poll. It is the one bound that is
  # not a row: its floor is a user-settable duration, read here rather
  # than frozen into the declaration at compile time.
  defp validate_interval(value) do
    minimum = Durations.get(:http_poll_min_interval_ms)

    if value < minimum,
      do: ProviderOptions.refuse_below(:interval_ms, value, minimum),
      else: :ok
  end

  # A request allowed to outlast its own interval means polls overlap or the
  # cadence silently becomes the timeout — and a black-holing endpoint would
  # keep a request outstanding essentially always.
  defp validate_budget(%{timeout_ms: timeout, interval_ms: interval}) when timeout >= interval,
    do: {:error, {:timeout_not_below_interval, timeout, interval}}

  defp validate_budget(_), do: :ok
end
