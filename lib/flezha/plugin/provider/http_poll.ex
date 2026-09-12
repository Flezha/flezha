defmodule Flezha.Plugin.Provider.HttpPoll do
  @moduledoc """
  Generic HTTP polling input source (`:io_binding`): the member of the source
  set whose upstream is a slowly-changing HTTP resource rather than a socket.

  It GETs `:url` every `:interval_ms` and injects the body on `:channel` as an
  ordinary entry frame — but ONLY when the body's SHA-256 differs from the last
  delivered one, so a steady endpoint costs one request per interval and zero
  frames. The first successful poll always injects, which is what converges a
  just-started or handed-over operative within one interval; consumers must
  treat that seed idempotently.

  `:idle_after_ms` is the age of the last SETTLED poll past which the worker
  reports `{:unhealthy, {:poll_idle, age_ms}}`; `0` means three `:interval_ms`.

  Required opts `:url` and an entry channel, written `:channel` or `:channel_in`;
  the rest: `docs/plugins/http_poll.md`.
  """

  alias Flezha.Config.Durations
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.HttpPoll.PollWorker
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  # The derived idle window, in polling intervals. Three, because a poll that
  # outlives its own interval stretches the cadence rather than stacking, so a
  # single slow cycle plus the poll after it must both land inside the window,
  # otherwise a merely slow endpoint would read as a dead one.
  @idle_intervals 3

  @max_body_ceiling_bytes 64 * 1_048_576

  use ProviderBehaviour,
    feeds: true,
    options: [
      url: [type: :string, required: true, doc: "The resource to GET each interval"],
      # One of the two is required and a row can only say so of itself, so each
      # says it of the pair — this is what both operator surfaces render.
      channel: [
        type: :string,
        doc:
          "The channel this plugin is attached to: the entry channel the body is injected on. Required, unless `channel_in` names it"
      ],
      channel_in: [
        type: :string,
        doc:
          "Entry channel the body is injected on; the explicit spelling of `channel`. Required, unless `channel` names it"
      ],
      plugin_id: [type: :string, doc: "Plugin id; defaults to the redacted URL"],
      headers: [type: :json, default: %{}, doc: "Request headers, as a JSON object"],
      interval_ms: [type: :int, default: 60_000, doc: "Milliseconds between polls"],
      timeout_ms: [type: :int, default: 10_000, min: 1, doc: "Budget for one request"],
      max_body_bytes: [
        type: :int,
        default: 1_048_576,
        min: 1,
        max: @max_body_ceiling_bytes,
        doc: "Largest body accepted before the poll is refused"
      ],
      failure_threshold: [
        type: :int,
        default: 3,
        min: 1,
        doc: "Consecutive failed polls before the plugin reports unhealthy"
      ],
      idle_after_ms: [
        type: :int,
        default: 0,
        min: 0,
        doc:
          "Age of the last settled poll past which the source is idle; 0 derives it from `interval_ms`"
      ]
    ],
    worker: [module: PollWorker, restart: :temporary]

  # A channel written as an atom, which an Elixir-side spec may carry.
  #
  # An empty name used to be refused here and is now refused by the shared
  # resolution, for all three spellings on every provider. What that no longer
  # covers is `build/1`, the CLI struct builder, which answers `{:ok, %HttpPoll{
  # channel: ""}}` where this clause answered a refusal — the struct reaches
  # nothing (`flz plugin enable` builds a `%Plugin{}` and lets the table
  # normalise), and the enable path still refuses.
  defp convert_value(field, value) when field in [:channel, :channel_in] and is_atom(value),
    do: {:ok, Atom.to_string(value)}

  @doc """
  Validate and normalise. Deliberately performs no upstream I/O — the first
  poll is the worker's, and the worker is started by the extension table at
  activation, so a slow or unreachable endpoint cannot stall the operative's
  plugin preparation.
  """
  @impl true
  def init_provider(opts) do
    with :ok <- require_entry_channel(opts),
         {:ok, raw_url} <- render_partition_placeholders(opts.url, opts),
         {:ok, url} <- HttpFetch.validate_url(raw_url, "http_poll"),
         {:ok, headers} <- HttpFetch.validate_headers(opts.headers, "http_poll"),
         :ok <- validate_interval(opts.interval_ms),
         :ok <- validate_budget(opts) do
      {:ok,
       Map.merge(opts, %{
         url: url,
         headers: headers,
         plugin_id: opts.plugin_id || redact(url)
       })}
    end
  end

  # A poll with nowhere to deliver is not a poll. Either spelling names the
  # channel, so the requirement is on the resolved value rather than on one row.
  defp require_entry_channel(opts) do
    if opts[:entry_channel],
      do: :ok,
      else: {:error, "http_poll requires an entry channel: write channel or channel_in"}
  end

  # One worker, keyed by the plugin id `init_provider/1` settled. What it reads
  # is narrowed where its state is built (`PollWorker.init/1`), so the list of
  # fields it takes lives there and not in a second copy here.
  @impl true
  def worker_specs(opts), do: [{Map.fetch!(opts, :plugin_id), opts}]

  @doc """
  Teardown is the host's: it kills the pid `activate/1` returned, on both
  unsubscribe and full teardown. This callback exists so the provider is
  subscribable at all — every subscription requires the matching export, and a
  spec with no subscriptions is refused — and so a `:on_terminate` spec reads
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
      &poller_health(&1, opts.failure_threshold, idle_window(opts))
    )
  end

  # The window an operator did not set is derived from the cadence they did.
  defp idle_window(%{idle_after_ms: ms}) when ms > 0, do: ms
  defp idle_window(opts), do: @idle_intervals * opts.interval_ms

  # The failure run outranks the idle age when both apply: it names the actual
  # error, and the age only says nothing has happened.
  defp poller_health(pid, threshold, idle_window) do
    case PollWorker.probe(pid) do
      {:ok, {failures, last_error, _last_settled_at}} when failures >= threshold ->
        {:unhealthy, {:poll_failing, last_error}}

      {:ok, {_failures, _last_error, last_settled_at}} ->
        idle_verdict(last_settled_at, idle_window)

      # The worker is alive but did not answer inside the probe budget: an
      # UNCERTAIN probe, not a verdict. Reported upward rather than swallowed —
      # `Extension.Table` is the layer that can name the plugin.
      :timeout ->
        {:fail_open, :probe_timeout}
    end
  end

  defp idle_verdict(last_settled_at, idle_window) do
    age = :erlang.monotonic_time(:millisecond) - last_settled_at

    if age > idle_window, do: {:unhealthy, {:poll_idle, age}}, else: :healthy
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
        {:error,
         "http_poll url carries #{unresolved} but this plugin has no partition identity — " <>
           "the placeholder is filled by a partitioned start's plugin fan-out"}
    end
  end

  defp render_partition_placeholders(url, _opts), do: {:ok, url}

  # A sub-second interval is a hammer, not a poll. It is the one bound that is
  # not a row: its floor is an operator-settable duration, read here rather
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
    do:
      {:error,
       "http_poll timeout_ms #{timeout} must be below interval_ms #{interval} so a poll " <>
         "cannot outlast its own interval"}

  defp validate_budget(_), do: :ok
end
