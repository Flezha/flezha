defmodule Flezha.Plugin.Provider.HttpPoll do
  @moduledoc """
  Generic HTTP polling input source (an operator plugin): the member of the source
  set whose upstream is a slowly-changing HTTP resource rather than a socket.

  It GETs `:url` every `:interval_ms` and injects the body on `:channel` as an
  ordinary entry frame — but ONLY when the body's SHA-256 differs from the last
  delivered one, so a steady endpoint costs one request per interval and zero
  frames. The first successful poll always injects, which is what converges a
  just-started or handed-over operator within one interval; consumers must
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

  @max_body_ceiling_bytes 64 * 1_048_576

  use ProviderBehaviour,
    feeds: true,
    shareable: true,
    options: [
      url: [type: :string, required: true, doc: "The resource to GET each interval"],
      # One of the two is required and a row can only say so of itself, so each
      # says it of the pair — this is what both user surfaces render.
      channel: [
        type: :names,
        doc:
          "The channel this plugin is attached to: the entry channel the body is injected on, and one name. Required, unless `channel_in` names it"
      ],
      channel_in: [
        type: :names,
        doc:
          "Entry channel the body is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it"
      ],
      plugin_id: [type: :string, doc: "Plugin id; defaults to the id it was enabled under"],
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

  # A channel written as an atom, which an Elixir-side spec may carry. It hands
  # the name back to the row rather than answering a binary of its own, so every
  # spelling reaches the option's type and resolves to the same shape.
  #
  # An empty name was refused by a clause here, then by the shared channel
  # resolution, and is now refused by the row's `:names` type. That is wider than
  # either: it reaches `build/1`, the CLI struct builder the resolution never
  # ran for.
  defp convert_value(field, value) when field in [:channel, :channel_in] and is_atom(value),
    do: convert_value(field, Atom.to_string(value))

  @doc """
  Validate and normalise. Deliberately performs no upstream I/O — the first
  poll is the worker's, and the worker is started by the extension table at
  activation, so a slow or unreachable endpoint cannot stall the operator's
  plugin preparation.
  """
  @impl true
  def init_provider(opts) do
    with :ok <- ProviderBehaviour.require_entry_channel(opts, __MODULE__),
         {:ok, raw_url} <- render_partition_placeholders(opts.url, opts),
         {:ok, url} <- HttpFetch.validate_url(raw_url, "http_poll"),
         {:ok, headers} <- HttpFetch.validate_headers(opts.headers, "http_poll"),
         :ok <- validate_interval(opts.interval_ms),
         :ok <- validate_budget(opts) do
      {:ok,
       Map.merge(opts, %{
         url: url,
         headers: headers,
         plugin_id: opts.plugin_id || opts[:source_plugin_id] || redact(url)
       })}
    end
  end

  # One worker, keyed by the plugin id `init_provider/1` settled. What it reads
  # is narrowed where its state is built (`PollWorker.init/1`), so the list of
  # fields it takes lives there and not in a second copy here.
  @impl true
  def worker_specs(opts), do: [{Map.fetch!(opts, :plugin_id), opts}]

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
