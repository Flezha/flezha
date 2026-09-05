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

  Required opts `:url` and `:channel`; the rest: `docs/plugins/http_poll.md`.
  """

  alias Flezha.Config.Durations
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.HttpPoll.PollWorker
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  import ProviderBehaviour, only: [convert_opts: 4, parse_int: 2]

  require Logger

  # The one default table. `defstruct`, the option allow-list, and the worker
  # opts all derive from it, so a new tunable is added in one place.
  @int_opts [
    interval_ms: 60_000,
    timeout_ms: 10_000,
    max_body_bytes: 1_048_576,
    failure_threshold: 3,
    # 0 = derive from `:interval_ms` (see `@idle_intervals`). There is
    # deliberately no "disable" value: a source that has stopped settling polls
    # is the wedge this window exists to catch.
    idle_after_ms: 0
  ]

  # The derived idle window, in polling intervals. Three, because a poll that
  # outlives its own interval stretches the cadence rather than stacking, so a
  # single slow cycle plus the poll after it must both land inside the window —
  # otherwise a merely slow endpoint would read as a dead one.
  @idle_intervals 3

  @max_body_ceiling_bytes 64 * 1_048_576

  # Field-guarded and therefore already total; see `Postgres`.
  @converted_opts []

  @int_fields Keyword.keys(@int_opts)

  @enforce_keys [:url, :channel]
  defstruct @enforce_keys ++ [:plugin_id, :overwatcher_pid, headers: %{}] ++ @int_opts

  # Declaring `@known_opts` is what makes this provider strict: `use
  # ProviderBehaviour` generates both the `build/1` refusal and `validate_opts/1`.
  @known_opts [
                :url,
                :channel,
                :plugin_id,
                :headers,
                # Injected by a partition group's plugin fan-out, not written
                # by hand. Only the two rendered into the URL are read here.
                :partition_index,
                :partition_count,
                :key_group_from,
                :key_group_to
              ] ++ Keyword.keys(@int_opts)

  @impl true
  def plugin_type(), do: :io_binding

  @doc """
  Validate and normalise. Deliberately performs no upstream I/O — the first
  poll is the worker's, scheduled in `activate/1`, so a slow or unreachable
  endpoint cannot stall the operative's plugin preparation.

  This is where the option allow-list is enforced, not only in `build/1`:
  `Plugin.SpecDecoder` hands a `--plugins` spec straight here without ever
  constructing the provider struct, so a check that lived only in `build/1`
  would never run on the path an operator actually uses.
  """
  def init_provider(opts) do
    with :ok <- validate_opts(opts),
         {:ok, raw_url} <- render_partition_placeholders(Map.get(opts, :url), opts),
         {:ok, url} <- HttpFetch.validate_url(raw_url, "http_poll"),
         {:ok, channel} <- validate_channel(Map.get(opts, :channel)),
         {:ok, headers} <- HttpFetch.validate_headers(Map.get(opts, :headers), "http_poll"),
         {:ok, tunables} <- validate_tunables(opts),
         :ok <- validate_budget(tunables) do
      opts =
        opts
        |> Map.merge(tunables)
        |> Map.merge(%{
          url: url,
          channel: channel,
          headers: headers,
          plugin_id: Map.get(opts, :plugin_id) || redact(url),
          overwatcher_pid: self()
        })

      {:ok, opts}
    end
  end

  @doc """
  Start the poller. Called after the operative's SDK side confirms bound, so
  the seed frame lands on a channel that can carry it.
  """
  def activate(opts) when is_map(opts) do
    case PollWorker.start(opts) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, reason} = err ->
        Logger.error(
          "[Plugin.Provider.HttpPoll] activate failed plugin_id=" <>
            "#{inspect(Map.get(opts, :plugin_id))} reason=#{inspect(reason)}"
        )

        err
    end
  end

  @doc """
  Teardown is the host's: it kills the pid `activate/1` returned, on both
  unsubscribe and full teardown. This callback exists so the provider is
  subscribable at all — every subscription requires the matching export, and a
  spec with no subscriptions is refused — and so a `:on_terminate` spec reads
  as the intent it is.
  """
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
    threshold = Map.get(opts, :failure_threshold) || @int_opts[:failure_threshold]

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
      &poller_health(&1, threshold, idle_window(opts))
    )
  end

  # The window an operator did not set is derived from the cadence they did.
  defp idle_window(opts) do
    case Map.get(opts, :idle_after_ms, @int_opts[:idle_after_ms]) do
      ms when ms > 0 -> ms
      _ -> @idle_intervals * Map.get(opts, :interval_ms, @int_opts[:interval_ms])
    end
  end

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

  defp validate_channel(channel) when is_binary(channel) and channel != "", do: {:ok, channel}

  defp validate_channel(channel) when is_atom(channel) and not is_nil(channel),
    do: {:ok, Atom.to_string(channel)}

  defp validate_channel(_),
    do: {:error, "http_poll requires :channel — the entry channel to inject on"}

  defp validate_tunables(opts) do
    with {:ok, tuned} <- convert_opts(__MODULE__, opts, @int_fields, &convert_value/2),
         :ok <- Enum.find_value(tuned, :ok, &bound_failure/1) do
      {:ok, tuned}
    end
  end

  defp bound_failure({field, value}) do
    case bound(field, value) do
      :ok -> nil
      {:error, _} = err -> err
    end
  end

  # A sub-second interval is a hammer, not a poll; an unbounded body cap
  # defeats the cap. Both are refused rather than clamped so a typo is visible.
  defp bound(:interval_ms, value) do
    minimum = Durations.get(:http_poll_min_interval_ms)

    if value < minimum,
      do: {:error, "http_poll interval_ms #{value} is below the #{minimum} ms minimum"},
      else: :ok
  end

  defp bound(:max_body_bytes, value) when value > @max_body_ceiling_bytes,
    do:
      {:error,
       "http_poll max_body_bytes #{value} exceeds the #{@max_body_ceiling_bytes} byte ceiling"}

  # 0 is the documented "derive from :interval_ms" value for the idle window,
  # so it is the one field where zero is not a typo.
  defp bound(:idle_after_ms, 0), do: :ok

  defp bound(field, value) when value < 1,
    do: {:error, "http_poll #{field} must be positive, got #{value}"}

  defp bound(_field, _value), do: :ok

  # A request allowed to outlast its own interval means polls overlap or the
  # cadence silently becomes the timeout — and a black-holing endpoint would
  # keep a request outstanding essentially always.
  defp validate_budget(%{timeout_ms: timeout, interval_ms: interval}) when timeout >= interval,
    do:
      {:error,
       "http_poll timeout_ms #{timeout} must be below interval_ms #{interval} so a poll " <>
         "cannot outlast its own interval"}

  defp validate_budget(_), do: :ok

  defp convert_value(field, value) when field in @int_fields, do: parse_int(value, field)
end
