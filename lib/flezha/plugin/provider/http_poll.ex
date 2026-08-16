defmodule Flezha.Plugin.Provider.HttpPoll do
  @moduledoc """
  Generic HTTP polling input source (`:io_binding`): the member of the source
  set whose upstream is a slowly-changing HTTP resource rather than a socket.
  Attach it to feed runtime configuration, reference data, or any external
  state onto an entry channel without restarting the operative.

  It polls `:url` every `:interval_ms` and injects the response body as an
  ordinary entry frame on `:channel` — the same journaled route as
  `flz operative send` — but only when the body's SHA-256 differs from the
  last delivered one. A steady endpoint therefore costs one request per
  interval and zero frames.

  The first successful poll after the plugin starts always injects (the seed),
  which is what converges an operative that has just been started, recovered,
  or handed over onto current content within one interval. Consumers must
  treat these frames idempotently: the seed repeats content the pre-handover
  instance may already have seen.

  Lifecycle follows the two-phase provider contract — `init_provider/1`
  validates and normalises with no upstream I/O, `activate/1` starts the
  poller once the SDK side is bound. All HTTP waiting happens in that worker
  (`#{inspect(__MODULE__)}.PollWorker`), never on the host's dispatch path.

  Required opts:
    * `:url`     — `http://` or `https://` resource to GET
    * `:channel` — entry channel the body is injected on

  Optional opts (defaults below):
    * `:interval_ms`       — int, default `60_000` (minimum `1_000`)
    * `:timeout_ms`        — int, default `10_000` (must be under the interval)
    * `:max_body_bytes`    — int, default `1_048_576` (1 MiB, ceiling 64 MiB)
    * `:failure_threshold` — int, default `3` consecutive failures → unhealthy
    * `:headers`           — map of static request headers (JSON object via CLI)
    * `:plugin_id`         — label for telemetry/logs; defaults to the URL
      with its credentials and query stripped

  The `:channel` name is not checked against the operative's declared entry
  channels: channel names are an SDK-side topology fact and never reach the
  controller (`AddOperativeResponse.metadata` carries capability declarations
  only), and `init_provider/1` runs before the operative binds to a runner at
  all. A name the class does not declare surfaces as a dispatch failure on the
  runner, exactly as a mistyped `flz operative send --channel` does.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.HttpPoll.PollWorker
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  import ProviderBehaviour, only: [coerce_int: 1, parse_int: 2]

  require Logger

  # The one default table. `defstruct`, the option allow-list, and the worker
  # opts all derive from it, so a new tunable is added in one place.
  @int_opts [
    interval_ms: 60_000,
    timeout_ms: 10_000,
    max_body_bytes: 1_048_576,
    failure_threshold: 3
  ]

  @min_interval_ms 1_000
  @max_body_ceiling_bytes 64 * 1_048_576

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
  Health verdict from the poller's own failure run — no new liveness state.
  A single failed poll is not unhealthiness; `:failure_threshold` consecutive
  ones are, and the next success clears the run on the poll that reports it.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    threshold = coerce_int(Map.get(opts, :failure_threshold, @int_opts[:failure_threshold]))

    ProviderBehaviour.worker_health(
      opts,
      [absent: :no_poller, dead: :poller_dead],
      &health_from_failures(&1, threshold)
    )
  end

  defp health_from_failures(pid, threshold) do
    case PollWorker.probe(pid) do
      {:ok, {failures, last_error}} when failures >= threshold ->
        {:unhealthy, {:poll_failing, last_error}}

      # A wedged probe reads healthy so the reconciler does not escalate on
      # uncertainty — a genuinely dead poller is the `dead` branch, and requests
      # run off the worker so a slow endpoint cannot wedge it.
      _ ->
        :healthy
    end
  end

  @doc """
  The URL with credentials and query stripped — the only form that reaches a
  log line, a telemetry label, or an error string. Config endpoints routinely
  carry an API key in the query or userinfo, and every telemetry label is
  exported to the observability sinks.
  """
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
    Enum.reduce_while(@int_opts, {:ok, %{}}, fn {field, default}, {:ok, acc} ->
      case parse_int(Map.get(opts, field, default), field) do
        {:ok, value} ->
          case bound(field, value) do
            :ok -> {:cont, {:ok, Map.put(acc, field, value)}}
            {:error, _} = err -> {:halt, err}
          end

        {:error, reason} ->
          {:halt, {:error, "http_poll #{reason}"}}
      end
    end)
  end

  # A sub-second interval is a hammer, not a poll; an unbounded body cap
  # defeats the cap. Both are refused rather than clamped so a typo is visible.
  defp bound(:interval_ms, value) when value < @min_interval_ms,
    do: {:error, "http_poll interval_ms #{value} is below the #{@min_interval_ms} ms minimum"}

  defp bound(:max_body_bytes, value) when value > @max_body_ceiling_bytes,
    do:
      {:error,
       "http_poll max_body_bytes #{value} exceeds the #{@max_body_ceiling_bytes} byte ceiling"}

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

  @int_fields Keyword.keys(@int_opts)

  defp convert_value(field, value) when field in @int_fields, do: parse_int(value, field)
end
