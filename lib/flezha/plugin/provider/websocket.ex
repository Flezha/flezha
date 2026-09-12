defmodule Flezha.Plugin.Provider.Websocket do
  @moduledoc """
  First-party Websocket provider (`:io_binding`). Its declared worker is a Fresh
  connection the table starts at activation; inbound frames reach the operative
  through the host's one inject.

  A two-tier watchdog separates CONNECTION liveness from DATA liveness: every
  inbound frame refreshes `last_frame_at`, but only non-`ignore_pattern` data
  frames refresh `last_data_frame_at`. So a heartbeat holds the socket open
  while a silent feed still triggers an in-place re-subscribe. The socket
  itself is `Websocket.Client`; what it asserts upstream is
  `Websocket.Subscription`.

  Required opts: `:uri`. Everything else, the subscription source and the
  telemetry events: `docs/plugins/websocket_subscription.md`.
  """

  alias Flezha.Config.Durations
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Websocket.Client
  alias Flezha.Plugin.Provider.Websocket.Sink
  alias Flezha.Plugin.Provider.Websocket.Subscription
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox

  require Logger

  @frame_types [:text, :binary]

  # The three timing defaults the declaration reads. Each is resolved once, at
  # compile time, from the table that owns the duration.
  @default_idle_timeout_ms Client.default_idle_timeout_ms()
  @default_resubscribe_after_ms Client.default_resubscribe_after_ms()
  @default_subscription_interval_ms Subscription.default_interval_ms()

  # The two "milliseconds, non-negative, 0 disables" knobs and their fallbacks,
  # in one table. Both are clamped rather than refused and both fall back on a
  # value that names no number: a socket knob an operator mistyped must not take
  # the connection down with it.
  @ms_defaults [
    idle_timeout_ms: @default_idle_timeout_ms,
    resubscribe_after_ms: @default_resubscribe_after_ms
  ]
  @ms_fields Keyword.keys(@ms_defaults)

  use ProviderBehaviour,
    feeds: true,
    options: [
      uri: [type: :string, required: true, doc: "The websocket endpoint to connect to"],
      plugin_id: [type: :string, doc: "Plugin id; defaults to the URI"],
      ignore_pattern: [
        type: :string,
        doc: "Substring marking a frame as a heartbeat rather than data"
      ],
      post_connect: [type: :json, default: %{}, doc: "Frame sent once the socket opens"],
      ws_opts: [type: :json, default: [], doc: "Options passed to the socket library"],
      payload_metadata: [type: :json, default: %{}, doc: "Metadata merged into every injection"],
      idle_timeout_ms: [
        type: :int,
        default: @default_idle_timeout_ms,
        doc: "Silence on the connection before it is reopened; 0 disables"
      ],
      resubscribe_after_ms: [
        type: :int,
        default: @default_resubscribe_after_ms,
        doc: "Silence of DATA frames before the subscription is reasserted; 0 disables"
      ],
      subscription_url: [type: :string, doc: "Endpoint the desired item set is polled from"],
      subscription_items: [type: :list, doc: "Literal desired item set, instead of polling one"],
      subscription_interval_ms: [
        type: :int,
        default: @default_subscription_interval_ms,
        doc: "How often the item source is polled"
      ],
      subscription_headers: [type: :json, default: %{}, doc: "Headers for the item source poll"],
      subscribe_template: [type: :string, doc: "Frame template carrying the items to subscribe"],
      unsubscribe_template: [type: :string, doc: "Frame template carrying the items to drop"],
      subscription_ack_mode: [
        type: :enum,
        values: ["required", "none"],
        default: "required",
        doc: "`\"none\"` commits a delta on successful send instead of on the server's ack"
      ],
      channel: [
        type: :string,
        doc:
          "The channel this plugin is attached to: inbound frames are injected on it and only outputs naming it are sent"
      ],
      channel_in: [
        type: :string,
        doc: "Entry channel inbound frames are injected on, for a plugin whose two names differ"
      ],
      channel_out: [
        type: :string,
        doc:
          "Send only outputs whose metadata channel matches, for a plugin whose two names differ"
      ],
      sink_frame_type: [
        type: :enum,
        values: ["text", "binary"],
        default: "text",
        doc: "Websocket frame type an output is sent as"
      ],
      sink_template: [
        type: :string,
        doc:
          "Frame template carrying the output; {{payload}} required, {{req_id}}/{{entity}}/{{seq}} available"
      ],
      sink_queue_max: [
        type: :int,
        default: 10_000,
        min: 1,
        doc: "Outbound frames held while the socket is down before the caller blocks"
      ],
      sink_ack_mode: [
        type: :enum,
        values: ["none", "required"],
        default: "none",
        doc: "`\"required\"` answers the outbox hold on the upstream's ack instead of at handoff"
      ],
      # The set this plugin last committed, read back from the cspec on a
      # restart rather than an option an operator writes.
      applied_subscriptions: [type: :any, internal: true],
      # The reconcile plan `init_provider/1` derives from the rows above.
      subscription: [type: :any, internal: true],
      # The outbound half, derived from the sink rows and the directions this
      # plugin was actually subscribed to.
      sink: [type: :any, internal: true]
    ],
    worker: [module: Client, restart: :temporary]

  defp convert_value(field, value) when field in @ms_fields and is_integer(value),
    do: {:ok, max(value, 0)}

  defp convert_value(field, value) when field in @ms_fields and is_binary(value) do
    case Integer.parse(value) do
      {n, ""} ->
        {:ok, max(n, 0)}

      _ ->
        default = Keyword.fetch!(@ms_defaults, field)

        Logger.warning(
          "[Plugin.Provider.Websocket] invalid #{field}=#{inspect(value)}, " <>
            "falling back to default #{default} ms"
        )

        {:ok, default}
    end
  end

  # A value that is neither takes the default, for the same reason.
  defp convert_value(field, _value) when field in @ms_fields,
    do: {:ok, Keyword.fetch!(@ms_defaults, field)}

  # `handle_connect/3` indexes `state.post_connect["payload"]`, so the value must
  # be a map; a CLI flag delivers it as a string. The frame type is a two-value
  # enum that `Subscription.subscription_frames/1` reads with no check of its
  # own, so it is closed here rather than silently read as `:text`.
  defp convert_value(:post_connect, value) when is_binary(value) and value != "" do
    case Jason.decode(value) do
      {:ok, %{} = decoded} -> convert_value(:post_connect, decoded)
      _ -> ProviderBehaviour.refuse_value(:post_connect, value)
    end
  end

  defp convert_value(:post_connect, %{"payload" => %{"type" => type}} = value) do
    with {:ok, _frame} <- ProviderBehaviour.parse_enum(type, "post_connect type", @frame_types),
         do: {:ok, value}
  end

  defp convert_value(:post_connect, %{} = value), do: {:ok, value}
  defp convert_value(:post_connect, ""), do: {:ok, %{}}

  # An empty pattern is no pattern: every data frame counts as data.
  defp convert_value(:ignore_pattern, value) when is_binary(value) and value != "",
    do: {:ok, value}

  defp convert_value(:ignore_pattern, _), do: {:ok, nil}

  # `init_provider/1` validates and normalises config; it does NOT open the
  # upstream WebSocket. The TLS handshake and the subscribe round trip are the
  # worker's, started by `Flezha.Plugin.Extension.Table` at activation once the
  # operative's SDK side has confirmed bound. See `operative-extensions`
  # "Two-phase plugin lifecycle gated on SDK readiness".
  @impl true
  def init_provider(opts) do
    with :ok <- one_channel_writer(opts),
         {:ok, subscription} <- Subscription.from_opts(opts),
         {:ok, sink} <- Sink.from_opts(opts) do
      plugin_id = opts.plugin_id || opts.uri
      warn_best_effort_hop(opts, sink, plugin_id)

      Logger.info(
        "[Plugin.Provider.Websocket] init_provider prepared uri=#{inspect(opts.uri)} " <>
          "plugin_id=#{inspect(plugin_id)} subscription_source=#{subscription != nil} " <>
          "sink=#{sink != nil}"
      )

      {:ok,
       Map.merge(opts, %{
         plugin_id: plugin_id,
         subscription: subscription,
         sink: sink,
         applied_subscriptions: Subscription.applied_set(opts.applied_subscriptions)
       })}
    end
  end

  # `payload_metadata` stays the general label bag and its own "channel" key
  # still selects the entry channel. Supplying it together with a channel option
  # puts two writers on one key, and which of them won would be an
  # implementation detail rather than a decision anybody made.
  defp one_channel_writer(%{payload_metadata: %{"channel" => bag}} = opts) do
    case opts[:entry_channel] || opts[:filter_channel] do
      nil ->
        :ok

      option ->
        {:error,
         "websocket payload_metadata names channel #{inspect(bag)} and a channel option " <>
           "names #{inspect(option)}; write one of the two"}
    end
  end

  defp one_channel_writer(_opts), do: :ok

  # Warn, never refuse. A durable sink alongside a live websocket view is the
  # standard topology, and refusing it would push operators into a worse one.
  # Refusing would also claim more than this change can honour: the operative's
  # internal guarantee is untouched and only the websocket hop is best-effort.
  defp warn_best_effort_hop(opts, %Sink{} = sink, plugin_id) do
    if opts[:effectively_once] do
      Logger.warning(
        "[Plugin.Provider.Websocket] plugin_id=#{inspect(plugin_id)} ships output from an " <>
          "effectively-once operative with sink_ack_mode=#{inspect(sink.ack_mode)}; the " <>
          "operative's internal guarantee is unchanged, its websocket output hop is not " <>
          "effectively-once"
      )
    end

    :ok
  end

  defp warn_best_effort_hop(_opts, _sink, _plugin_id), do: :ok

  @doc """
  Ship one dispatched payload upstream on the socket this plugin already holds.

  The handoff is a correlated ASYNCHRONOUS ask, never a call. On the
  effectively-once tier the socket process confirms an injected input by calling
  the Overwatch without a deadline, and the Overwatch is the process running
  this chain: a synchronous send in this direction closes that cycle and hangs
  the operative permanently. The bounded wait is what keeps the cycle open, and
  a frame that outlives it is still safely in the socket's hands.
  """
  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: ship(ctx, opts)

  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: ship(ctx, opts)

  defp ship(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
    with true <- ProviderBehaviour.in_channel?(md, opts.filter_channel),
         pid when is_pid(pid) <- WorkerSup.worker_pid(__MODULE__, opts.plugin_id) do
      # Take ownership of a delivery-guaranteed output before handing it over:
      # the socket answers the hold at handoff or on the upstream's ack, by
      # `sink_ack_mode`. A plain output holds nothing and this is a no-op.
      Outbox.hold(md)
      handed_off(ctx, opts, ProviderBehaviour.ask(pid, {:sink_frame, msg, md}, handoff_budget()))
    else
      # Another channel's output passes through untouched; no socket means no
      # hold was taken, so refusing the caller — one of the three answers the
      # spec allows for a frame that is not sent — leaves nothing owing.
      false -> {:ok, ctx}
      nil -> {:error, {:no_connection, opts.plugin_id}}
    end
  end

  defp handed_off(ctx, _opts, {:ok, :ok}), do: {:ok, ctx}

  defp handed_off(ctx, opts, :timeout) do
    # The frame is in the socket's mailbox and is queued when it is read; what
    # ran out is this chain's willingness to wait, which is what keeps the
    # confirming-ingest cycle from closing.
    Logger.warning(
      "[Plugin.Provider.Websocket] outbound handoff budget spent for plugin_id=" <>
        "#{inspect(opts.plugin_id)}; the socket is saturated or busy confirming an input"
    )

    {:ok, ctx}
  end

  defp handoff_budget(), do: Durations.get(:ws_sink_handoff_timeout_ms)

  @impl true
  def worker_specs(opts) do
    [
      {Map.fetch!(opts, :plugin_id),
       opts
       |> Map.take([
         :uri,
         :plugin_id,
         :ws_opts,
         :post_connect,
         :overwatcher_pid,
         :idle_timeout_ms,
         :resubscribe_after_ms,
         :ignore_pattern,
         :subscription,
         :sink,
         :applied_subscriptions,
         :source_plugin_id,
         :source_namespace,
         :source_operative_id
       ])
       |> Map.put(:sync_ingest?, opts[:effectively_once] || false)
       |> Map.put(:payload_metadata, ingest_metadata(opts))}
    ]
  end

  # The resolved entry channel rides the bag every injection already carries, so
  # the socket holds one map and the frame path stays one send. Nothing can
  # overwrite the operator's own "channel" key: naming it both ways is refused
  # when the plugin is enabled.
  #
  # Both keys are read by access, never by pattern: the teardown path asks for
  # the declared worker keys from a manifest's RAW options, where neither the
  # host's resolution nor the row's default has been applied.
  defp ingest_metadata(opts) do
    bag = Map.get(opts, :payload_metadata) || %{}

    case opts[:entry_channel] do
      nil -> bag
      channel -> Map.put(bag, "channel", channel)
    end
  end

  @doc """
  Health verdict reusing the two-tier watchdog's canonical timestamps — no new
  liveness state. Healthy means the connection process is alive and within its
  `idle_timeout_ms` (connection liveness) and, when `resubscribe_after_ms` is
  configured, within its data window (data liveness). The in-process watchdog
  (idle reconnect / resubscribe) remains the fast path; this contract only lets
  the reconciler escalate after it has repeatedly failed.

  A stalled subscription source is deliberately NOT unhealthy: the socket keeps
  streaming what it already applied, and tearing it down would lose that to fix
  nothing. The failure is reported through `inspect_subscription/1` instead.
  """
  @impl ProviderBehaviour
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.worker_health(
      opts,
      [absent: :no_connection, dead: :not_alive],
      &health_from_liveness(&1, opts.idle_timeout_ms, opts.resubscribe_after_ms)
    )
  end

  defp health_from_liveness(pid, idle, resub) do
    case ProviderBehaviour.ask(pid, :liveness_probe) do
      {:ok, {frame_age, data_age}} ->
        cond do
          idle > 0 and frame_age >= idle -> {:unhealthy, :connection_idle}
          resub > 0 and data_age >= resub -> {:unhealthy, :data_idle}
          true -> :healthy
        end

      # A wedged probe does not escalate — an uncertain probe never drives a
      # heal — but it is reported rather than swallowed, so `Extension.Table`
      # can name the plugin that stopped answering. A genuinely dead pid is
      # caught by `worker_health/3`.
      :timeout ->
        {:fail_open, :probe_timeout}
    end
  end

  @doc """
  The reconcile loop's operator-facing state: the applied set, the outcome of
  the last reconcile and the last error. `nil` for a plugin with no
  subscription source — there is nothing an operator needs to see about a
  static one that its configuration does not already say.
  """
  @impl ProviderBehaviour
  @spec inspect_state(map()) :: map() | nil
  def inspect_state(opts) do
    pid = ProviderBehaviour.worker_pid(opts)

    cond do
      is_nil(Map.get(opts, :subscription)) ->
        nil

      not (is_pid(pid) and Process.alive?(pid)) ->
        last_known(opts, :no_connection)

      true ->
        case ProviderBehaviour.ask(pid, :subscription_probe) do
          {:ok, report} -> report
          :timeout -> last_known(opts, :probe_timeout)
        end
    end
  end

  # An unreachable connection process still has a last-known applied set on its
  # stored opts; reporting that beats reporting nothing.
  # Same key set as `subscription_report/1`: one callback must not report a
  # different shape depending on whether its process happened to answer, or an
  # operator reads a missing field as a missing capability.
  defp last_known(opts, reason) do
    %{
      applied: opts[:applied_subscriptions] || [],
      desired: nil,
      inflight: 0,
      last_reconcile: nil,
      last_error: reason
    }
  end

  @doc "The watchdog tick interval for `state`, or `nil` when both tiers are off."
  @spec idle_tick_interval(map()) :: pos_integer() | nil
  defdelegate idle_tick_interval(state), to: Client

  # :on_interval tick triggers nothing; connection is driven by the Fresh pid.
  @impl true
  def on_interval(%Ctx.Tick{}, _opts), do: :ok

  # The socket is the declared worker, so the table stops it once. This callback
  # exists so a `:on_terminate` subscription is accepted at all.
  @impl true
  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok
end
