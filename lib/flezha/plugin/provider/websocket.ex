defmodule Flezha.Plugin.Provider.Websocket do
  @moduledoc """
  First-party Websocket provider (`:io_binding`). `activate/1` starts a Fresh
  connection; inbound frames are cast to the overwatcher as `{:io_in, ...}`.

  A two-tier watchdog separates CONNECTION liveness from DATA liveness: every
  inbound frame refreshes `last_frame_at`, but only non-`ignore_pattern` data
  frames refresh `last_data_frame_at`. So a heartbeat holds the socket open
  while a silent feed still triggers an in-place re-subscribe. The socket
  itself is `Websocket.Client`; what it asserts upstream is
  `Websocket.Subscription`.

  Required opts: `:uri`. Everything else, the subscription source and the
  telemetry events: `docs/plugins/websocket_subscription.md`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Websocket.Client
  alias Flezha.Plugin.Provider.Websocket.Subscription
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  use ProviderBehaviour

  # Every "milliseconds, non-negative, 0 disables" option and its fallback, in
  # one table: `convert_value/2` and `ms_opt/2` both read it, so a new timing
  # knob is added here rather than as a fifth pair of near-identical clauses.
  @ms_defaults [
    idle_timeout_ms: Client.default_idle_timeout_ms(),
    resubscribe_after_ms: Client.default_resubscribe_after_ms()
  ]
  @ms_fields Keyword.keys(@ms_defaults)

  @frame_types [:text, :binary]

  # The value-guarded options. The `@ms_fields` and `:post_connect` clauses
  # are total on their own, so listing them would generate a clause that
  # never matches.
  @converted_opts [:ws_opts, :payload_metadata, :subscription_headers]

  @enforce_keys [:uri]
  defstruct @enforce_keys ++
              [
                :conn,
                :plugin_id,
                :ignore_pattern,
                :subscription_url,
                :subscribe_template,
                :unsubscribe_template,
                post_connect: %{},
                ws_opts: [],
                payload_metadata: %{},
                idle_timeout_ms: Client.default_idle_timeout_ms(),
                resubscribe_after_ms: Client.default_resubscribe_after_ms(),
                # Left unset: `Subscription.from_opts/1` owns every
                # subscription-source default, so there is one place to read
                # them off rather than two that can drift.
                subscription_interval_ms: nil,
                subscription_headers: %{},
                subscription_ack_mode: nil,
                subscription_items: nil,
                partition_index: nil,
                partition_count: nil,
                key_group_from: nil,
                key_group_to: nil
              ]

  @known_opts [
    :uri,
    :conn,
    :plugin_id,
    :ignore_pattern,
    :post_connect,
    :ws_opts,
    :payload_metadata,
    :idle_timeout_ms,
    :resubscribe_after_ms,
    :subscription_url,
    :subscription_interval_ms,
    :subscription_headers,
    :subscribe_template,
    :unsubscribe_template,
    :subscription_ack_mode,
    :subscription_items,
    # Injected by a partition group's plugin fan-out, not written by hand.
    :partition_index,
    :partition_count,
    :key_group_from,
    :key_group_to
  ]

  # This provider's own committed state, read back out of the cspec on a
  # restart rather than an option an operator writes. Accepted by
  # `init_provider/1` for exactly that reason; `build/1` never sees it, so no
  # CLI flag offers it.
  @internal_opts [:applied_subscriptions]

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:ws_opts, value) when is_binary(value) do
    Jason.decode(value)
  end

  defp convert_value(:payload_metadata, value) when is_binary(value) do
    Jason.decode(value)
  end

  # `handle_connect/3` indexes `state.post_connect["payload"]`, so the value must
  # be a map; CLI flags arrive as strings. The frame type is a two-value enum
  # that `Subscription.subscription_frames/1` reads with no check of its own, so
  # it is closed here rather than silently read as `:text`.
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
  defp convert_value(:post_connect, nil), do: {:ok, %{}}
  defp convert_value(:post_connect, ""), do: {:ok, %{}}

  defp convert_value(:post_connect, value),
    do: ProviderBehaviour.refuse_value(:post_connect, value)

  defp convert_value(:subscription_headers, value) when is_binary(value) do
    Jason.decode(value)
  end

  defp convert_value(:subscription_interval_ms, value) do
    ProviderBehaviour.parse_int(value, :subscription_interval_ms)
  end

  defp convert_value(field, value) when field in @ms_fields and is_integer(value) do
    {:ok, max(value, 0)}
  end

  defp convert_value(field, value) when field in @ms_fields and is_binary(value) do
    case Integer.parse(value) do
      {n, ""} ->
        {:ok, max(n, 0)}

      _ ->
        default = Keyword.fetch!(@ms_defaults, field)

        Logger.warning(
          "[Plugin.Provider.Websocket] invalid #{field}=#{inspect(value)} — " <>
            "falling back to default #{default} ms"
        )

        {:ok, default}
    end
  end

  # `ms_opt/2`'s own last clause: a value that is neither takes the default.
  defp convert_value(field, _value) when field in @ms_fields,
    do: {:ok, Keyword.fetch!(@ms_defaults, field)}

  defp convert_value(:ignore_pattern, value) when is_binary(value) and value != "" do
    {:ok, value}
  end

  defp convert_value(:ignore_pattern, _), do: {:ok, nil}

  # `init_provider/1` validates and normalises config; it does NOT open the
  # upstream WebSocket. The TLS handshake + subscribe round-trip moves to
  # `activate/1`, called by `Flezha.Plugin.Extension.Table.bulk_activate/1`
  # after the operative's SDK side has confirmed bound. See `operative-extensions`
  # "Two-phase plugin lifecycle gated on SDK readiness".
  def init_provider(opts) do
    with :ok <- validate_opts(opts),
         {:ok, post_connect} <- convert_value(:post_connect, Map.get(opts, :post_connect)),
         {:ok, tuned} <-
           ProviderBehaviour.convert_opts(__MODULE__, opts, @ms_fields, &convert_value/2),
         {:ok, subscription} <- Subscription.from_opts(opts) do
      uri = Map.fetch!(opts, :uri)
      ignore_pattern = normalize_ignore_pattern(Map.get(opts, :ignore_pattern))
      plugin_id = Map.get(opts, :plugin_id) || uri

      Logger.info(
        "[Plugin.Provider.Websocket] init_provider prepared uri=#{inspect(uri)} " <>
          "plugin_id=#{inspect(plugin_id)} subscription_source=#{subscription != nil}"
      )

      opts =
        opts
        |> Map.merge(tuned)
        |> Map.put(:overwatcher_pid, self())
        |> Map.put(:post_connect, post_connect)
        |> Map.put(:payload_metadata, Map.get(opts, :payload_metadata, %{}))
        |> Map.put(:ws_opts, Map.get(opts, :ws_opts, []))
        |> Map.put(:ignore_pattern, ignore_pattern)
        |> Map.put(:plugin_id, plugin_id)
        |> Map.put(:subscription, subscription)
        # The set this plugin last committed, read back from the cspec. A
        # restart resumes with it; a plugin that never committed starts empty.
        |> Map.put(
          :applied_subscriptions,
          Subscription.applied_set(Map.get(opts, :applied_subscriptions))
        )

      {:ok, opts}
    end
  end

  def activate(opts) when is_map(opts) do
    uri = Map.fetch!(opts, :uri)
    plugin_id = Map.fetch!(opts, :plugin_id)
    overwatcher_pid = Map.fetch!(opts, :overwatcher_pid)

    Logger.info(
      "[Plugin.Provider.Websocket] activate begin uri=#{inspect(uri)} plugin_id=#{inspect(plugin_id)}"
    )

    case Client.start_link(uri,
           ws_opts: Map.get(opts, :ws_opts, []),
           post_connect: Map.get(opts, :post_connect, %{}),
           overwatcher_pid: overwatcher_pid,
           payload_metadata: Map.get(opts, :payload_metadata, %{}),
           idle_timeout_ms: Map.get(opts, :idle_timeout_ms, Client.default_idle_timeout_ms()),
           resubscribe_after_ms:
             Map.get(opts, :resubscribe_after_ms, Client.default_resubscribe_after_ms()),
           ignore_pattern: Map.get(opts, :ignore_pattern),
           plugin_id: plugin_id,
           sync_ingest?: Map.get(opts, :effectively_once, false),
           subscription: Map.get(opts, :subscription),
           applied_subscriptions: Map.get(opts, :applied_subscriptions, []),
           source_plugin_id: Map.get(opts, :source_plugin_id),
           source_namespace: Map.get(opts, :source_namespace),
           source_operative_id: Map.get(opts, :source_operative_id)
         ) do
      {:ok, pid} ->
        Logger.info(
          "[Plugin.Provider.Websocket] activate success plugin_id=#{inspect(plugin_id)} pid=#{inspect(pid)}"
        )

        {:ok, pid}

      {:error, reason} = err ->
        Logger.error(
          "[Plugin.Provider.Websocket] activate failed plugin_id=#{inspect(plugin_id)} reason=#{inspect(reason)}"
        )

        err
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
    idle = Map.get(opts, :idle_timeout_ms, Client.default_idle_timeout_ms())
    resub = Map.get(opts, :resubscribe_after_ms, Client.default_resubscribe_after_ms())

    ProviderBehaviour.worker_health(
      opts,
      [absent: :no_connection, dead: :not_alive],
      &health_from_liveness(&1, idle, resub)
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
    pid = ProviderBehaviour.worker_pid(opts, [:owned_pid, :conn])

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
      applied: Map.get(opts, :applied_subscriptions, []),
      desired: nil,
      inflight: 0,
      last_reconcile: nil,
      last_error: reason
    }
  end

  @doc "The watchdog tick interval for `state`, or `nil` when both tiers are off."
  @spec idle_tick_interval(map()) :: pos_integer() | nil
  defdelegate idle_tick_interval(state), to: Client

  defp normalize_ignore_pattern(value) when is_binary(value) and value != "", do: value
  defp normalize_ignore_pattern(_), do: nil

  # :on_interval tick triggers nothing; connection is driven by the Fresh pid.
  def on_interval(%Ctx.Tick{}, _opts), do: :ok

  def on_terminate(%Ctx.Terminate{}, %{conn: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    :ok
  end

  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok
end
