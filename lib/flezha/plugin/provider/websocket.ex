defmodule Flezha.Plugin.Provider.Websocket do
  @moduledoc """
  First-party Websocket provider. `init_provider/1` starts a Fresh WebSocket
  connection process; incoming frames are cast as `{:io_in, msg, metadata}`
  to the overwatcher pid. The plugin subscribes to `:on_terminate` so the
  connection is closed cleanly when the plugin is disabled.

  The plugin runs a two-tier application-level watchdog that separates
  *connection liveness* from *data liveness*:

    * `last_frame_at` is refreshed by **every** inbound frame — data
      frames (`handle_in/2`), `ignore_pattern`-matching heartbeats, and
      control frames (`handle_control/2` ping/pong). When
      `now - last_frame_at >= idle_timeout_ms` the connection is treated
      as dead: the Fresh process exits with `{:shutdown, :idle_timeout}`,
      the plugin manager observes the abnormal exit and restarts the
      plugin via its retry path (`Flezha.Plugin.Extension.Table.handle_down`).
    * `last_data_frame_at` is refreshed only by non-`ignore_pattern` data
      frames. When `resubscribe_after_ms > 0`, the connection is alive,
      and `now - last_data_frame_at >= resubscribe_after_ms`, the plugin
      resends its subscription on the existing connection (no teardown,
      no reconnect), rate-limited to once per window by
      `last_resubscribe_at`.

  A heartbeat — or any inbound frame — therefore holds the socket open
  even when no market data is flowing. `ignore_pattern` (string) still
  classifies a text data frame as heartbeat-shaped when its payload
  contains the pattern as a substring; such frames refresh only
  `last_frame_at`. Binary frames never match the pattern; when no pattern
  is configured every data frame counts as data.

  Per-frame liveness tracking is O(1) and allocation-free: each frame is
  classified exactly once against a `:binary.compile_pattern/1` matcher
  built at start, the monotonic clock is read once and reused for both
  timestamps, and every hot-path field is pre-initialised so updates use
  the known-key `%{state | …}` form.

  ## Subscription source

  What the socket is subscribed to is static by default: the `post_connect`
  payload, sent on connect and resent by the data-liveness tier. A plugin that
  configures `subscription_url` instead *converges* on a set published
  elsewhere, without ever reconnecting to change it.

  Every `subscription_interval_ms` the provider GETs `subscription_url` and
  reads the response as the desired set:

      {"version": 7, "items": ["BTC/USD", "ETH/USD"]}

  Item strings are opaque — the provider never parses, validates or interprets
  them. It computes `added = desired − applied` and `removed = applied −
  desired`, renders `subscribe_template` / `unsubscribe_template` with
  `{{items}}` substituted by the delta as a JSON array and `{{req_id}}` by a
  fresh integer, and sends those frames on the **existing** connection. All
  protocol vocabulary lives in the templates and in the source's response.

  A delta folds into the applied set only when an acknowledgement arrives:
  an inbound JSON object whose `"req_id"` equals the one sent, reporting
  success unless it carries `"error"` or `"success": false`. Failed, malformed
  and unanswered requests are simply not committed, so the next cycle
  re-derives an identical delta and retries — convergence is the retry
  mechanism, and there is no bespoke retry state. Protocols that echo no
  correlation id set `subscription_ack_mode: "none"`, which commits on
  successful send and still corrects drift one cycle later.

  The applied set is persisted to the plugin cspec after every commit, so a
  plugin restart, an operative handover and a controller restart all resume
  with what the socket was actually subscribed to. A failed poll is "no
  information", never an empty desired set: it can never cause an unsubscribe.

  Nothing above costs anything when `subscription_url` is unset — no poll
  timer is scheduled and the per-frame path carries no subscription state, so
  `handle_in/2` matches its ordinary clause on a state map that has no
  `:inflight` key at all.

  Each forced reconnect emits `[:flezha, :ws, :idle_reconnect]`
  (`%{silence_ms: integer()}`, `%{plugin_id: binary()}`); each in-place
  re-subscribe emits `[:flezha, :ws, :resubscribe]`
  (`%{data_silence_ms: integer()}`, `%{plugin_id: binary()}`); each reconcile
  that sent at least one frame emits `[:flezha, :ws, :reconcile]`
  (`%{added: integer(), removed: integer()}`, `%{plugin_id: binary()}`).

  Required opts: `:uri`.
  Optional opts: `:post_connect`, `:ws_opts`, `:payload_metadata`,
  `:idle_timeout_ms` (default 30_000; set to 0 to disable the connection
  watchdog), `:resubscribe_after_ms` (default 0 = disabled),
  `:ignore_pattern` (string substring, default unset),
  `:plugin_id` (label for telemetry/logs; defaults to `:uri`),
  `:subscription_url`, `:subscription_interval_ms` (default 30_000, minimum
  1_000), `:subscription_headers`, `:subscribe_template`,
  `:unsubscribe_template`, `:subscription_ack_mode` (`"required"` default, or
  `"none"`).
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.Websocket.Subscription
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  use Fresh
  use ProviderBehaviour

  @default_idle_timeout_ms 30_000
  @default_resubscribe_after_ms 0
  @min_tick_interval_ms 1_000

  # Every "milliseconds, non-negative, 0 disables" option and its fallback, in
  # one table: `convert_value/2` and `ms_opt/2` both read it, so a new timing
  # knob is added here rather than as a fifth pair of near-identical clauses.
  @ms_defaults [
    idle_timeout_ms: @default_idle_timeout_ms,
    resubscribe_after_ms: @default_resubscribe_after_ms
  ]
  @ms_fields Keyword.keys(@ms_defaults)

  # A re-assertion (connect, data-liveness re-subscribe) re-sends what is
  # ALREADY applied, so it has nothing to commit. It carries a reserved id
  # outside the in-flight space — which starts at 1 — so its acknowledgement,
  # however late, can never be mistaken for a pending delta's.
  @reassert_req_id 0

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
                idle_timeout_ms: @default_idle_timeout_ms,
                resubscribe_after_ms: @default_resubscribe_after_ms,
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

  # `handle_connect/3` indexes `state.post_connect["payload"]`, so the value
  # must be a map. CLI flags arrive as strings — decode them here.
  defp convert_value(:post_connect, value) when is_binary(value) and value != "" do
    Jason.decode(value)
  end

  defp convert_value(:post_connect, _), do: {:ok, %{}}

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

  defp convert_value(:ignore_pattern, value) when is_binary(value) and value != "" do
    {:ok, value}
  end

  defp convert_value(:ignore_pattern, _), do: {:ok, nil}

  # init_provider receives `opts` from Flezha.Plugin, where the value may be
  # a raw JSON string (legacy CLI path), a map (installer/new path), or nil.
  # Coerce all of those to a map so handle_connect/3 can safely index into it.
  defp normalize_post_connect(value) when is_map(value), do: value

  defp normalize_post_connect(value) when is_binary(value) and value != "" do
    case Jason.decode(value) do
      {:ok, decoded} when is_map(decoded) -> decoded
      _ -> %{}
    end
  end

  defp normalize_post_connect(_), do: %{}

  # `init_provider/1` validates and normalises config; it does NOT open the
  # upstream WebSocket. The TLS handshake + subscribe round-trip moves to
  # `activate/1`, called by `Flezha.Plugin.Extension.Table.bulk_activate/1`
  # after the operative's SDK side has confirmed bound. See `operative-extensions`
  # "Two-phase plugin lifecycle gated on SDK readiness".
  def init_provider(opts) do
    with :ok <- validate_opts(opts),
         {:ok, subscription} <- Subscription.from_opts(opts) do
      uri = Map.fetch!(opts, :uri)
      idle_timeout_ms = ms_opt(Map.get(opts, :idle_timeout_ms), @default_idle_timeout_ms)

      resubscribe_after_ms =
        ms_opt(Map.get(opts, :resubscribe_after_ms), @default_resubscribe_after_ms)

      ignore_pattern = normalize_ignore_pattern(Map.get(opts, :ignore_pattern))
      plugin_id = Map.get(opts, :plugin_id) || uri

      Logger.info(
        "[Plugin.Provider.Websocket] init_provider prepared uri=#{inspect(uri)} " <>
          "plugin_id=#{inspect(plugin_id)} subscription_source=#{subscription != nil}"
      )

      opts =
        opts
        |> Map.put(:overwatcher_pid, self())
        |> Map.put(:post_connect, normalize_post_connect(Map.get(opts, :post_connect)))
        |> Map.put(:payload_metadata, Map.get(opts, :payload_metadata, %{}))
        |> Map.put(:ws_opts, Map.get(opts, :ws_opts, []))
        |> Map.put(:idle_timeout_ms, idle_timeout_ms)
        |> Map.put(:resubscribe_after_ms, resubscribe_after_ms)
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

    case start_link(uri,
           ws_opts: Map.get(opts, :ws_opts, []),
           post_connect: Map.get(opts, :post_connect, %{}),
           overwatcher_pid: overwatcher_pid,
           payload_metadata: Map.get(opts, :payload_metadata, %{}),
           idle_timeout_ms: Map.get(opts, :idle_timeout_ms, @default_idle_timeout_ms),
           resubscribe_after_ms:
             Map.get(opts, :resubscribe_after_ms, @default_resubscribe_after_ms),
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
    idle = Map.get(opts, :idle_timeout_ms, @default_idle_timeout_ms)
    resub = Map.get(opts, :resubscribe_after_ms, @default_resubscribe_after_ms)

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

  @doc """
  The watchdog tick interval for `state`, or `nil` when both tiers are off.

  Half the nearest enabled deadline so neither tier is observed late, floored
  so a small deadline cannot produce a busy tick.
  """
  @spec idle_tick_interval(map()) :: pos_integer() | nil
  def idle_tick_interval(state) do
    case Enum.filter([state.idle_timeout_ms, state.resubscribe_after_ms], &(&1 > 0)) do
      [] -> nil
      enabled -> max(@min_tick_interval_ms, div(Enum.min(enabled), 2))
    end
  end

  # One meaning for every "milliseconds, non-negative, 0 disables" option:
  # clamp a negative to 0, and fall back to the default for anything that is
  # not a number at all. Reached from stored opts rather than a CLI string,
  # which is why it is silent where `convert_value/2` warns.
  defp ms_opt(value, _default) when is_integer(value), do: max(value, 0)

  defp ms_opt(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {n, ""} -> max(n, 0)
      _ -> default
    end
  end

  defp ms_opt(_value, default), do: default

  defp normalize_ignore_pattern(value) when is_binary(value) and value != "", do: value
  defp normalize_ignore_pattern(_), do: nil

  # :on_interval tick triggers nothing; connection is driven by the Fresh pid.
  def on_interval(%Ctx.Tick{}, _opts), do: :ok

  def on_terminate(%Ctx.Terminate{}, %{conn: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    :ok
  end

  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  def start_link(uri, opts) do
    idle_timeout_ms = Keyword.get(opts, :idle_timeout_ms, @default_idle_timeout_ms)
    resubscribe_after_ms = Keyword.get(opts, :resubscribe_after_ms, @default_resubscribe_after_ms)
    ignore_pattern = Keyword.get(opts, :ignore_pattern)
    plugin_id = Keyword.get(opts, :plugin_id) || uri

    now = :erlang.monotonic_time(:millisecond)

    state = %{
      uri: uri,
      plugin_id: plugin_id,
      post_connect: opts[:post_connect],
      overwatcher_pid: opts[:overwatcher_pid],
      payload_metadata: Keyword.get(opts, :payload_metadata, %{}),
      idle_timeout_ms: idle_timeout_ms,
      resubscribe_after_ms: resubscribe_after_ms,
      ignore_pattern: compile_ignore_pattern(ignore_pattern),
      # Effectively-once tier (from the activation env): ingest via a
      # blocking call so a zero-credit park stops the socket read.
      sync_ingest?: Keyword.get(opts, :sync_ingest?, false),
      websocket_established?: false,
      frames_seen: 0,
      last_frame_at: now,
      last_data_frame_at: now,
      last_resubscribe_at: now
    }

    Fresh.start_link(
      uri,
      __MODULE__,
      with_subscription_state(state, opts),
      opts[:ws_opts]
    )
  end

  # The reconcile-loop fields exist ONLY on a plugin that configured a source.
  # That is what keeps the promise that an unconfigured plugin pays nothing:
  # `handle_in/2`'s ack clause matches on `:inflight`, so its head simply does
  # not match here, and no timer is ever scheduled.
  defp with_subscription_state(state, opts) do
    case Keyword.get(opts, :subscription) do
      nil ->
        state

      subscription ->
        Map.merge(state, %{
          subscription: subscription,
          applied: MapSet.new(Keyword.get(opts, :applied_subscriptions, [])),
          # A literal item list is the desired set from the start; a polled
          # one has none until a poll lands.
          desired: Subscription.static_desired(subscription),
          inflight: %{},
          next_req_id: 1,
          cycle: 0,
          poll: nil,
          poll_timer: nil,
          source_etag: nil,
          last_reconcile: nil,
          last_error: nil,
          source_plugin_id: Keyword.get(opts, :source_plugin_id),
          source_namespace: Keyword.get(opts, :source_namespace),
          source_operative_id: Keyword.get(opts, :source_operative_id)
        })
    end
  end

  # Compile the substring matcher once at start so the per-frame path
  # runs `:binary.match/2` over a precompiled pattern rather than
  # recompiling on every frame. `nil` means "no pattern" — every data
  # frame counts as data with no scan at all.
  defp compile_ignore_pattern(pattern) when is_binary(pattern) and pattern != "" do
    :binary.compile_pattern(pattern)
  end

  defp compile_ignore_pattern(_), do: nil

  @impl Fresh
  def handle_terminate(reason, _state) do
    Logger.info("Websocket terminating: #{inspect(reason)}")
    :ok
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    Logger.info("Connected to websocket!")

    now = :erlang.monotonic_time(:millisecond)

    state = %{
      state
      | websocket_established?: true,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now
    }

    schedule_idle_check(state)
    state = schedule_subscription_poll(state)

    # Issue the subscribe via Fresh's reply form so the frame is sent
    # after the upgrade lands. A re-entrant `Fresh.send/2` here races the
    # upgrade and crashes with `{:badkey, :extensions, nil}`.
    #
    # Re-assert what is already applied, then converge on whatever the desired
    # set says beyond it — which is what makes a literal `subscription_items`
    # source subscribe with no poll to wait for. A polled source has no desired
    # set yet on a first connect, so the reconcile is a no-op there.
    {state, deltas} = reconcile(state)

    case subscription_frames(state) ++ deltas do
      [] -> {:ok, state}
      frames -> {:reply, frames, state}
    end
  end

  @impl Fresh
  def handle_disconnect(_code, _reason, state) do
    {:reconnect, %{state | websocket_established?: false}}
  end

  # Any control frame (ping/pong) proves the socket is alive, so it
  # refreshes the connection-liveness timer without touching data
  # liveness. Fresh auto-replies to pings before this callback runs.
  @impl Fresh
  def handle_control({:pong, _}, state) do
    {:ok, %{state | last_frame_at: :erlang.monotonic_time(:millisecond)}}
  end

  @impl Fresh
  def handle_control(control_frame, state) do
    Logger.warning("Received unexpected control frame: #{inspect(control_frame)}")
    {:ok, %{state | last_frame_at: :erlang.monotonic_time(:millisecond)}}
  end

  # A frame that acknowledges an outstanding subscription request commits its
  # delta. The clause head matches on `:inflight`, so a plugin with no
  # subscription source never reaches it — the key is not in its state — and a
  # configured plugin with nothing outstanding falls through on the empty-map
  # guard. The frame is then handled exactly as any other, so an acknowledgement
  # affects `last_data_frame_at` only through the existing `ignore_pattern`
  # classification.
  @impl Fresh
  def handle_in({:text, message} = frame, %{inflight: inflight} = state)
      when inflight != %{} do
    ingest(frame, commit_acknowledged(state, message))
  end

  @impl Fresh
  def handle_in(frame, state), do: ingest(frame, state)

  defp ingest(
         {type, message},
         %{overwatcher_pid: overwatcher, payload_metadata: metadata} = state
       ) do
    if Map.get(state, :sync_ingest?, false) do
      # Effectively-once tier ingest: await the journal ack so a
      # zero-credit park blocks this connection process — the socket stops
      # being read (TCP back-pressure) instead of the input-log growing
      # without bound.
      _ = GenServer.call(overwatcher, {:io_in, message, metadata}, :infinity)
    else
      GenServer.cast(overwatcher, {:io_in, message, metadata})
    end

    # One clock read and one classification per frame; `last_frame_at`
    # is set unconditionally (connection liveness) and `last_data_frame_at`
    # only when the frame is real data (subscription liveness).
    now = :erlang.monotonic_time(:millisecond)
    counter = state.frames_seen + 1
    is_data? = data_frame?(type, message, state.ignore_pattern)

    if rem(counter, 1000) == 0 do
      sample = String.slice(message, 0, 120)

      Logger.debug(
        "[Plugin.Provider.Websocket] frames_seen=#{counter} plugin_id=#{inspect(state.plugin_id)} last_msg_size=#{byte_size(message)} sample=#{inspect(sample)}"
      )
    end

    state = %{state | frames_seen: counter, last_frame_at: now}
    state = if is_data?, do: %{state | last_data_frame_at: now}, else: state

    {:ok, state}
  end

  # Data-vs-heartbeat classification (drives `last_data_frame_at`):
  #   * No `ignore_pattern` configured → every frame counts as data
  #     (no scan at all).
  #   * Binary frames → always data (the pattern is a text substring;
  #     scanning opaque payloads would be a false positive).
  #   * Text frames containing the pattern → heartbeat-shaped, not data.
  defp data_frame?(_type, _message, nil), do: true

  defp data_frame?(:text, message, pattern) when is_binary(message) do
    :binary.match(message, pattern) == :nomatch
  end

  defp data_frame?(_type, _message, _pattern), do: true

  # Two-tier watchdog tick:
  #   1. connection dead (`now - last_frame_at >= idle_timeout_ms`) → reconnect
  #   2. live but data stale (`resubscribe_after_ms` window elapsed) → re-subscribe
  #   3. otherwise → reschedule
  @impl Fresh
  def handle_info(:idle_check, state) do
    now = :erlang.monotonic_time(:millisecond)

    cond do
      # While Fresh is reconnecting there is no socket to police and
      # last_frame_at stays stale until handle_connect resets it; gate the
      # idle tier on an established socket so it can't tear down a healthy
      # reconnect with :idle_timeout.
      state.websocket_established? and state.idle_timeout_ms > 0 and
          now - state.last_frame_at >= state.idle_timeout_ms ->
        reconnect_dead_connection(state, now - state.last_frame_at)

      state.resubscribe_after_ms > 0 and
        now - state.last_data_frame_at >= state.resubscribe_after_ms and
          now - state.last_resubscribe_at >= state.resubscribe_after_ms ->
        resubscribe_stale_data(state, now, now - state.last_data_frame_at)

      true ->
        schedule_idle_check(state)
        {:ok, state}
    end
  end

  # Liveness probe for the health contract (Phase 7.1): report the two-tier
  # watchdog's current silence ages without disturbing them. Off the data path.
  @impl Fresh
  def handle_info({:liveness_probe, from, ref}, state) do
    now = :erlang.monotonic_time(:millisecond)
    send(from, {ref, {now - state.last_frame_at, now - state.last_data_frame_at}})
    {:ok, state}
  end

  def handle_info({:subscription_probe, from, ref}, %{subscription: _} = state) do
    send(from, {ref, subscription_report(state)})
    {:ok, state}
  end

  # A tick that lands on an outstanding request is dropped rather than queued:
  # the running fetch schedules the next one when it settles, so a slow
  # endpoint stretches the cadence instead of stacking concurrent requests.
  def handle_info(:subscription_poll, %{subscription: subscription, poll: nil} = state) do
    connection = self()

    request = Subscription.fetch_request(subscription, state.source_etag)

    # The fetch runs in a child so the connection process keeps reading the
    # socket while a slow or hanging source is outstanding.
    {pid, ref} =
      spawn_monitor(fn ->
        send(connection, {:subscription_fetched, self(), HttpFetch.fetch(request)})
      end)

    {:ok, %{state | poll: {pid, ref}}}
  end

  def handle_info(:subscription_poll, %{subscription: _} = state), do: {:ok, state}

  def handle_info({:subscription_fetched, pid, result}, %{poll: {pid, ref}} = state) do
    Process.demonitor(ref, [:flush])
    settle_poll(%{state | poll: nil}, result)
  end

  def handle_info({:DOWN, ref, :process, _dead, reason}, %{poll: {_pid, ref}} = state) do
    settle_poll(%{state | poll: nil}, {:error, {:fetch_crashed, reason}})
  end

  def handle_info(any, state) do
    Logger.warning("Received unexpected info at websocket process: #{inspect(any)}")
    {:ok, state}
  end

  defp reconnect_dead_connection(state, silence_ms) do
    Logger.warning(
      "[Plugin.Provider.Websocket] idle watchdog firing for plugin_id=" <>
        "#{inspect(state.plugin_id)} silence_ms=#{silence_ms} reason=idle_timeout"
    )

    :telemetry.execute(
      [:flezha, :ws, :idle_reconnect],
      %{silence_ms: silence_ms},
      %{plugin_id: state.plugin_id}
    )

    # The plugin manager monitors this pid; an abnormal-reason exit is
    # handled by `Flezha.Plugin.Extension.Table.handle_down/5`, which
    # calls `attempt_restart` and re-runs `init_provider/1` to spin up
    # a fresh Fresh.Connection with the subscription re-sent.
    Process.exit(self(), {:shutdown, :idle_timeout})
    {:ok, state}
  end

  defp resubscribe_stale_data(state, now, data_silence_ms) do
    # Rate-limit guard advances even when the send is skipped, so a
    # transiently-unestablished socket re-tries at most once per window.
    state = %{state | last_resubscribe_at: now}
    schedule_idle_check(state)

    with true <- established(state, "re-subscribe"),
         [_ | _] = frames <- subscription_frames(state) do
      :telemetry.execute(
        [:flezha, :ws, :resubscribe],
        %{data_silence_ms: data_silence_ms},
        %{plugin_id: state.plugin_id}
      )

      {:reply, frames, state}
    else
      _ -> {:ok, state}
    end
  end

  ## ---------- the subscription this socket asserts ----------

  # The ONE answer to "what is this socket subscribed to". Every site that
  # (re)asserts the full subscription — the connect handshake and the
  # data-liveness tier — reads it here, which is what stops the watchdog from
  # resending a boot-time payload a runtime change has since superseded.
  defp subscription_frames(%{subscription: subscription, applied: applied}) do
    items = Enum.sort(applied)

    if items == [] do
      []
    else
      [{:text, Subscription.render(subscription.subscribe_template, items, @reassert_req_id)}]
    end
  end

  defp subscription_frames(state) do
    pcp = state.post_connect["payload"]

    if is_map(pcp) and Map.has_key?(pcp, "type") and Map.has_key?(pcp, "message") do
      type = if pcp["type"] == "binary", do: :binary, else: :text
      [{type, pcp["message"]}]
    else
      []
    end
  end

  ## ---------- the reconcile cycle ----------

  # A failed poll is "no information", never an empty desired set: the applied
  # set stands and nothing is unsubscribed. Only a decoded response moves it.
  #
  # Delta frames leave through Fresh's reply form, the same mechanism the
  # data-liveness tier uses, so nothing is ever sent re-entrantly.
  defp settle_poll(state, result) do
    {state, frames} =
      case result do
        {:ok, :not_modified} -> reconcile(%{state | last_error: nil})
        {:ok, body, etag} -> decoded_source(%{state | source_etag: etag}, body)
        {:error, reason} -> {poll_failed(state, reason), []}
      end

    state = schedule_subscription_poll(state)

    case frames do
      [] -> {:ok, state}
      frames -> {:reply, frames, state}
    end
  end

  defp decoded_source(state, body) do
    case Subscription.desired_set(state.subscription, body) do
      {:ok, desired} -> reconcile(%{state | desired: desired, last_error: nil})
      {:error, reason} -> {poll_failed(state, reason), []}
    end
  end

  defp poll_failed(state, reason) do
    Logger.warning(
      "[Plugin.Provider.Websocket] subscription poll failed plugin_id=" <>
        "#{inspect(state.plugin_id)} reason=#{inspect(reason)}"
    )

    %{state | last_error: reason}
  end

  # No subscription source at all — a plain static plugin, whose state carries
  # no reconcile keys.
  defp reconcile(state) when not is_map_key(state, :desired), do: {state, []}

  # Nothing has been fetched yet, so there is no desired set to converge on.
  defp reconcile(%{desired: nil} = state), do: {state, []}

  defp reconcile(state) do
    added = MapSet.difference(state.desired, state.applied)
    removed = MapSet.difference(state.applied, state.desired)

    if MapSet.size(added) == 0 and MapSet.size(removed) == 0 do
      {%{state | last_reconcile: %{added: 0, removed: 0, sent: false}}, []}
    else
      send_deltas(%{state | cycle: state.cycle + 1}, added, removed)
    end
  end

  defp send_deltas(state, added, removed) do
    if established(state, "reconcile") do
      {state, frames} =
        {state, []}
        |> delta_frame(:add, added, state.subscription.subscribe_template)
        |> delta_frame(:remove, removed, state.subscription.unsubscribe_template)

      :telemetry.execute(
        [:flezha, :ws, :reconcile],
        %{added: MapSet.size(added), removed: MapSet.size(removed)},
        %{plugin_id: state.plugin_id}
      )

      state =
        state
        |> expire_stale_inflight()
        |> commit_if_ack_free(frames)
        |> Map.put(:last_reconcile, %{
          added: MapSet.size(added),
          removed: MapSet.size(removed),
          sent: true
        })

      {state, frames}
    else
      # The delta stays uncommitted and the next cycle re-derives it.
      {%{state | last_reconcile: %{added: 0, removed: 0, sent: false}}, []}
    end
  end

  defp delta_frame({state, frames}, kind, items, template) do
    if MapSet.size(items) == 0 do
      {state, frames}
    else
      req_id = state.next_req_id
      sorted = Enum.sort(items)

      state = %{
        state
        | next_req_id: req_id + 1,
          inflight: Map.put(state.inflight, req_id, {kind, sorted, state.cycle})
      }

      {state, frames ++ [{:text, Subscription.render(template, sorted, req_id)}]}
    end
  end

  # An unanswered request from a previous cycle is abandoned: its delta was
  # re-derived and re-sent under a fresh req_id, so keeping the old one would
  # let a very late acknowledgement commit a set the source has moved on from.
  defp expire_stale_inflight(state) do
    %{
      state
      | inflight:
          state.inflight
          |> Enum.filter(fn {_req_id, {_kind, _items, cycle}} -> cycle >= state.cycle end)
          |> Map.new()
    }
  end

  defp commit_if_ack_free(%{subscription: %Subscription{ack_mode: "none"}} = state, frames)
       when frames != [] do
    Enum.reduce(Map.keys(state.inflight), state, &commit(&2, &1))
  end

  defp commit_if_ack_free(state, _frames), do: state

  ## ---------- acknowledgement-gated commit ----------

  # The correlation contract: the upstream echoes the `{{req_id}}` the template
  # carried. An object naming an outstanding one is that request's answer, and
  # reports success unless it says otherwise.
  defp commit_acknowledged(state, message) do
    with {:ok, %{"req_id" => req_id} = decoded} <- Jason.decode(message),
         true <- is_integer(req_id),
         {:ok, _entry} <- Map.fetch(state.inflight, req_id) do
      if Subscription.acknowledged?(decoded) do
        commit(state, req_id)
      else
        # Not committed: the next cycle re-derives an identical delta.
        Logger.warning(
          "[Plugin.Provider.Websocket] subscription request rejected plugin_id=" <>
            "#{inspect(state.plugin_id)} req_id=#{req_id}"
        )

        %{state | inflight: Map.delete(state.inflight, req_id), last_error: {:rejected, req_id}}
      end
    else
      _ -> state
    end
  end

  defp commit(state, req_id) do
    case Map.fetch(state.inflight, req_id) do
      {:ok, {kind, items, _cycle}} ->
        applied =
          case kind do
            :add -> MapSet.union(state.applied, MapSet.new(items))
            :remove -> MapSet.difference(state.applied, MapSet.new(items))
          end

        state = %{
          state
          | applied: applied,
            inflight: Map.delete(state.inflight, req_id)
        }

        persist_applied(state)
        state

      :error ->
        state
    end
  end

  # The applied set is written to the plugin cspec — the same record the enable
  # lives in — so a plugin restart, an operative handover and a controller
  # restart all resume with what the socket actually holds. Written after the
  # commit, so a crash between send and acknowledgement degrades to a re-send
  # rather than to a subscription nobody remembers.
  defp persist_applied(%{source_plugin_id: plugin_id, overwatcher_pid: host} = state)
       when is_binary(plugin_id) and is_pid(host) do
    GenServer.cast(
      host,
      {:plugin_opt_persist, plugin_id, :applied_subscriptions, Enum.sort(state.applied)}
    )
  end

  defp persist_applied(_state), do: :ok

  ## ---------- sending ----------

  defp established(%{websocket_established?: true}, _what), do: true

  defp established(state, what) do
    Logger.warning(
      "[Plugin.Provider.Websocket] skipping #{what}; websocket not established " <>
        "plugin_id=#{inspect(state.plugin_id)}"
    )

    false
  end

  ## ---------- observability ----------

  defp subscription_report(state) do
    %{
      applied: Enum.sort(state.applied),
      desired: state.desired && Enum.sort(state.desired),
      inflight: map_size(state.inflight),
      last_reconcile: state.last_reconcile,
      last_error: state.last_error
    }
  end

  ## ---------- timers ----------

  defp schedule_idle_check(state) do
    case idle_tick_interval(state) do
      nil ->
        :ok

      tick ->
        Process.send_after(self(), :idle_check, tick)
        :ok
    end
  end

  # Its own timer, independent of the watchdog tick: the two cadences answer
  # different questions and a source poll must not be paced by a liveness
  # deadline. No source configured means no timer at all.
  #
  # Idempotent by cancelling first, because `handle_connect/3` runs on EVERY
  # reconnect while a poll chain from the previous connection is still armed.
  # Without the cancel each reconnect would leave another chain running and the
  # source would be polled twice, then four times, then eight.
  defp schedule_subscription_poll(%{subscription: subscription} = state) do
    case Map.get(state, :poll_timer) do
      nil -> :ok
      ref -> Process.cancel_timer(ref, async: true, info: false)
    end

    # A literal item list has no endpoint to poll; its desired set is already
    # final and the reconcile at connect is the whole loop.
    if Subscription.polled?(subscription) do
      %{
        state
        | poll_timer: Process.send_after(self(), :subscription_poll, subscription.interval_ms)
      }
    else
      %{state | poll_timer: nil}
    end
  end

  defp schedule_subscription_poll(state), do: state
end
