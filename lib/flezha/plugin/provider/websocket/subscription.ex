defmodule Flezha.Plugin.Provider.Websocket.Subscription do
  @moduledoc """
  A websocket plugin's polled subscription source: where the desired item set is
  published, the templates a delta renders through, and the rule for reading an
  upstream acknowledgement.

  This is the *policy* half of the reconcile loop — what a delta means and
  whether a reply accepted it — with no knowledge of the socket, the Fresh
  callbacks, or the connection state they run against. `Websocket` owns the loop
  and its timers; this owns what the loop is saying.

  Absent (`from_opts/1` returning `{:ok, nil}`) is the ordinary static plugin,
  which subscribes with its `post_connect` payload and never polls anything.
  """

  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  # A re-assertion (connect, data-liveness re-subscribe) re-sends what is
  # ALREADY applied, so it has nothing to commit. It carries a reserved id
  # outside the in-flight space — which starts at 1 — so its acknowledgement,
  # however late, can never be mistaken for a pending delta's.
  @reassert_req_id 0

  @default_interval_ms 30_000
  @min_interval_ms 1_000

  # The source is a small document on an operator-controlled endpoint; both
  # bounds are deliberately fixed rather than tunable, so a subscription source
  # cannot be configured into a slow-loris or an unbounded allocation.
  @timeout_ms 10_000
  @max_body_bytes 1_048_576

  @items_placeholder "{{items}}"
  @req_id_placeholder "{{req_id}}"
  @ack_modes ["required", "none"]

  @enforce_keys [:subscribe_template, :unsubscribe_template]
  defstruct @enforce_keys ++
              [
                # Exactly one of these two is set: a polled endpoint, or a
                # literal list. Both feed the same desired/reconcile machinery.
                url: nil,
                items: nil,
                # The partition's owned key-group range, when this plugin
                # belongs to a group member. `nil` = the whole space.
                range: nil,
                headers: %{},
                interval_ms: @default_interval_ms,
                ack_mode: "required"
              ]

  @type t() :: %__MODULE__{
          url: binary() | nil,
          items: [binary()] | nil,
          range: {non_neg_integer(), non_neg_integer()} | nil,
          subscribe_template: binary(),
          unsubscribe_template: binary(),
          headers: %{binary() => binary()},
          interval_ms: pos_integer(),
          ack_mode: binary()
        }

  @doc "The placeholder every template must carry, for error messages."
  @spec items_placeholder() :: binary()
  def items_placeholder(), do: @items_placeholder

  @doc """
  Build a source from a plugin's opts, or `{:ok, nil}` when none is configured.

  Every refusal here fails the enable and opens no connection.
  """
  @spec from_opts(map()) :: {:ok, t() | nil} | {:error, binary()}
  def from_opts(opts) do
    url = blank_to_nil(Map.get(opts, :subscription_url))
    items = Map.get(opts, :subscription_items)

    cond do
      url != nil and items != nil ->
        {:error,
         "websocket subscription_url and subscription_items are mutually exclusive — " <>
           "a source is either polled or literal"}

      url == nil and items == nil ->
        {:ok, nil}

      true ->
        build(opts, url, items)
    end
  end

  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  defp build(opts, url, items) do
    with {:ok, url} <- validate_url(url),
         {:ok, items} <- validate_items(items),
         {:ok, headers} <-
           HttpFetch.validate_headers(
             Map.get(opts, :subscription_headers),
             "websocket",
             :subscription_headers
           ),
         {:ok, interval_ms} <- interval(Map.get(opts, :subscription_interval_ms)),
         {:ok, subscribe} <- template(opts, :subscribe_template),
         {:ok, unsubscribe} <- template(opts, :unsubscribe_template),
         {:ok, ack_mode} <- ack_mode(Map.get(opts, :subscription_ack_mode)) do
      {:ok,
       %__MODULE__{
         url: url,
         items: items,
         range: range(opts),
         headers: headers,
         interval_ms: interval_ms,
         subscribe_template: subscribe,
         unsubscribe_template: unsubscribe,
         ack_mode: ack_mode
       }}
    end
  end

  defp validate_url(nil), do: {:ok, nil}

  defp validate_url(url),
    do: HttpFetch.validate_url(url, "websocket", :subscription_url, :subscription_headers)

  defp validate_items(nil), do: {:ok, nil}

  defp validate_items(items) when is_list(items) do
    if Enum.all?(items, &(is_binary(&1) and &1 != "")) do
      {:ok, Enum.uniq(items)}
    else
      {:error, "websocket subscription_items must be a list of non-empty strings"}
    end
  end

  defp validate_items(_items),
    do: {:error, "websocket subscription_items must be a list of non-empty strings"}

  # The member's owned key-group range, injected into the plugin's opts by the
  # group fan-out. A partitioned member asks upstream only for its own share,
  # so nothing is received and discarded.
  defp range(opts) do
    with count when is_integer(count) and count >= 2 <- Map.get(opts, :partition_count),
         from when is_integer(from) <- Map.get(opts, :key_group_from),
         to when is_integer(to) <- Map.get(opts, :key_group_to) do
      {from, to}
    else
      _ -> nil
    end
  end

  defp interval(nil), do: {:ok, @default_interval_ms}

  defp interval(value) do
    case ProviderBehaviour.parse_int(value, :subscription_interval_ms) do
      {:ok, ms} when ms >= @min_interval_ms ->
        {:ok, ms}

      {:ok, ms} ->
        {:error,
         "websocket subscription_interval_ms #{ms} is below the #{@min_interval_ms} ms minimum"}

      {:error, reason} ->
        {:error, "websocket #{reason}"}
    end
  end

  # Both templates are required alongside a source: a set that can only grow is
  # not a reconcile loop, and discovering the gap at the first removal — on a
  # live socket, in production — is exactly the failure this refuses.
  defp template(opts, field) do
    case Map.get(opts, field) do
      value when is_binary(value) and value != "" ->
        if String.contains?(value, @items_placeholder) do
          {:ok, value}
        else
          {:error, "websocket #{field} must contain the #{@items_placeholder} placeholder"}
        end

      _ ->
        {:error, "websocket subscription_url requires #{field}"}
    end
  end

  defp ack_mode(nil), do: {:ok, "required"}
  defp ack_mode(mode) when mode in @ack_modes, do: {:ok, mode}

  defp ack_mode(mode),
    do:
      {:error,
       "websocket subscription_ack_mode #{inspect(mode)} must be one of " <>
         Enum.map_join(@ack_modes, ", ", &inspect/1)}

  @doc "The `HttpFetch` request for one poll, conditional on the last ETag."
  @spec fetch_request(t(), binary() | nil) :: HttpFetch.request()
  def fetch_request(%__MODULE__{} = subscription, etag) do
    %{
      url: subscription.url,
      headers: subscription.headers,
      etag: etag,
      timeout_ms: @timeout_ms,
      max_body_bytes: @max_body_bytes
    }
  end

  @doc """
  Render a template for `items`, substituting `{{items}}` with the JSON array
  and `{{req_id}}` with the correlation id.
  """
  @spec render(binary(), [binary()], integer()) :: binary()
  def render(template, items, req_id) do
    template
    |> String.replace(@items_placeholder, Jason.encode!(items))
    |> String.replace(@req_id_placeholder, Integer.to_string(req_id))
  end

  @doc """
  The desired set from a decoded source response, or the reason it is unusable.

  A failure here is *no information* to the caller — never an empty desired set,
  which would read as "unsubscribe everything".
  """
  @spec desired_set(t(), binary()) :: {:ok, MapSet.t()} | {:error, atom()}
  def desired_set(%__MODULE__{} = subscription, body) do
    case Jason.decode(body) do
      {:ok, %{"items" => items}} when is_list(items) ->
        if Enum.all?(items, &is_binary/1) do
          {:ok, owned(subscription, items)}
        else
          {:error, :items_must_be_strings}
        end

      {:ok, _} ->
        {:error, :missing_items}

      {:error, _} ->
        {:error, :invalid_json}
    end
  end

  @doc """
  The desired set of a literal `subscription_items` source, or `nil` for a
  polled one (whose set only exists once a poll has landed).
  """
  @spec static_desired(t() | nil) :: MapSet.t() | nil
  def static_desired(%__MODULE__{items: items} = subscription) when is_list(items),
    do: owned(subscription, items)

  def static_desired(_subscription), do: nil

  @doc "Whether this source is polled (as opposed to a literal item list)."
  @spec polled?(t() | nil) :: boolean()
  def polled?(%__MODULE__{url: url}), do: url != nil
  def polled?(_subscription), do: false

  # Keep only the items this partition owns. The one filter point: everything
  # downstream — the applied set, the delta frames, the durable
  # `applied_subscriptions`, the connect-time re-assert — derives from here.
  defp owned(%__MODULE__{range: nil}, items), do: MapSet.new(items)

  defp owned(%__MODULE__{range: range}, items),
    do: items |> Enum.filter(&Flezha.Partition.owns?(range, &1)) |> MapSet.new()

  @doc """
  Does this decoded inbound frame accept the request it names? An
  acknowledgement reports success unless it says otherwise.
  """
  @spec acknowledged?(map()) :: boolean()
  def acknowledged?(%{"success" => success}), do: success == true
  def acknowledged?(%{"error" => _}), do: false
  def acknowledged?(_decoded), do: true

  @doc """
  Normalise a persisted applied set: de-duplicated and ordered, so a set
  restored from the cspec and a freshly-committed one compare equal.
  """
  @spec applied_set(term()) :: [binary()]
  def applied_set(items) when is_list(items) do
    items |> Enum.filter(&is_binary/1) |> Enum.uniq() |> Enum.sort()
  end

  def applied_set(_items), do: []

  ## ---------- the subscription this socket asserts ----------

  # The ONE answer to "what is this socket subscribed to". Every site that
  # (re)asserts the full subscription — the connect handshake and the
  # data-liveness tier — reads it here, which is what stops the watchdog from
  # resending a boot-time payload a runtime change has since superseded.
  def subscription_frames(%{subscription: subscription, applied: applied}) do
    items = Enum.sort(applied)

    if items == [] do
      []
    else
      [{:text, render(subscription.subscribe_template, items, @reassert_req_id)}]
    end
  end

  def subscription_frames(state) do
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
  def settle_poll(state, result) do
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
    case desired_set(state.subscription, body) do
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
  def reconcile(state) when not is_map_key(state, :desired), do: {state, []}

  # Nothing has been fetched yet, so there is no desired set to converge on.
  def reconcile(%{desired: nil} = state), do: {state, []}

  def reconcile(state) do
    added = MapSet.difference(state.desired, state.applied)
    removed = MapSet.difference(state.applied, state.desired)

    if MapSet.size(added) == 0 and MapSet.size(removed) == 0 do
      {%{state | last_reconcile: %{added: 0, removed: 0, sent: false}}, []}
    else
      send_deltas(%{state | cycle: state.cycle + 1}, added, removed)
    end
  end

  def send_deltas(state, added, removed) do
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

      {state, frames ++ [{:text, render(template, sorted, req_id)}]}
    end
  end

  # An unanswered request from a previous cycle is abandoned: its delta was
  # re-derived and re-sent under a fresh req_id, so keeping the old one would
  # let a very late acknowledgement commit a set the source has moved on from.
  def expire_stale_inflight(state) do
    %{
      state
      | inflight:
          state.inflight
          |> Enum.filter(fn {_req_id, {_kind, _items, cycle}} -> cycle >= state.cycle end)
          |> Map.new()
    }
  end

  def commit_if_ack_free(%{subscription: %__MODULE__{ack_mode: "none"}} = state, frames)
      when frames != [] do
    Enum.reduce(Map.keys(state.inflight), state, &commit(&2, &1))
  end

  def commit_if_ack_free(state, _frames), do: state

  ## ---------- acknowledgement-gated commit ----------

  # The correlation contract: the upstream echoes the `{{req_id}}` the template
  # carried. An object naming an outstanding one is that request's answer, and
  # reports success unless it says otherwise.
  def commit_acknowledged(state, message) do
    with {:ok, %{"req_id" => req_id} = decoded} <- Jason.decode(message),
         true <- is_integer(req_id),
         {:ok, _entry} <- Map.fetch(state.inflight, req_id) do
      if acknowledged?(decoded) do
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
  def persist_applied(%{source_plugin_id: plugin_id, overwatcher_pid: host} = state)
      when is_binary(plugin_id) and is_pid(host) do
    GenServer.cast(
      host,
      {:plugin_opt_persist, plugin_id, :applied_subscriptions, Enum.sort(state.applied)}
    )
  end

  def persist_applied(_state), do: :ok

  ## ---------- sending ----------

  def established(%{websocket_established?: true}, _what), do: true

  def established(state, what) do
    Logger.warning(
      "[Plugin.Provider.Websocket] skipping #{what}; websocket not established " <>
        "plugin_id=#{inspect(state.plugin_id)}"
    )

    false
  end

  ## ---------- observability ----------

  def subscription_report(state) do
    %{
      applied: Enum.sort(state.applied),
      desired: state.desired && Enum.sort(state.desired),
      inflight: map_size(state.inflight),
      last_reconcile: state.last_reconcile,
      last_error: state.last_error
    }
  end

  def schedule_subscription_poll(%{subscription: subscription} = state) do
    case Map.get(state, :poll_timer) do
      nil -> :ok
      ref -> Process.cancel_timer(ref, async: true, info: false)
    end

    # A literal item list has no endpoint to poll; its desired set is already
    # final and the reconcile at connect is the whole loop.
    if polled?(subscription) do
      %{
        state
        | poll_timer: Process.send_after(self(), :subscription_poll, subscription.interval_ms)
      }
    else
      %{state | poll_timer: nil}
    end
  end

  def schedule_subscription_poll(state), do: state
end
