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

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.Host
  alias Flezha.Config.Durations
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Process.Timer
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  require Logger

  # A re-assertion (connect, data-liveness re-subscribe) re-sends what is
  # ALREADY applied, so it has nothing to commit. It carries a reserved id
  # outside the in-flight space — which starts at 1 — so its acknowledgement,
  # however late, can never be mistaken for a pending delta's.
  @reassert_req_id 0

  # The source is a small document on a user-controlled endpoint; both
  # bounds are deliberately fixed rather than tunable, so a subscription source
  # cannot be configured into a slow-loris or an unbounded allocation.
  @max_body_bytes 1_048_576

  @items_placeholder "{{items}}"
  @req_id_placeholder "{{req_id}}"
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
                interval_ms: nil,
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
  The placeholder a template carries its correlation id in.

  Published because the correlation contract is one contract: the outbound sink
  renders through the same placeholder and reads its answer through
  `acknowledgement/1`, so an upstream acknowledges a subscription delta and a
  shipped output exactly the same way.
  """
  @spec req_id_placeholder() :: binary()
  def req_id_placeholder(), do: @req_id_placeholder

  @doc "The polling cadence a subscription source takes when none is written."
  @spec default_interval_ms() :: pos_integer()
  def default_interval_ms(), do: Durations.get(:ws_subscription_interval_ms)

  @doc """
  Build a source from a plugin's opts, or `{:ok, nil}` when none is configured.

  Every refusal here fails the enable and opens no connection.
  """
  @spec from_opts(map()) :: {:ok, t() | nil} | {:error, term()}
  def from_opts(opts) do
    url = blank_to_nil(Map.get(opts, :subscription_url))
    items = Map.get(opts, :subscription_items)

    cond do
      url != nil and items != nil ->
        {:error, {:options_exclusive, :subscription_url, :subscription_items}}

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
         :ok <- interval(opts.subscription_interval_ms),
         {:ok, subscribe} <- template(opts, :subscribe_template),
         {:ok, unsubscribe} <- template(opts, :unsubscribe_template) do
      {:ok,
       %__MODULE__{
         url: url,
         items: items,
         range: range(opts),
         headers: headers,
         interval_ms: opts.subscription_interval_ms,
         subscribe_template: subscribe,
         unsubscribe_template: unsubscribe,
         ack_mode: opts.subscription_ack_mode
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
      ProviderBehaviour.refuse_value(:subscription_items, items)
    end
  end

  defp validate_items(items), do: ProviderBehaviour.refuse_value(:subscription_items, items)

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

  # The one bound that is not a declared row: its floor is a user-settable
  # duration, read here rather than frozen into the declaration at compile time.
  defp interval(value) do
    minimum = Durations.get(:ws_subscription_min_interval_ms)

    if value < minimum,
      do: ProviderOptions.refuse_below(:subscription_interval_ms, value, minimum),
      else: :ok
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
          {:error, {:template_missing_placeholder, field, @items_placeholder}}
        end

      _ ->
        {:error, {:option_requires, :subscription_url, field}}
    end
  end

  @doc "The `HttpFetch` request for one poll, conditional on the last ETag."
  @spec fetch_request(t(), binary() | nil) :: HttpFetch.request()
  def fetch_request(%__MODULE__{} = subscription, etag) do
    %{
      url: subscription.url,
      headers: subscription.headers,
      etag: etag,
      timeout_ms: Durations.get(:ws_subscription_timeout_ms),
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

  @doc """
  Keep only the items this partition owns. The one filter point: everything
  downstream — the applied set, the delta frames, the durable
  `applied_subscriptions`, the connect-time re-assert — derives from here,
  including the persisted set a starting socket resumes from, which after a
  rescale was committed under another member's range.
  """
  @spec owned(t(), [binary()]) :: MapSet.t(binary())
  def owned(%__MODULE__{range: nil}, items), do: MapSet.new(items)

  def owned(%__MODULE__{range: range}, items),
    do: items |> Enum.filter(&Flezha.Partition.owns?(range, &1)) |> MapSet.new()

  @doc """
  What this inbound frame acknowledges: the `req_id` it echoes and whether it
  accepted the request, or `:error` when it acknowledges nothing.

  The correlation contract, read once. Both directions send an id in a template
  and both read the answer here; the id SPACES are disjoint (a delta's is an
  integer, a shipped output's a string), so neither side can commit the other's
  work through one reply.
  """
  @spec acknowledgement(binary()) :: {:ok, term(), boolean()} | :error
  def acknowledgement(message) when is_binary(message) do
    case Jason.decode(message) do
      {:ok, %{"req_id" => req_id} = decoded} -> {:ok, req_id, acknowledged?(decoded)}
      _other -> :error
    end
  end

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
  # Answers the state and the frames it owes, never a connection-library return
  # shape: the Client turns frames into a reply, which is what keeps this module
  # ignorant of the callbacks it runs under. Nothing is ever sent re-entrantly.
  def settle_poll(state, result) do
    {state, frames} =
      case result do
        {:ok, :not_modified} -> reconcile(%{state | last_error: nil})
        {:ok, body, etag} -> decoded_source(%{state | source_etag: etag}, body)
        {:error, reason} -> {poll_failed(state, reason), []}
      end

    {schedule_subscription_poll(state), frames}
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

      emit(
        :reconcile,
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
  def commit_acknowledged(%{inflight: inflight} = state, message) when inflight != %{} do
    with {:ok, req_id, accepted?} <- acknowledgement(message),
         true <- is_integer(req_id),
         {:ok, _entry} <- Map.fetch(state.inflight, req_id) do
      if accepted? do
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

  # A plugin with no subscription source carries no in-flight map, and a
  # configured one with nothing outstanding has nothing to commit. The guard
  # lives here rather than in the caller's clause head so both directions settle
  # an inbound frame the same way.
  def commit_acknowledged(state, _message), do: state

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
  # lives in — so a plugin restart, a relay handover and a controller
  # restart all resume with what the socket actually holds. Written after the
  # commit, so a crash between send and acknowledgement degrades to a re-send
  # rather than to a subscription nobody remembers.
  def persist_applied(state) do
    Host.persist_opt(
      Map.get(state, :overwatcher_pid),
      Map.get(state, :source_plugin_id),
      :applied_subscriptions,
      Enum.sort(state.applied)
    )
  end

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

  def subscription_report(%{subscription: _} = state) do
    %{
      applied: Enum.sort(state.applied),
      desired: state.desired && Enum.sort(state.desired),
      inflight: map_size(state.inflight),
      last_reconcile: state.last_reconcile,
      last_error: state.last_error
    }
  end

  def subscription_report(_static), do: %{}

  # Armed on the client's behalf, under the `:poll` key that module declares.
  # A literal item list has no endpoint to poll; its desired set is already
  # final and the reconcile at connect is the whole loop.
  def schedule_subscription_poll(%{subscription: subscription} = state) do
    if polled?(subscription) do
      %{
        state
        | timers: Timer.arm(state.timers, :poll, subscription.interval_ms, :subscription_poll)
      }
    else
      %{state | timers: Timer.cancel(state.timers, :poll)}
    end
  end

  def schedule_subscription_poll(state), do: state
  # The prefix is derived from the provider's id; nothing here names a root.
  @provider "websocket"

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)
end
