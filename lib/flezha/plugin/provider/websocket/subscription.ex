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
end
