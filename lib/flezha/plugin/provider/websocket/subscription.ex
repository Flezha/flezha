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

  @enforce_keys [:url, :subscribe_template, :unsubscribe_template]
  defstruct @enforce_keys ++
              [
                headers: %{},
                interval_ms: @default_interval_ms,
                ack_mode: "required"
              ]

  @type t() :: %__MODULE__{
          url: binary(),
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
    case Map.get(opts, :subscription_url) do
      nil -> {:ok, nil}
      "" -> {:ok, nil}
      url -> build(opts, url)
    end
  end

  defp build(opts, url) do
    with {:ok, url} <-
           HttpFetch.validate_url(url, "websocket", :subscription_url, :subscription_headers),
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
         headers: headers,
         interval_ms: interval_ms,
         subscribe_template: subscribe,
         unsubscribe_template: unsubscribe,
         ack_mode: ack_mode
       }}
    end
  end

  defp interval(nil), do: {:ok, @default_interval_ms}

  defp interval(value) do
    case Flezha.Plugin.ProviderBehaviour.parse_int(value, :subscription_interval_ms) do
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
  @spec desired_set(binary()) :: {:ok, MapSet.t()} | {:error, atom()}
  def desired_set(body) do
    case Jason.decode(body) do
      {:ok, %{"items" => items}} when is_list(items) ->
        if Enum.all?(items, &is_binary/1) do
          {:ok, MapSet.new(items)}
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
