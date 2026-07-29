defmodule Flezha.Plugin.SpecDecoder do
  @moduledoc """
  The one decoder for the JSON plugin-spec array carried by
  `flz operative start --plugins` and `flz schedule add --plugins`.

  Turns the array into `%Flezha.Plugin{}` structs ready for
  `Flezha.Operative.Builder`'s `opts = [plugins: ...]`. Provider ids are
  resolved against the controller's currently enabled providers, so the
  decoded form is runtime state: a durable schedule stores the JSON and decodes
  it again at every firing.

  Error vocabulary (unchanged from the CLI's original decoder):

    * `{:invalid_plugins_json, reason}`
    * `{:unknown_provider, provider_id}`
    * `{:invalid_subscription, details}`
    * `{:invalid_opts, details}`
  """

  alias Flezha.Plugin
  alias Flezha.Service.ConfigManager

  @doc """
  Decode `plugins_json` into plugin structs bound to `namespace` and
  `operative_id`. An empty/blank spec decodes to `[]`.
  """
  @spec decode(binary() | nil, atom() | binary(), binary()) ::
          {:ok, [Plugin.t()]} | {:error, term()}
  def decode(plugins_json, namespace, operative_id) do
    case decode_json(plugins_json) do
      {:ok, []} ->
        {:ok, []}

      {:ok, specs} ->
        providers = ConfigManager.get_enabled_plugins()

        Enum.reduce_while(specs, {:ok, []}, fn spec, {:ok, acc} ->
          case build_plugin(spec, providers, namespace, operative_id) do
            {:ok, plugin} -> {:cont, {:ok, [plugin | acc]}}
            {:error, _} = err -> {:halt, err}
          end
        end)
        |> case do
          {:ok, plugins} -> {:ok, Enum.reverse(plugins)}
          err -> err
        end

      {:error, reason} ->
        {:error, {:invalid_plugins_json, reason}}
    end
  end

  @doc "Structural check only — no provider resolution. Used where the spec is stored, not run."
  @spec valid_json_array?(binary() | nil) :: boolean()
  def valid_json_array?(plugins_json), do: match?({:ok, _}, decode_json(plugins_json))

  @doc "String keys to atoms; the JSON wire format cannot carry atom keys."
  @spec atomize_keys(map()) :: map()
  def atomize_keys(map) when is_map(map) do
    Map.new(map, fn {k, v} ->
      {(is_binary(k) && String.to_atom(k)) || k, v}
    end)
  end

  ## ----- private -----

  defp decode_json(j) when j in [nil, "", "[]", "{}"], do: {:ok, []}

  defp decode_json(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, list} when is_list(list) -> {:ok, list}
      {:ok, _} -> {:error, :not_a_json_array}
      {:error, %Jason.DecodeError{} = err} -> {:error, Exception.message(err)}
    end
  end

  defp decode_json(_), do: {:ok, []}

  defp build_plugin(spec, providers, namespace, operative_id) when is_map(spec) do
    with {:ok, plugin_id} <- fetch_string(spec, "id"),
         {:ok, provider_id} <- fetch_string(spec, "provider"),
         {:ok, provider_mod} <- resolve_provider(providers, provider_id),
         {:ok, raw_opts} <- fetch_map(spec, "opts"),
         {:ok, subscriptions} <- build_subscriptions(Map.get(spec, "subscriptions", [])) do
      {:ok,
       %Plugin{
         id: plugin_id,
         provider: provider_mod,
         opts: atomize_keys(raw_opts),
         subscriptions: subscriptions,
         namespace: namespace,
         operative_id: operative_id
       }}
    end
  end

  defp build_plugin(_, _, _, _), do: {:error, {:invalid_opts, :plugin_spec_not_a_map}}

  defp fetch_string(spec, key) do
    case Map.get(spec, key) do
      s when is_binary(s) and byte_size(s) > 0 -> {:ok, s}
      _ -> {:error, {:invalid_opts, {:missing_field, key}}}
    end
  end

  defp fetch_map(spec, key) do
    case Map.get(spec, key, %{}) do
      m when is_map(m) -> {:ok, m}
      _ -> {:error, {:invalid_opts, {:not_a_map, key}}}
    end
  end

  defp resolve_provider(providers, provider_id) do
    case Map.get(providers, provider_id) do
      nil -> {:error, {:unknown_provider, provider_id}}
      mod -> {:ok, mod}
    end
  end

  defp build_subscriptions(subs) when is_list(subs) and subs != [] do
    Enum.reduce_while(subs, {:ok, []}, fn sub, {:ok, acc} ->
      case build_subscription(sub) do
        {:ok, tuple} -> {:cont, {:ok, [tuple | acc]}}
        err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      err -> err
    end
  end

  defp build_subscriptions([]), do: {:error, {:invalid_subscription, :empty}}
  defp build_subscriptions(_), do: {:error, {:invalid_subscription, :not_a_list}}

  defp build_subscription([name, opts]) when is_binary(name) and is_map(opts) do
    {:ok, {String.to_atom(name), atomize_sub_opts(opts)}}
  end

  defp build_subscription([name]) when is_binary(name) do
    {:ok, {String.to_atom(name), []}}
  end

  defp build_subscription(name) when is_binary(name) do
    {:ok, {String.to_atom(name), []}}
  end

  defp build_subscription(%{"name" => name} = sub) when is_binary(name) do
    opts = Map.get(sub, "opts", %{})

    if is_map(opts) do
      {:ok, {String.to_atom(name), atomize_sub_opts(opts)}}
    else
      {:error, {:invalid_subscription, {:opts_not_a_map, name}}}
    end
  end

  defp build_subscription(other),
    do: {:error, {:invalid_subscription, {:unrecognised_shape, other}}}

  # Subscription opts carry both atom-keyed keys and atom-valued flags
  # (e.g. `on_error: :empty`). The JSON wire format loses both — keys and
  # values arrive as strings. Atomise both so the downstream
  # `Table.validate_on_error/1` sees the shape it expects.
  defp atomize_sub_opts(map) when is_map(map) do
    Enum.map(map, fn {k, v} ->
      {atomize_binary(k), atomize_sub_opt_value(v)}
    end)
  end

  defp atomize_binary(s) when is_binary(s), do: String.to_atom(s)
  defp atomize_binary(other), do: other

  defp atomize_sub_opt_value(v) when is_binary(v), do: String.to_atom(v)
  defp atomize_sub_opt_value(v), do: v
end
