defmodule Flezha.Plugin.SpecDecoder do
  @moduledoc """
  The one decoder for the JSON plugin-spec array carried by
  `flz operator start --plugins` and `flz schedule add --plugins`. Each spec
  names a plugin (`name`) and the hooks the operator is bound to it with. A
  spec that also names a `provider`, with its `opts` and `single`, enables the
  plugin when it is not enabled yet; a spec that only binds may name the
  binding's `channel` in its `opts`
  (`Flezha.Service.PluginManager.start_bindings/3`).

  Provider ids resolve against the controller's currently enabled providers, so
  the decoded form is RUNTIME state: a durable schedule stores the JSON and
  decodes it again at every firing.

  Refusals: `{:invalid_plugins_json, reason}`, `{:unknown_provider, id}`, the
  hook refusals of `Flezha.Plugin.hooks/1`, `:plugin_spec_not_object`,
  `{:plugin_spec_field_missing, field}` and `{:plugin_spec_field_not_object, field}`.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Providers

  @doc """
  Decode `plugins_json` into one-step plugin structs for `operator_id` in
  `namespace`: the plugin's name as the id, its provider (or `nil` for a spec
  that only binds), its options and the binding's hooks. An empty/blank spec
  decodes to `[]`.
  """
  @spec decode(binary() | nil, atom() | binary(), binary()) ::
          {:ok, [Plugin.t()]} | {:error, term()}
  def decode(plugins_json, namespace, operator_id) do
    case decode_json(plugins_json) do
      {:ok, []} ->
        {:ok, []}

      {:ok, specs} ->
        providers = Providers.all()

        specs
        |> Enum.reduce_while(
          {:ok, []},
          &collect_plugin(&1, &2, providers, namespace, operator_id)
        )
        |> case do
          {:ok, plugins} -> {:ok, Enum.reverse(plugins)}
          err -> err
        end

      {:error, _reason} = err ->
        err
    end
  end

  defp collect_plugin(spec, {:ok, acc}, providers, namespace, operator_id) do
    case build_plugin(spec, providers, namespace, operator_id) do
      {:ok, plugin} -> {:cont, {:ok, [plugin | acc]}}
      {:error, _} = err -> {:halt, err}
    end
  end

  @doc "Structural check only — no provider resolution. Used where the spec is stored, not run."
  @spec valid_json_array?(binary() | nil) :: boolean()
  def valid_json_array?(plugins_json), do: match?({:ok, _}, decode_json(plugins_json))

  ## ----- private -----

  defp decode_json(j) when j in [nil, "", "[]", "{}"], do: {:ok, []}

  defp decode_json(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, list} when is_list(list) ->
        {:ok, list}

      {:ok, _} ->
        {:error, {:invalid_plugins_json, :not_a_json_array}}

      {:error, %Jason.DecodeError{} = err} ->
        {:error, {:invalid_plugins_json, Exception.message(err)}}
    end
  end

  defp decode_json(_), do: {:ok, []}

  # A source binding names no hook, so `hooks` may be left out; a key no spec
  # reads is refused rather than dropped.
  @spec_keys ["name", "provider", "opts", "hooks", "single"]

  defp build_plugin(spec, providers, namespace, operator_id) when is_map(spec) do
    with :ok <- refuse_unknown_keys(spec),
         {:ok, name} <- fetch_string(spec, "name"),
         {:ok, provider_mod} <- resolve_provider(providers, Map.get(spec, "provider")),
         {:ok, raw_opts} <- fetch_map(spec, "opts"),
         {:ok, hooks} <- read_hooks(Map.get(spec, "hooks")) do
      {:ok,
       %Plugin{
         id: name,
         provider: provider_mod,
         opts: Plugin.option_keys(provider_mod, raw_opts),
         hooks: hooks,
         namespace: namespace,
         operator_id: operator_id,
         single: Map.get(spec, "single") == true
       }}
    end
  end

  defp build_plugin(_, _, _, _), do: {:error, :plugin_spec_not_object}

  defp refuse_unknown_keys(spec) do
    case Map.keys(spec) -- @spec_keys do
      [] -> :ok
      unknown -> {:error, {:plugin_fields_unknown, Enum.sort(unknown)}}
    end
  end

  defp read_hooks(nil), do: {:ok, []}
  defp read_hooks(written), do: Plugin.hooks(written)

  defp fetch_string(spec, key) do
    case Map.get(spec, key) do
      s when is_binary(s) and byte_size(s) > 0 -> {:ok, s}
      _ -> {:error, {:plugin_spec_field_missing, key}}
    end
  end

  defp fetch_map(spec, key) do
    case Map.get(spec, key, %{}) do
      m when is_map(m) -> {:ok, m}
      _ -> {:error, {:plugin_spec_field_not_object, key}}
    end
  end

  defp resolve_provider(_providers, nil), do: {:ok, nil}

  defp resolve_provider(providers, provider_id) do
    case Map.get(providers, provider_id) do
      nil -> {:error, {:unknown_provider, provider_id}}
      mod -> {:ok, mod}
    end
  end
end
