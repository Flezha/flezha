defmodule Flezha.Plugin.Extension.Table.Subscription do
  @moduledoc """
  The extension table's subscriber lists: `subs` and `request_subs`, plus the
  canonicalisation and validation that decide what may enter them.

  It takes the table as a plain map and never names `Extension.Table`. It
  mutates only the subscriber lists — the compiled chain and the active bit
  follow separately, in `Extension.Table.Chain`.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table.Health
  alias Flezha.Plugin.ProviderBehaviour

  @typedoc "One subscriber's registration under an extension name."
  @type entry() :: map()

  @doc "Return every subscriber entry registered for the given extension name."
  @spec subscribers(map(), atom()) :: [entry()]
  def subscribers(table, name) do
    case Map.get(table.catalog, name) do
      %{kind: :request} ->
        table.request_subs
        |> Map.get(name, %{})
        |> Map.values()
        |> Enum.map(fn {_plugin_id, entry} -> entry end)

      _ ->
        Map.get(table.subs, name, [])
    end
  end

  # `position` orders a chain, and the unordered kinds have none — so they
  # declare it refused rather than unknown, which is the answer callers read.
  @position_refused {:refused, :position_not_applicable}

  # The whole sub-option vocabulary, by dispatch kind: every name an operator
  # may write and the type its value carries. Coercion and refusal both read
  # it, so a new sub-option is declared in the module that validates it.
  @sub_opt_types %{
    fold: [position: :int, timeout_ms: :int, on_error: {:enum, ["fail", "empty"]}],
    transform: [position: :int],
    fanout: [position: @position_refused],
    request: [key: :string, position: @position_refused],
    timer: [interval_ms: :int, position: @position_refused]
  }

  # A kind absent from the table would raise at its first subscribe. Fail the
  # build instead.
  @undeclared_kinds Flezha.Plugin.Host.valid_kinds() -- Map.keys(@sub_opt_types)
  if @undeclared_kinds != [] do
    raise "@sub_opt_types declares no sub-options for #{inspect(@undeclared_kinds)}"
  end

  # What a sub-option means when the operator omits it — read by the validator
  # and by the dispatch chain, so it is written once.
  @sub_opt_defaults [timeout_ms: 5_000, on_error: "fail"]

  @doc "One sub-option, falling back to its declared default."
  @spec sub_opt(keyword(), atom()) :: term()
  def sub_opt(sub_opts, name),
    do: Keyword.get(sub_opts, name, Keyword.get(@sub_opt_defaults, name))

  @doc """
  Canonicalise a plugin's subscriptions against `catalog` and the declared
  sub-option types: `{atom, keyword}` tuples whose values carry their declared
  type.

  An extension name may arrive as text or as an atom, and sub-opts as a map or
  a keyword list — the shapes the CLI, the operative-start spec, an installed
  package manifest and direct construction produce. Both are resolved against
  a declaration rather than interned, so operator text never creates an atom;
  a name that resolves to nothing is refused.

  `subscribe/3` runs it, so the table stores canonical values and a provider
  restart, an unsubscribe and a handover all observe them. A caller that writes
  durable intent runs it first, so nothing ill-typed is ever persisted. It is
  idempotent: canonical input is returned unchanged.
  """
  @spec canonicalise(map(), Plugin.map()) :: {:ok, Plugin.map()} | {:error, term()}
  def canonicalise(catalog, %Plugin{subscriptions: subs} = plugin)
      when is_map(catalog) and is_list(subs) do
    subs
    |> Enum.reduce_while({:ok, []}, fn sub, {:ok, acc} ->
      case canonicalise_subscription(catalog, sub) do
        {:ok, canonical} -> {:cont, {:ok, [canonical | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, %Plugin{plugin | subscriptions: Enum.reverse(acc)}}
      err -> err
    end
  end

  def canonicalise(catalog, %Plugin{}) when is_map(catalog),
    do: {:error, {:invalid_subscription, :not_a_list}}

  defp canonicalise_subscription(catalog, {name, sub_opts}) do
    with {:ok, name, kind} <- resolve_extension(catalog, name),
         {:ok, opts} <- canonicalise_sub_opts(kind, sub_opts) do
      {:ok, {name, opts}}
    end
  end

  defp canonicalise_subscription(_catalog, other),
    do: {:error, {:invalid_subscription, other}}

  defp resolve_extension(catalog, name) when is_atom(name) and not is_nil(name) do
    case Map.fetch(catalog, name) do
      {:ok, %{kind: kind}} -> {:ok, name, kind}
      :error -> {:error, {:unknown_extension, name}}
    end
  end

  defp resolve_extension(catalog, name) do
    case Enum.find(catalog, fn {known, _entry} -> same_name?(known, name) end) do
      {known, %{kind: kind}} -> {:ok, known, kind}
      nil -> {:error, {:unknown_extension, name}}
    end
  end

  defp canonicalise_sub_opts(kind, sub_opts) do
    declared = Map.fetch!(@sub_opt_types, kind)

    with {:ok, pairs} <- declared_order(declared, sub_opts) do
      pairs
      |> Enum.reduce_while({:ok, []}, &coerce_pair(declared, kind, &1, &2))
      |> case do
        {:ok, acc} -> {:ok, Enum.reverse(acc)}
        err -> err
      end
    end
  end

  defp coerce_pair(declared, kind, {key, value}, {:ok, acc}) do
    with {:ok, name, type} <- resolve_sub_opt(declared, kind, key),
         {:ok, coerced} <- coerce_sub_opt(type, name, value) do
      {:cont, {:ok, [{name, coerced} | acc]}}
    else
      {:error, _} = err -> {:halt, err}
    end
  end

  # A map carries no order to preserve, so it is emitted in declared order:
  # one declaration canonicalises to one term whatever shape it arrived in.
  # A keyword list already has one, and keeping it makes canonical input
  # come back unchanged.
  defp declared_order(declared, sub_opts) when is_map(sub_opts) do
    {:ok,
     Enum.sort_by(sub_opts, fn {key, _value} ->
       Enum.find_index(declared, fn {name, _type} -> same_name?(name, key) end) ||
         length(declared)
     end)}
  end

  defp declared_order(_declared, sub_opts) when is_list(sub_opts) do
    if Enum.all?(sub_opts, &match?({_k, _v}, &1)),
      do: {:ok, sub_opts},
      else: {:error, {:invalid_subscription_opts, :not_a_keyword}}
  end

  defp declared_order(_declared, _sub_opts),
    do: {:error, {:invalid_subscription_opts, :not_a_keyword}}

  defp resolve_sub_opt(declared, kind, key) do
    case Enum.find(declared, fn {name, _type} -> same_name?(name, key) end) do
      {name, type} -> {:ok, name, type}
      nil -> {:error, {:unknown_subscription_opt, kind, key}}
    end
  end

  defp same_name?(declared, key) when is_atom(key) and not is_nil(key),
    do: declared == key

  defp same_name?(declared, key) when is_binary(key), do: Atom.to_string(declared) == key
  defp same_name?(_declared, _key), do: false

  # Reuses the provider layer's parse_int/2 — the same "the CLI ships text"
  # problem, already solved once.
  defp coerce_sub_opt(:int, name, value) when is_integer(value) or is_binary(value) do
    case ProviderBehaviour.parse_int(value, name) do
      {:ok, int} -> {:ok, int}
      {:error, _msg} -> invalid_sub_opt(name, value)
    end
  end

  defp coerce_sub_opt(:string, _name, value) when is_binary(value), do: {:ok, value}

  defp coerce_sub_opt(:string, _name, value) when is_atom(value) and not is_nil(value),
    do: {:ok, Atom.to_string(value)}

  defp coerce_sub_opt({:enum, allowed}, name, value)
       when is_binary(value) or (is_atom(value) and not is_nil(value)) do
    text = to_string(value)

    if text in allowed, do: {:ok, text}, else: invalid_sub_opt(name, value)
  end

  defp coerce_sub_opt({:refused, reason}, _name, _value), do: {:error, reason}

  defp coerce_sub_opt(_type, name, value), do: invalid_sub_opt(name, value)

  defp invalid_sub_opt(name, value), do: {:error, {:invalid_subscription_opts, {name, value}}}

  ## Private — validation

  def validate_plugin_unique(%{plugins: plugins}, plugin_id) do
    if Map.has_key?(plugins, plugin_id) do
      {:error, {:plugin_already_subscribed, plugin_id}}
    else
      :ok
    end
  end

  def validate_subscriptions(%{catalog: catalog} = table, %Plugin{} = plugin) do
    Enum.reduce_while(plugin.subscriptions, :ok, fn {name, sub_opts}, :ok ->
      with {:ok, entry} <- fetch_catalog_entry(catalog, name),
           :ok <- validate_callback(plugin.provider, name, entry.kind),
           :ok <- validate_kind_specific(entry.kind, name, sub_opts, table, plugin) do
        {:cont, :ok}
      else
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp fetch_catalog_entry(catalog, name) do
    case Map.fetch(catalog, name) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, {:unknown_extension, name}}
    end
  end

  # The declaration says which extension callbacks a provider implements. It is
  # derived at compile time from the same source a run-time export probe reads,
  # so the refusal is unchanged and nothing asks the code loader.
  defp validate_callback(provider, name, kind) do
    callback = callback_name(name, kind)

    cond do
      not Code.ensure_loaded?(provider) ->
        {:error, {:module_not_loaded, provider}}

      callback not in provider.__provider__().extensions ->
        {:error, {:missing_callback, callback}}

      true ->
        :ok
    end
  end

  defp callback_name(name, _kind), do: name

  defp validate_kind_specific(:timer, _name, sub_opts, _table, _plugin),
    do: validate_positive(sub_opts, :interval_ms)

  defp validate_kind_specific(:fanout, _name, _sub_opts, _table, _plugin), do: :ok

  defp validate_kind_specific(:request, name, sub_opts, table, _plugin) do
    case Keyword.fetch(sub_opts, :key) do
      :error ->
        {:error, {:invalid_subscription_opts, :missing_key}}

      {:ok, key} ->
        case get_in(table.request_subs, [name, key]) do
          nil -> :ok
          _ -> {:error, {:key_conflict, name, key}}
        end
    end
  end

  defp validate_kind_specific(:fold, _name, sub_opts, _table, _plugin) do
    with :ok <- validate_position(sub_opts) do
      validate_positive(sub_opts, :timeout_ms)
    end
  end

  defp validate_kind_specific(:transform, _name, sub_opts, _table, _plugin),
    do: validate_position(sub_opts)

  # Canonicalisation types `position` as an integer; only the range is left,
  # and a negative one would still reach `insert_by_position/3`.
  defp validate_position(sub_opts) do
    case Keyword.get(sub_opts, :position) do
      nil -> :ok
      n when is_integer(n) and n >= 0 -> :ok
      other -> {:error, {:invalid_subscription_opts, {:position, other}}}
    end
  end

  # The bounds a type cannot express. `interval_ms` has no default, so an
  # absent one fails the guard — which is what makes it required.
  defp validate_positive(sub_opts, name) do
    case sub_opt(sub_opts, name) do
      n when is_integer(n) and n > 0 -> :ok
      other -> {:error, {:invalid_subscription_opts, {name, other}}}
    end
  end

  def add_subscription(table, kind, name, plugin_id, plugin, sub_opts)
      when kind in [:fold, :transform] do
    entry = build_entry(plugin_id, plugin, sub_opts)

    current = Map.get(table.subs, name, [])
    position = Keyword.get(sub_opts, :position)
    new_list = insert_by_position(current, entry, position)

    Map.update!(table, :subs, &Map.put(&1, name, new_list))
  end

  def add_subscription(table, :fanout, name, plugin_id, plugin, sub_opts) do
    entry = build_entry(plugin_id, plugin, sub_opts)
    current = Map.get(table.subs, name, [])
    new_list = current ++ [entry]

    Map.update!(table, :subs, &Map.put(&1, name, new_list))
  end

  def add_subscription(table, :request, name, plugin_id, plugin, sub_opts) do
    key = Keyword.fetch!(sub_opts, :key)
    current = Map.get(table.request_subs, name, %{})
    entry = build_entry(plugin_id, plugin, sub_opts)
    new_map = Map.put(current, key, {plugin_id, entry})

    Map.update!(table, :request_subs, &Map.put(&1, name, new_map))
  end

  def add_subscription(table, :timer, name, plugin_id, plugin, sub_opts) do
    entry = build_entry(plugin_id, plugin, sub_opts)

    interval_ms = Keyword.fetch!(sub_opts, :interval_ms)
    timer_ref = Process.send_after(self(), {:extension_timer, name, plugin_id}, interval_ms)

    entry = %{entry | timer_ref: timer_ref}
    current = Map.get(table.subs, name, [])
    new_list = current ++ [entry]

    table
    |> Map.update!(:subs, &Map.put(&1, name, new_list))
    |> Map.update!(:timers, &Map.put(&1, timer_ref, {plugin_id, name}))
  end

  defp build_entry(plugin_id, %Plugin{provider: provider, opts: opts}, sub_opts) do
    %{
      plugin_id: plugin_id,
      provider: provider,
      provider_opts: opts || %{},
      sub_opts: sub_opts,
      position: Keyword.get(sub_opts, :position),
      owned_pid: nil,
      monitor_ref: nil,
      timer_ref: nil
    }
  end

  defp insert_by_position(list, entry, nil) do
    position = length(list)
    list ++ [%{entry | position: position}]
  end

  defp insert_by_position(list, entry, position)
       when is_integer(position) and position >= 0 do
    {before_split, after_split} = Enum.split(list, position)
    entry = %{entry | position: position}

    shifted =
      after_split
      |> Enum.with_index(position + 1)
      |> Enum.map(fn {e, new_pos} -> %{e | position: new_pos} end)

    before_split ++ [entry] ++ shifted
  end

  def remove_subscription(table, name, plugin_id) do
    case Map.fetch!(table.catalog, name) do
      %{kind: :request} ->
        request_map =
          table.request_subs
          |> Map.get(name, %{})
          |> Enum.reject(fn {_key, {pid_id, _entry}} -> pid_id == plugin_id end)
          |> Map.new()

        %{table | request_subs: Map.put(table.request_subs, name, request_map)}

      %{kind: :timer} ->
        {removed, remaining} =
          Enum.split_with(Map.get(table.subs, name, []), &(&1.plugin_id == plugin_id))

        Enum.each(removed, fn e ->
          Health.safe_cancel_timer(e.timer_ref)
        end)

        timers_without =
          Enum.reduce(removed, table.timers, fn e, acc -> Map.delete(acc, e.timer_ref) end)

        %{table | subs: Map.put(table.subs, name, remaining), timers: timers_without}

      %{kind: kind} when kind in [:fold, :transform] ->
        current = Map.get(table.subs, name, [])
        remaining = Enum.reject(current, &(&1.plugin_id == plugin_id))
        remaining = recalc_positions(remaining)

        %{table | subs: Map.put(table.subs, name, remaining)}

      %{kind: :fanout} ->
        current = Map.get(table.subs, name, [])
        remaining = Enum.reject(current, &(&1.plugin_id == plugin_id))

        %{table | subs: Map.put(table.subs, name, remaining)}
    end
  end

  defp recalc_positions(list) do
    list
    |> Enum.with_index()
    |> Enum.map(fn {entry, idx} -> %{entry | position: idx} end)
  end

  def reschedule_timer(table, name, plugin_id, entry) do
    Health.safe_cancel_timer(entry.timer_ref)
    timers = Map.delete(table.timers, entry.timer_ref)

    interval_ms = Keyword.fetch!(entry.sub_opts, :interval_ms)

    new_timer =
      Process.send_after(self(), {:extension_timer, name, plugin_id}, interval_ms)

    updated_entry = %{entry | timer_ref: new_timer}

    subs =
      Map.update!(table.subs, name, fn list ->
        Enum.map(list, &if(&1.plugin_id == plugin_id, do: updated_entry, else: &1))
      end)

    %{table | subs: subs, timers: Map.put(timers, new_timer, {plugin_id, name})}
  end

  defp owned_entry({pid_id, entry}, plugin_id), do: if(pid_id == plugin_id, do: entry)

  def find_entry(table, name, plugin_id) do
    case Map.fetch(table.catalog, name) do
      {:ok, %{kind: :request}} ->
        table.request_subs
        |> Map.get(name, %{})
        |> Map.values()
        |> Enum.find_value(&owned_entry(&1, plugin_id))

      {:ok, _} ->
        table.subs
        |> Map.get(name, [])
        |> Enum.find(&(&1.plugin_id == plugin_id))

      :error ->
        nil
    end
  end
end
