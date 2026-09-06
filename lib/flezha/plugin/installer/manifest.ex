defmodule Flezha.Plugin.Installer.Manifest do
  @moduledoc """
  Parses `manifest.exs` into a plain term and validates its shape.

  The manifest is declarative Elixir — we want nested maps/lists with
  native syntax — but we refuse to execute *code*. The parser walks the
  AST returned by `Code.string_to_quoted/2` and rejects anything that
  isn't a literal: function calls, variables, sigils, special forms. Only
  maps, lists, tuples, atoms, binaries, numbers, and module aliases are
  allowed. This keeps the manifest shape flexible without letting a
  malicious package execute `System.get_env/1` at install time.

  Validation enforces the schema the installer promises: id regex, semver
  version, non-empty modules, provides shape (including provider ∈ modules
  and scope == :operative), dependency constraint strings parseable by
  `Version`.
  """

  alias Flezha.Identificator

  @type manifest :: %{
          required(:id) => binary(),
          required(:version) => binary(),
          required(:modules) => [atom()],
          required(:provides) => [map()],
          required(:dependencies) => [{binary(), binary()}],
          optional(:description) => binary(),
          optional(:author) => binary()
        }

  @doc """
  Parses manifest source bytes into an Elixir term. Returns
  `{:ok, term}` or `{:error, :non_literal_ast | {:syntax, reason}}`.
  """
  @spec parse(binary()) :: {:ok, term()} | {:error, :non_literal_ast | {:syntax, term()}}
  def parse(source) when is_binary(source) do
    case Code.string_to_quoted(source) do
      {:ok, ast} ->
        if literal_ast?(ast) do
          try do
            {term, _bindings} = Code.eval_quoted(ast, [])
            {:ok, term}
          rescue
            e -> {:error, {:eval_failed, Exception.message(e)}}
          end
        else
          {:error, :non_literal_ast}
        end

      {:error, reason} ->
        {:error, {:syntax, reason}}
    end
  end

  @doc """
  Validates a parsed manifest term against the spec's schema. Returns
  `:ok` or `{:error, {:manifest_invalid, reason}}` where `reason` names
  the first violation.
  """
  @spec validate(term()) :: :ok | {:error, {:manifest_invalid, term()}}
  def validate(term) when is_map(term) do
    with :ok <- validate_required_keys(term),
         :ok <- validate_id(term[:id]),
         :ok <- validate_version(term[:version]),
         :ok <- validate_modules(term[:modules]),
         :ok <- validate_provides(term[:provides], term[:modules]) do
      validate_dependencies(Map.get(term, :dependencies, []))
    end
  end

  def validate(_), do: {:error, {:manifest_invalid, :not_a_map}}

  ## Private — AST walking

  # Leaves.
  defp literal_ast?(node) when is_atom(node), do: true
  defp literal_ast?(node) when is_binary(node), do: true
  defp literal_ast?(node) when is_number(node), do: true

  # Tuple literal.
  defp literal_ast?({a, b}), do: literal_ast?(a) and literal_ast?(b)

  # Lists.
  defp literal_ast?(list) when is_list(list), do: Enum.all?(list, &literal_ast?/1)

  # Two-element tuple for AST calls comes in 3-tuple form `{name, meta, args}`.
  # We only allow a handful of these:
  #   1. Module aliases: {:__aliases__, meta, [:Foo, :Bar]}
  #   2. Map literal:    {:%{}, meta, [{k, v}, ...]}
  #   3. Tuple sugar for n>2 tuples: {:{}, meta, [elems...]}
  defp literal_ast?({:__aliases__, _meta, segs}) when is_list(segs),
    do: Enum.all?(segs, &is_atom/1)

  defp literal_ast?({:%{}, _meta, pairs}) when is_list(pairs) do
    Enum.all?(pairs, fn
      {k, v} -> literal_ast?(k) and literal_ast?(v)
      _ -> false
    end)
  end

  defp literal_ast?({:{}, _meta, elems}) when is_list(elems),
    do: Enum.all?(elems, &literal_ast?/1)

  # Negative number sugar: `-n` becomes `{:-, meta, [n]}` — allow it.
  defp literal_ast?({:-, _meta, [n]}) when is_number(n), do: true

  # Anything else — function call, variable, sigil, etc. — is forbidden.
  defp literal_ast?(_), do: false

  ## Private — schema validation

  defp validate_required_keys(map) do
    required = [:id, :version, :modules, :provides]

    case Enum.find(required, fn k -> not Map.has_key?(map, k) end) do
      nil -> :ok
      missing -> {:error, {:manifest_invalid, {:missing_field, missing}}}
    end
  end

  defp validate_id(id) do
    case Identificator.validate(:plugin, id) do
      :ok -> :ok
      {:error, _refusal} -> {:error, {:manifest_invalid, {:invalid_id, id}}}
    end
  end

  defp validate_version(v) when is_binary(v) do
    case Version.parse(v) do
      {:ok, _} -> :ok
      :error -> {:error, {:manifest_invalid, {:invalid_version, v}}}
    end
  end

  defp validate_version(v), do: {:error, {:manifest_invalid, {:invalid_version, v}}}

  defp validate_modules(mods) when is_list(mods) and mods != [] do
    if Enum.all?(mods, &is_atom/1) do
      :ok
    else
      {:error, {:manifest_invalid, {:invalid_modules, mods}}}
    end
  end

  defp validate_modules(mods), do: {:error, {:manifest_invalid, {:invalid_modules, mods}}}

  defp validate_provides(provides, modules) when is_list(provides) and provides != [] do
    mod_set = MapSet.new(modules)

    ids =
      Enum.map(provides, fn
        %{id: id} -> id
        _ -> nil
      end)

    duplicate = ids -- Enum.uniq(ids)

    if duplicate == [] do
      Enum.reduce_while(provides, :ok, fn p, _ -> validate_provision(p, mod_set) end)
    else
      {:error, {:manifest_invalid, {:duplicate_provision_id, List.first(duplicate)}}}
    end
  end

  defp validate_provides(provides, _),
    do: {:error, {:manifest_invalid, {:invalid_provides, provides}}}

  defp validate_provision(p, mod_set) when is_map(p) do
    with :ok <- require_keys(p, [:id, :provider, :subscriptions, :scope]),
         id = Map.fetch!(p, :id),
         provider = Map.fetch!(p, :provider),
         subs = Map.fetch!(p, :subscriptions),
         scope = Map.fetch!(p, :scope),
         :ok <- require_binary(id, :invalid_provision_id),
         :ok <- require_module(provider, mod_set),
         :ok <- require_subscriptions(subs),
         :ok <- require_scope(scope),
         :ok <- require_default_opts(Map.get(p, :default_opts, %{})) do
      {:cont, :ok}
    else
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp validate_provision(p, _),
    do: {:halt, {:error, {:manifest_invalid, {:invalid_provision, p}}}}

  defp require_keys(map, keys) do
    case Enum.find(keys, fn k -> not Map.has_key?(map, k) end) do
      nil -> :ok
      missing -> {:error, {:manifest_invalid, {:missing_field, missing}}}
    end
  end

  defp require_binary(v, _) when is_binary(v), do: :ok
  defp require_binary(v, tag), do: {:error, {:manifest_invalid, {tag, v}}}

  defp require_module(mod, mod_set) do
    cond do
      not is_atom(mod) ->
        {:error, {:manifest_invalid, {:provider_not_atom, mod}}}

      not MapSet.member?(mod_set, mod) ->
        {:error, {:manifest_invalid, {:provider_not_declared, mod}}}

      true ->
        :ok
    end
  end

  defp require_subscriptions(subs) when is_list(subs) and subs != [] do
    Enum.reduce_while(subs, :ok, fn
      {name, opts}, _ when is_atom(name) and is_list(opts) ->
        if Keyword.keyword?(opts),
          do: {:cont, :ok},
          else: {:halt, {:error, {:manifest_invalid, {:invalid_subscription_opts, opts}}}}

      other, _ ->
        {:halt, {:error, {:manifest_invalid, {:invalid_subscription, other}}}}
    end)
  end

  defp require_subscriptions(subs),
    do: {:error, {:manifest_invalid, {:invalid_subscriptions, subs}}}

  defp require_scope(:operative), do: :ok
  defp require_scope(scope), do: {:error, {:manifest_invalid, {:unsupported_scope, scope}}}

  defp require_default_opts(opts) when is_map(opts), do: :ok

  defp require_default_opts(opts),
    do: {:error, {:manifest_invalid, {:invalid_default_opts, opts}}}

  defp validate_dependencies([]), do: :ok

  defp validate_dependencies(deps) when is_list(deps) do
    Enum.reduce_while(deps, :ok, fn
      {id, constraint}, _ when is_binary(id) and is_binary(constraint) ->
        case Version.parse_requirement(constraint) do
          {:ok, _} -> {:cont, :ok}
          :error -> {:halt, {:error, {:manifest_invalid, {:invalid_constraint, id, constraint}}}}
        end

      other, _ ->
        {:halt, {:error, {:manifest_invalid, {:invalid_dependency, other}}}}
    end)
  end

  defp validate_dependencies(deps),
    do: {:error, {:manifest_invalid, {:invalid_dependencies, deps}}}
end
