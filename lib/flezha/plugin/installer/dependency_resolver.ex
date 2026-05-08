defmodule Flezha.Plugin.Installer.DependencyResolver do
  @moduledoc """
  Resolves plugin-to-plugin dependency constraints against the live
  installed-plugin catalog.

  The installer evaluates a new manifest's `:dependencies` list of
  `{plugin_id, constraint}` tuples. Every entry must point at an
  already-installed plugin whose `:version` satisfies the constraint
  (via `Version.match?/2`). The graph formed by every manifest's deps
  plus the new manifest must remain acyclic.

  Upgrades additionally check that dependents of the upgrading plugin
  still have their constraints satisfied by the new version.

  No transitive install: the executor installs deps in order. If a
  dep is missing the install is refused with a clear error.
  """

  alias Flezha.Plugin.Installer.Store

  @type catalog_row :: {binary(), map()}

  @doc """
  Validates that every dep in `manifest` is satisfied by `catalog` and
  that adding `manifest` would not create a cycle. Returns `:ok` or
  `{:error, ...}`.
  """
  @spec validate_install(map(), [catalog_row()]) ::
          :ok
          | {:error,
             {:dependency_missing, binary(), binary()}
             | {:dependency_constraint, binary(), binary(), binary()}
             | {:dependency_cycle, [binary()]}}
  def validate_install(manifest, catalog) do
    with :ok <- check_deps_present(manifest, catalog),
         :ok <- check_cycles(manifest, catalog) do
      :ok
    end
  end

  @doc """
  Validates an upgrade: `new_manifest` replaces `old_row` at `plugin_id`.
  Same checks as `validate_install/2` plus a dependents check — for every
  existing plugin whose deps name `plugin_id`, the new version must still
  satisfy the constraint.
  """
  @spec validate_upgrade(map(), binary(), [catalog_row()]) ::
          :ok | {:error, term()}
  def validate_upgrade(new_manifest, plugin_id, catalog) when is_binary(plugin_id) do
    catalog_without_self = Enum.reject(catalog, fn {id, _} -> id == plugin_id end)

    with :ok <- validate_install(new_manifest, catalog_without_self),
         :ok <- check_dependents_satisfied(new_manifest, plugin_id, catalog_without_self) do
      :ok
    end
  end

  @doc """
  Return the list of plugin ids that declare a dep on `plugin_id` in the
  given catalog.
  """
  @spec find_dependents(binary(), [catalog_row()]) :: [binary()]
  def find_dependents(plugin_id, catalog) when is_binary(plugin_id) do
    Enum.flat_map(catalog, fn {id, row} ->
      deps = Map.get(row, :dependencies, [])

      if Enum.any?(deps, fn
           {^plugin_id, _} -> true
           _ -> false
         end) do
        [id]
      else
        []
      end
    end)
  end

  @doc "Read the live catalog from the Store (convenience)."
  @spec live_catalog() :: [catalog_row()]
  def live_catalog, do: Store.all()

  ## Private

  defp check_deps_present(manifest, catalog) do
    catalog_map = Map.new(catalog)

    manifest
    |> Map.get(:dependencies, [])
    |> Enum.reduce_while(:ok, fn {dep_id, constraint}, _ ->
      case Map.fetch(catalog_map, dep_id) do
        :error ->
          {:halt, {:error, {:dependency_missing, dep_id, constraint}}}

        {:ok, row} ->
          version = row[:version] || ""

          case Version.match?(version, constraint) do
            true -> {:cont, :ok}
            false -> {:halt, {:error, {:dependency_constraint, dep_id, constraint, version}}}
          end
      end
    end)
  rescue
    # Version.match? raises on malformed version strings; treat as failure.
    e -> {:error, {:dependency_constraint, :version_raise, Exception.message(e)}}
  end

  # Build an adjacency map from the current catalog PLUS the new manifest's
  # deps, treating the new id as a node pointing at each of its deps. Run an
  # iterative DFS with color marking and report any cycle back including its
  # full path.
  defp check_cycles(manifest, catalog) do
    new_id = manifest[:id]
    new_deps = manifest |> Map.get(:dependencies, []) |> Enum.map(fn {id, _} -> id end)

    adjacency =
      catalog
      |> Enum.into(%{}, fn {id, row} ->
        deps = Map.get(row, :dependencies, []) |> Enum.map(fn {dep_id, _} -> dep_id end)
        {id, deps}
      end)
      |> Map.put(new_id, new_deps)

    # DFS from the new node — if a cycle exists that includes the new
    # manifest, we'll hit it here.
    case dfs_cycle(new_id, adjacency, %{}, [new_id]) do
      :ok -> :ok
      {:cycle, path} -> {:error, {:dependency_cycle, path}}
    end
  end

  defp dfs_cycle(node, adjacency, colors, stack) do
    case Map.get(colors, node) do
      :black ->
        :ok

      :gray ->
        {:cycle, stack |> Enum.reverse()}

      _ ->
        colors = Map.put(colors, node, :gray)
        children = Map.get(adjacency, node, [])

        result =
          Enum.reduce_while(children, {:ok, colors}, fn child, {:ok, colors} ->
            child_stack = [child | stack]

            case dfs_cycle(child, adjacency, colors, child_stack) do
              :ok -> {:cont, {:ok, Map.put(colors, child, :black)}}
              {:cycle, path} -> {:halt, {:cycle, path}}
            end
          end)

        case result do
          {:ok, _colors} -> :ok
          {:cycle, path} -> {:cycle, path}
        end
    end
  end

  # For an upgrade of `plugin_id` to the manifest's version, refuse if any
  # installed dependent has a constraint the new version fails.
  defp check_dependents_satisfied(new_manifest, plugin_id, catalog) do
    new_version = new_manifest[:version]
    dependents = find_dependents(plugin_id, catalog)

    blocking =
      Enum.flat_map(dependents, fn dep_id ->
        {^dep_id, row} = Enum.find(catalog, fn {id, _} -> id == dep_id end)
        deps = Map.get(row, :dependencies, [])

        Enum.flat_map(deps, fn
          {^plugin_id, constraint} ->
            if Version.match?(new_version, constraint) do
              []
            else
              [{dep_id, constraint}]
            end

          _ ->
            []
        end)
      end)

    case blocking do
      [] -> :ok
      _ -> {:error, {:dependents_block, blocking}}
    end
  rescue
    _ -> {:error, {:dependents_block, :version_raise}}
  end
end
