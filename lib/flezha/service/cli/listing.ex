defmodule Flezha.Service.Cli.Listing do
  @moduledoc """
  `flz list namespaces | runners | operatives` and `flz cluster status`.
  """

  alias Flezha.Partition
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Controller
  alias Flezha.Service.HubManager

  # A group is exactly as healthy as its sickest member: an aggregate that
  # rounded up would hide the one partition that stopped serving its keys.
  @health_order [:healthy, :degraded, :unbound, :failed]

  @doc "Lists all running namespaces."
  def list_namespaces(output_format \\ "text") do
    namespaces = Flezha.list_namespaces()
    handle_list_output(namespaces, "running namespaces", output_format)
  end

  defp handle_list_output(items, item_name, output_format, context \\ nil) do
    case {items, output_format} do
      {items, "json"} ->
        result =
          case context do
            nil -> %{String.to_atom(item_name) => items}
            %{key: key, value: value} -> %{String.to_atom(item_name) => items, key => value}
          end

        IO.puts(Jason.encode!(result))

      {[], _} ->
        message =
          case context do
            nil ->
              "No #{item_name} found."

            %{display: display, value: value} ->
              "No #{item_name} found for #{display} '#{value}'."
          end

        IO.puts(message)

      {items, _} ->
        header =
          case context do
            nil ->
              "#{String.capitalize(item_name)}:"

            %{display: display, value: value} ->
              "#{String.capitalize(item_name)} in #{display} '#{value}':"
          end

        IO.puts(header)
        Enum.each(items, &IO.puts("  - #{&1}"))
    end
  end

  @doc "Lists runners for a specific namespace."
  def list_runners(namespace, output_format \\ "text") do
    namespace_display = Context.normalize_namespace(namespace)

    case Flezha.list_runners(namespace_display) do
      {:ok, runners} when is_list(runners) ->
        context = %{key: :namespace, value: namespace_display, display: "namespace"}
        handle_list_output(runners, "runners", output_format, context)
    end
  end

  @doc """
  Lists a namespace's operatives with the reconciler's per-operative health, assigned
  runner, and live node. Reads the durable registry, so it surfaces FAILED
  operatives (whose only placement is a dead node) and is multi-node-safe — it no
  longer crashes on a operative registered across nodes.
  """
  def list_operatives(namespace, output_format \\ "text", opts \\ []) do
    namespace_display = Context.normalize_namespace(namespace)

    # Listings are namespace-scoped: a principal bound to another namespace is
    # refused here (namespace_mismatch), so no verb leaks another tenant's rows.
    _ = Context.authorize_cli!(opts, :"operative.list", namespace_display)

    rows =
      namespace_display
      |> HubManager.operative_rows()
      |> group_partition_rows()

    render_operative_list(rows, namespace_display, output_format)
  end

  @doc """
  Collapse a partition group's member rows into one logical row.

  A pure function between `HubManager.operative_rows/1` and the renderers, so
  the group view is recomputed from the rows on every read — there is no stored
  group record that could disagree with them. Ungrouped operatives pass
  through untouched, which is why `cluster_status` (whose per-node counts are
  per-process facts) simply does not call this.
  """
  @spec group_partition_rows([map()]) :: [map()]
  def group_partition_rows(rows) do
    plain = Enum.reject(rows, &(&1[:partition] != nil))
    Enum.sort_by(plain ++ Enum.map(Partition.groups(rows), &group_row/1), &inspect(&1.id))
  end

  defp group_row(report) do
    members = report.members

    %{
      id: report.group,
      health: worst_health(members),
      runner:
        Enum.map_join(
          members,
          ", ",
          &"p#{&1.partition.index}→#{Context.label_or(&1.runner, "-")}"
        ),
      factory: members |> hd() |> Map.get(:factory),
      partitions: "#{length(members)}/#{report.declared}",
      node: members |> hd() |> Map.get(:node),
      members: members
    }
  end

  defp worst_health(rows) do
    rows
    |> Enum.map(& &1.health)
    |> Enum.max_by(fn health ->
      case Enum.find_index(@health_order, &(&1 == health)) do
        nil -> length(@health_order)
        index -> index
      end
    end)
  end

  defp render_operative_list(rows, namespace, "json") do
    result = %{
      namespace: namespace,
      operatives:
        Enum.map(rows, fn r ->
          %{
            id: r.id,
            health: r.health,
            runner: r.runner,
            node: r.node && to_string(r.node)
          }
          |> Context.put_unless_nil(:partitions, r[:partitions])
        end)
    }

    IO.puts(Jason.encode!(result))
  end

  defp render_operative_list([], namespace, _) do
    IO.puts("No operatives found for namespace '#{namespace}'.")
  end

  defp render_operative_list(rows, namespace, _) do
    IO.puts("Operatives in namespace '#{namespace}':")
    IO.puts("")

    IO.puts([
      "  ",
      String.pad_trailing("OPERATIVE", 34),
      String.pad_trailing("HEALTH", 10),
      String.pad_trailing("PARTS", 8),
      String.pad_trailing("NODE", 22),
      "RUNNER"
    ])

    Enum.each(rows, fn r ->
      IO.puts([
        "  ",
        String.pad_trailing(to_string(r.id), 34),
        String.pad_trailing(to_string(r.health), 10),
        String.pad_trailing(Context.label_or(r[:partitions], "-"), 8),
        String.pad_trailing(Context.label_or(r.node, "(unplaced)"), 22),
        Context.label_or(r.runner, "(none)")
      ])
    end)
  end

  @doc """
  Cluster topology for operators: the elected leader, whether this node is the
  leader, each node's live operative count, any unplaced (FAILED) operatives, and a
  health rollup. Composes `HubManager` registry access with the reconciler's
  `Leader` so a split view (one node sees operatives, its peer sees none) is
  visible at a glance.
  """
  def cluster_status(namespace, output_format \\ "text") do
    namespace_display = Context.normalize_namespace(namespace)
    nodes = HubManager.get_hub_nodes(namespace_display, true)
    rows = HubManager.operative_rows(namespace_display)
    counts = Enum.frequencies_by(rows, & &1.node)
    health = Enum.frequencies_by(rows, & &1.health)
    recovery_per_node = recovery_per_node(namespace_display, nodes)

    render_cluster_status(
      %{
        namespace: namespace_display,
        nodes: nodes,
        leader: safe_leader(),
        is_leader: safe_is_leader(),
        counts: counts,
        health: health,
        recovery_per_node: recovery_per_node
      },
      output_format
    )
  end

  # Through the published read, so a refusal is reported rather than rendered as
  # a namespace with no recovery state — an empty column and an unanswerable one
  # look identical to an operator.
  defp recovery_per_node(namespace, nodes) do
    case Controller.recovery_per_node(namespace, nodes, Context.cli_context([])) do
      {:ok, per_node} -> per_node
      {:error, reason} -> Context.cli_error!("error: #{inspect(reason)}", :stderr)
    end
  end

  defp render_cluster_status(s, "json") do
    result = %{
      namespace: s.namespace,
      leader: s.leader && to_string(s.leader),
      local_node: to_string(node()),
      is_leader: s.is_leader,
      nodes:
        Enum.map(s.nodes, fn n ->
          %{
            node: to_string(n),
            operative_count: Map.get(s.counts, n, 0),
            leader: n == s.leader,
            recovery_state: Map.get(s.recovery_per_node, n, :unknown)
          }
        end),
      unplaced_operatives: Map.get(s.counts, nil, 0),
      operative_health: s.health
    }

    IO.puts(Jason.encode!(result))
  end

  defp render_cluster_status(s, _) do
    IO.puts("Cluster status for namespace '#{s.namespace}':")
    IO.puts("")
    IO.puts("  Leader:     #{Context.label_or(s.leader, "(none)")}")
    IO.puts("  Local node: #{node()}#{if s.is_leader, do: " (leader)", else: ""}")
    IO.puts("")
    IO.puts("  Nodes:")

    if s.nodes == [] do
      IO.puts("    (none)")
    else
      Enum.each(s.nodes, fn n ->
        marker = if n == s.leader, do: " [leader]", else: ""
        rs = Map.get(s.recovery_per_node, n, :unknown)

        IO.puts("    #{n}: #{Map.get(s.counts, n, 0)} operative(s) recovery_state=#{rs}#{marker}")
      end)
    end

    case Map.get(s.counts, nil, 0) do
      0 -> :ok
      unplaced -> IO.puts("    (unplaced): #{unplaced} operative(s)")
    end

    IO.puts("")
    IO.puts("  Operative health: #{health_summary(s.health)}")
  end

  defp health_summary(health) when map_size(health) == 0, do: "no operatives"

  defp health_summary(health) do
    health
    |> Enum.map(fn {state, count} -> "#{state}=#{count}" end)
    |> Enum.join(", ")
  end

  # There is no leader-election process: the convergence Loop uses local
  # reads + process_hub's own coordinator for cluster-wide work. The CLI
  # cluster_status surface keeps the column and surfaces the local node
  # as the de-facto leader.
  defp safe_leader do
    node()
  end

  defp safe_is_leader do
    true
  end
end
