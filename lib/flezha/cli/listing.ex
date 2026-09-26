defmodule Flezha.Service.Cli.Listing do
  @moduledoc """
  `flz namespace list`, `flz list runners | operators` and `flz cluster
  status`. The first answers the durable catalog; the rest read live state.
  """

  alias Flezha.AuthZ.Namespace
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Convergence.Snapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Partition
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Controller
  alias Flezha.Service.HubManager

  # A group is exactly as healthy as its sickest member: an aggregate that
  # rounded up would hide the one partition that stopped serving its keys.

  @doc """
  Lists the namespaces in the durable catalog, each with whether this
  controller runs a hub for it. Reading the running hub set alone left a
  namespace created a moment earlier invisible until something connected to
  it, and reported one that had been torn down as gone from the catalog.
  """
  def list_namespaces(output_format, call) do
    running = MapSet.new(HubManager.get_running_namespaces())

    # `unbound: :all`: the verb is `:caller`-scoped, and a caller with no
    # namespace here is an ungated deployment's caller. Narrowing it to
    # `_system` would hide the namespaces that caller just created.
    NamespaceStore.list()
    |> Enum.map(& &1.name)
    |> Namespace.visible(call.context, :all)
    |> Enum.sort()
    |> Enum.map(&%{namespace: &1, running: MapSet.member?(running, &1)})
    |> render_list(output_format, %{
      json: fn rows -> %{namespaces: rows} end,
      empty: "No namespaces found.",
      header: "Namespaces:",
      columns: [
        {"NAMESPACE", 35, & &1.namespace},
        {"RUNNING", 0, &if(&1.running, do: "yes", else: "no")}
      ]
    })
  end

  # Every `flz` listing answers three ways: a JSON document, one line saying
  # there is nothing, or a padded table. Only the two texts and the columns
  # differ, so the shape is written once and each listing declares its own.
  # JSON first, so an empty list still renders a document rather than prose.
  defp render_list(rows, "json", %{json: json}), do: IO.puts(Jason.encode!(json.(rows)))
  defp render_list([], _format, %{empty: empty}), do: IO.puts(empty)

  defp render_list(rows, _format, %{header: header, columns: columns}) do
    IO.puts(header)
    IO.puts("")
    IO.puts(line(columns, fn {title, _width, _render} -> title end))

    Enum.each(rows, fn row ->
      IO.puts(line(columns, fn {_title, _width, render} -> render.(row) end))
    end)
  end

  # A column pads to its declared width. The last one declares 0, which
  # `pad_trailing/2` leaves alone, so no line ends in trailing whitespace.
  defp line(columns, text) do
    [
      "  "
      | Enum.map(columns, fn {_title, width, _render} = column ->
          String.pad_trailing(text.(column), width)
        end)
    ]
  end

  @doc """
  Lists runners for a specific namespace. Bulleted rather than tabular: a
  runner id is the whole row, so there is no second column to line up.
  """
  def list_runners(namespace, output_format, _call) do
    namespace_display = Namespace.or_default(namespace)
    {:ok, runners} = Flezha.list_runners(namespace_display)

    case {runners, output_format} do
      {_, "json"} ->
        IO.puts(Jason.encode!(%{runners: runners, namespace: namespace_display}))

      {[], _} ->
        IO.puts("No runners found for namespace '#{namespace_display}'.")

      _ ->
        IO.puts("Runners in namespace '#{namespace_display}':")
        Enum.each(runners, &IO.puts("  - #{&1}"))
    end
  end

  @doc """
  Lists a namespace's operators with the reconciler's per-operator health, assigned
  runner, and live node. Reads the durable registry, so it surfaces FAILED
  operators (whose only placement is a dead node) and is multi-node-safe — it no
  longer crashes on an operator registered across nodes.

  A `failed` or `unbound` row reads `recovering` while the loop is still inside
  the unmet-intent grace `/healthz` uses, so a takeover in progress is not
  shown as a fault; the table and the JSON render the same word.
  """
  def list_operators(namespace, output_format, _call) do
    namespace_display = Namespace.or_default(namespace)
    facts = Snapshot.unmet_intent()

    namespace_display
    |> HubManager.operator_rows()
    |> group_partition_rows()
    |> Enum.map(&%{&1 | health: Verdict.listed_state(&1.health, facts)})
    |> render_list(output_format, %{
      json: fn rows ->
        %{namespace: namespace_display, operators: Enum.map(rows, &operator_json/1)}
      end,
      empty: "No operators found for namespace '#{namespace_display}'.",
      header: "Operators in namespace '#{namespace_display}':",
      columns: [
        {"OPERATOR", 34, &to_string(&1.id)},
        {"HEALTH", 12, &to_string(&1.health)},
        {"PARTS", 8, &Context.label_or(&1[:partitions], "-")},
        {"NODE", 22, &Context.label_or(&1.node, "(unplaced)")},
        {"RUNNER", 0, &Context.label_or(&1.runner, "(none)")}
      ]
    })
  end

  @doc """
  Collapse a partition group's member rows into one logical row.

  A pure function between `HubManager.operator_rows/1` and the renderers, so
  the group view is recomputed from the rows on every read — there is no stored
  group record that could disagree with them. Ungrouped operators pass
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

  # The worst row of a group, by the record module's one rank — the same order
  # `/healthz` folds by, so a group and the endpoint cannot disagree about
  # which member is the problem.
  defp worst_health(rows) do
    rows |> Enum.map(& &1.health) |> Enum.max_by(&Verdict.rank/1)
  end

  defp operator_json(row) do
    %{
      id: row.id,
      health: row.health,
      runner: row.runner,
      node: row.node && to_string(row.node)
    }
    |> Context.put_unless_nil(:partitions, row[:partitions])
  end

  @doc """
  Cluster topology for callers: the elected leader, whether this node is the
  leader, each node's live operator count, any unplaced (FAILED) operators, and a
  health rollup. Composes `HubManager` registry access with the reconciler's
  `Leader` so a split view (one node sees operators, its peer sees none) is
  visible at a glance.
  """
  def cluster_status(namespace, output_format, _call) do
    namespace_display = Namespace.or_default(namespace)
    nodes = HubManager.get_hub_nodes(namespace_display, true)
    rows = HubManager.operator_rows(namespace_display)
    counts = Enum.frequencies_by(rows, & &1.node)
    health = Enum.frequencies_by(rows, & &1.health)
    recovery_per_node = Controller.recovery_map(namespace_display, nodes)

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
            operator_count: Map.get(s.counts, n, 0),
            leader: n == s.leader,
            recovery_state: Map.get(s.recovery_per_node, n, :unknown)
          }
        end),
      unplaced_operators: Map.get(s.counts, nil, 0),
      operator_health: s.health
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

        IO.puts("    #{n}: #{Map.get(s.counts, n, 0)} operator(s) recovery_state=#{rs}#{marker}")
      end)
    end

    case Map.get(s.counts, nil, 0) do
      0 -> :ok
      unplaced -> IO.puts("    (unplaced): #{unplaced} operator(s)")
    end

    IO.puts("")
    IO.puts("  Operator health: #{health_summary(s.health)}")
  end

  defp health_summary(health) when map_size(health) == 0, do: "no operators"

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
