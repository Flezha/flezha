defmodule Flezha.Operative.PartitionGroup do
  @moduledoc """
  The lifecycle of a partition group: starting one, reading its members, and
  rescaling it.

  A partition is an *ordinary* operative instance, so every member start and
  stop goes through `Flezha.Service.OperativeManager` unchanged — this module
  owns only what is true of the group: the preflights that must all pass before
  the first member starts, the all-or-nothing rollback, and the
  quiesce → move → restart sequence a rescale is.

  There is no stored group record. Membership is read back from the members'
  own cspec opts (`Flezha.Partition.identity/1`), so a missing member is simply
  absent here and the CLI's coverage check is what makes that visible.
  """

  require Logger

  alias Flezha.AuthZ.Quotas
  alias Flezha.Dlq
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Partition
  alias Flezha.Plugin
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Validator
  alias Flezha.State.InputLog
  alias Flezha.State.Registry, as: StateRegistry
  alias Flezha.State.WalTransfer

  @doc """
  Start `operative` as a partition group of `partitions` members.

  The group is N ordinary starts through `start_operative/3` — `<id>#p0` …
  `<id>#p{N-1}`, each carrying its own key-group range in its builder opts and
  therefore in its durable cspec. There is no group record: every group-level
  view is recomputed from the member rows.

  Everything that can refuse runs before the first member starts, because a
  half-started group is worse than a refused one. On a mid-group failure the
  already-started members are stopped and the error names the member that
  failed and why.

  `plugins_json` is decoded once per member with that member's id and partition
  opts (`SpecDecoder.decode/4`), so each member's source knows
  which share of the stream is its own.

  Returns `{:ok, %{group:, members:, warnings:}}`; `warnings` carries advice
  the start proceeded despite (fewer runners than partitions).
  """
  @spec start(String.t(), Flezha.Operative.Builder.t(), pos_integer(), binary()) ::
          {:ok, map()} | {:error, term()}
  def start(namespace, operative, partitions, plugins_json \\ "[]") do
    plugins = fn member_id, partition_opts ->
      SpecDecoder.decode(plugins_json, namespace, member_id, partition_opts)
    end

    with :ok <- check_partitionable(namespace, operative, partitions, 0) do
      start_members(namespace, operative, Partition.ranges(partitions), plugins)
    end
  end

  @doc """
  The member ids of `group_id` currently registered in `namespace`, in
  partition order, each with the partition identity read off its cspec opts.

  The one way "who is in this group" is answered — there is no stored group
  record, so a member that is missing is simply absent here and the coverage
  check is what makes that visible.
  """
  @spec members(String.t(), String.t()) :: [%{id: String.t(), partition: map()}]
  def members(namespace, group_id) do
    namespace
    |> HubManager.operative_rows()
    |> Enum.filter(&(&1[:partition] != nil and &1.partition.group == group_id))
    |> Enum.map(&%{id: &1.id, partition: &1.partition, node: &1[:node]})
    |> Enum.sort_by(& &1.partition.index)
  end

  defp check_partitionable(namespace, operative, partitions, already_held) do
    cond do
      partitions < 2 or partitions > Partition.key_groups() ->
        {:error,
         "partitions must be between 2 and #{Partition.key_groups()} " <>
           "(one partition per key group); got #{partitions}"}

      operative.kind != :stream ->
        {:error,
         "partitions apply to stream operatives only; #{operative.id} is a " <>
           "#{operative.kind} operative"}

      byte_size(Partition.member_id(operative.id, partitions - 1)) > Validator.id_byte_limit() ->
        {:error, id_budget_refusal(operative.id, partitions)}

      ClassRegistry.keyless?(operative.factory_id) ->
        {:error,
         "PartitionsWithoutKeyedProcedure: factory #{operative.factory_id} has no keyed " <>
           "entry-fed procedure, so its partitions would each fold a slice of the input " <>
           "into a private copy of the same state — declare `key_by` on the entry edge " <>
           "before partitioning"}

      true ->
        check_group_quota(namespace, partitions - already_held)
    end
  end

  defp id_budget_refusal(base_id, partitions) do
    suffix = byte_size(Partition.member_suffix(partitions - 1))

    "operative id #{base_id} is #{byte_size(base_id)} bytes; #{partitions} partitions append " <>
      "#{suffix} more and the limit is #{Validator.id_byte_limit()} — " <>
      "the base id may be at most #{Validator.id_byte_limit() - suffix} bytes for this N"
  end

  # One preflight for all N units. Allocation itself stays per-member (there is
  # no atomic N-reservation seam), so a concurrent starter can still win a slot
  # between here and member k — which the rollback below handles loudly.
  defp check_group_quota(_namespace, needed) when needed <= 0, do: :ok

  defp check_group_quota(namespace, needed) do
    case Quotas.check_allocation(namespace, :max_operatives, needed) do
      :ok -> :ok
      {:error, :namespace_unknown} -> :ok
      {:error, _} = err -> err
    end
  end

  defp start_members(namespace, operative, ranges, plugins_fun) do
    count = length(ranges)

    ranges
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {range, index}, {:ok, started} ->
      case start_member(namespace, operative, index, count, range, plugins_fun) do
        {:ok, member_id} ->
          {:cont, {:ok, [member_id | started]}}

        {:error, reason} ->
          {:halt, rollback_group(namespace, started, operative.id, index, reason)}
      end
    end)
    |> case do
      {:ok, started} ->
        {:ok,
         %{
           group: operative.id,
           members: Enum.reverse(started),
           warnings: runner_count_warnings(namespace, count)
         }}

      err ->
        err
    end
  end

  defp start_member(namespace, operative, index, count, range, plugins_fun) do
    member_id = Partition.member_id(operative.id, index)
    partition_opts = Partition.member_opts(operative.id, index, count, range)

    with {:ok, plugins} <- plugins_fun.(member_id, partition_opts) do
      member = %{
        operative
        | id: member_id,
          opts: member_opts(operative.opts, partition_opts, plugins)
      }

      case OperativeManager.start_operative(namespace, member) do
        {:ok, _} -> {:ok, member_id}
        :duplicate -> {:error, "#{member_id}: #{Validator.id_in_use()}"}
        {:error, reason} -> {:error, "#{member_id}: #{inspect(reason)}"}
      end
    end
  end

  defp member_opts(base_opts, partition_opts, plugins) do
    base_opts
    |> Keyword.merge(partition_opts)
    |> then(fn opts -> if plugins == [], do: opts, else: Keyword.put(opts, :plugins, plugins) end)
  end

  # A group is all-or-nothing at start: a member that failed leaves the others
  # running as an operative that half-serves its key space, which is the one
  # shape partitioning must never produce silently.
  defp rollback_group(namespace, started, group_id, index, reason) do
    Enum.each(started, &OperativeManager.stop_operative(namespace, &1))

    {:error,
     "partition #{index} of group #{group_id} failed to start (#{reason}); " <>
       "#{length(started)} already-started member(s) were stopped"}
  end

  defp runner_count_warnings(namespace, partitions) do
    case length(FleetManager.active_runners(namespace)) do
      runners when runners > 0 and runners < partitions ->
        [
          "#{partitions} partitions but only #{runners} active runner(s) in namespace " <>
            "#{namespace}: members will share machines (about " <>
            "#{Float.round(partitions / runners, 1)} per runner). More partitions than " <>
            "runners buys no parallelism — start the fleet first, or use fewer partitions."
        ]

      _ ->
        []
    end
  end

  @doc """
  Rescale the partition group `group_id` to `target` members.

  The order is quiesce → preflight → stop → move → start, and it is what makes
  the operation safe rather than merely sequential: nothing durable moves until
  every member has consumed its input log and its DLQ is empty, and no member
  runs again until every owed move has landed. A crash between those two is
  recoverable by re-running the command — each move is atomic per Entity, so a
  re-run re-derives what is left from what is on disk.

  Options: `:quiesce_timeout_ms` (default 30_000) bounds the catch-up wait.
  """
  @spec rescale(String.t(), String.t(), pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def rescale(namespace, group_id, target, opts \\ []) do
    with {:ok, members} <- rescale_members(namespace, group_id),
         {:ok, template, plugins} <- rescale_template(namespace, group_id, members),
         :ok <- check_partitionable(namespace, template, target, length(members)),
         :ok <- quiesce_group(namespace, members, opts),
         moves = plan_moves(members, Partition.ranges(target), group_id),
         :ok <- stop_members(namespace, members),
         :ok <- apply_moves(moves),
         {:ok, started} <-
           start_members(namespace, template, Partition.ranges(target), plugins) do
      vacated = cleanup_vacated(members, started.members)

      {:ok,
       Map.merge(started, %{
         from: length(members),
         to: target,
         entities_moved: length(moves),
         vacated: vacated
       })}
    end
  end

  defp rescale_members(namespace, group_id) do
    case members(namespace, group_id) do
      [] -> {:error, "no partition group named #{group_id} in namespace #{namespace}"}
      members -> {:ok, members}
    end
  end

  # The base builder every new member is minted from, read off a live member
  # rather than re-supplied: a rescale must not silently change the factory,
  # the init values, or the plugin set the group was started with.
  defp rescale_template(namespace, group_id, [%{id: member_id} | _]) do
    case OperativeManager.get_overwatch_state(namespace, member_id) do
      {:ok, %{builder: builder}} ->
        template = %{
          builder
          | id: group_id,
            opts:
              builder.opts
              |> Keyword.drop([:plugins | Partition.opt_keys()])
              # A rescale assembles the whole group's Entities on the node
              # running it; a member ProcessHub then places elsewhere pulls
              # its class tree from here before restoring.
              |> Keyword.put(:state_source_node, node())
        }

        {:ok, template, restamp_plugins(Keyword.get(builder.opts, :plugins, []))}

      {:error, reason} ->
        {:error, "cannot read #{member_id} to rescale from: #{inspect(reason)}"}
    end
  end

  # Re-point one member's plugin set at another member. Same rule the group
  # start's JSON fan-out applies, from decoded structs instead.
  defp restamp_plugins(plugins) do
    fn member_id, partition_opts ->
      identity = Partition.identity(partition_opts)
      {:ok, Enum.map(plugins, &Plugin.for_partition(&1, member_id, identity))}
    end
  end

  @peer_call_timeout_ms 30_000
  @quiesce_timeout_ms 30_000
  @quiesce_poll_ms 250

  # Sources off, every input log fully consumed, no parked entries anywhere.
  # Until all three hold, moving an Entity's files would move it away from
  # input it has not read yet.
  defp quiesce_group(namespace, members, opts) do
    Enum.each(members, &PluginManager.disable_operative_plugins(namespace, &1.id))

    with :ok <- refuse_on_parked_entries(members) do
      deadline =
        System.monotonic_time(:millisecond) +
          Keyword.get(opts, :quiesce_timeout_ms, @quiesce_timeout_ms)

      await_consumed(members, deadline)
    end
  end

  defp refuse_on_parked_entries(members) do
    members
    |> Enum.map(&{&1.id, length(Dlq.Store.list(&1.id))})
    |> Enum.reject(fn {_id, count} -> count == 0 end)
    |> case do
      [] ->
        :ok

      parked ->
        {:error,
         "rescale refused: parked DLQ entries must be replayed or dropped first — " <>
           Enum.map_join(parked, ", ", fn {id, count} -> "#{id}: #{count} entries" end)}
    end
  end

  defp await_consumed(members, deadline) do
    case Enum.find(members, &(not consumed?(&1.id))) do
      nil ->
        :ok

      laggard ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, quiesce_timeout_message(laggard.id)}
        else
          Process.sleep(@quiesce_poll_ms)
          await_consumed(members, deadline)
        end
    end
  end

  defp consumed?(member_id) do
    last = InputLog.last_offset(member_id)
    last < 0 or InputLog.watermark(member_id) == last
  end

  defp quiesce_timeout_message(member_id) do
    watermark = InputLog.watermark(member_id)
    last = InputLog.last_offset(member_id)

    "rescale aborted: #{member_id} has not consumed its input log " <>
      "(watermark #{inspect(watermark)}, last offset #{last}); nothing was stopped or moved. " <>
      "A checkpoint-ack class only advances its watermark at a checkpoint close, so the wait " <>
      "rides its checkpoint cadence — retry with a longer --timeout, or drive a checkpoint."
  end

  # Every Entity whose key group changes owner under the new ranges, as
  # `{source_node, source_entity_id, dest_entity_id}`. Derived from what is on
  # disk, which is what makes a re-run after a crash complete the remainder
  # rather than move anything twice.
  defp plan_moves(members, new_ranges, group_id) do
    reserved = StateRegistry.reserved_entity_keys()

    for member <- members,
        {_class, procedure, key} = entity <- entities_of_member(member),
        key not in reserved,
        new_index = Partition.owner(new_ranges, Partition.group_of(key)),
        new_index != nil,
        new_class = Partition.member_id(group_id, new_index),
        # A move is owed when the owner changes — and also when the Entity
        # merely sits on another controller node, because the rescale gathers
        # the whole group here before the new members start.
        new_class != member.id or member_node(member) not in [nil, node()],
        do: {member_node(member), entity, {new_class, procedure, key}}
  end

  defp entities_of_member(%{id: member_id} = member) do
    case member_node(member) do
      nil ->
        StateRegistry.entities_of(member_id)

      source when source == node() ->
        StateRegistry.entities_of(member_id)

      source ->
        peer_entities(source, member_id)
    end
  end

  # A peer that cannot be reached would silently contribute no moves, leaving
  # its Entities behind under a member that is about to stop. Say so.
  defp peer_entities(source_node, member_id) do
    case :erpc.call(source_node, WalTransfer, :entities, [member_id], @peer_call_timeout_ms) do
      entities when is_list(entities) -> entities
      other -> log_unreachable_peer(source_node, member_id, other)
    end
  catch
    kind, reason -> log_unreachable_peer(source_node, member_id, {kind, reason})
  end

  defp log_unreachable_peer(source_node, member_id, reason) do
    Logger.error(
      "[PartitionGroup] #{member_id} lives on #{inspect(source_node)}, which did not answer " <>
        "(#{inspect(reason)}): its Entities cannot be moved by this rescale"
    )

    []
  end

  defp member_node(member), do: Map.get(member, :node)

  defp stop_members(namespace, members) do
    Enum.each(members, &OperativeManager.stop_operative(namespace, &1.id, preserve_state: true))
    :ok
  end

  # No member runs again until every owed move has landed. A move that fails
  # aborts here with the members still stopped — which is the same state a
  # crash mid-rescale leaves, and the same remedy: re-run `scale`, which
  # re-derives what is left from what is on disk.
  defp apply_moves(moves) do
    moves
    |> Enum.reduce_while(:ok, fn {source_node, source, destination}, :ok ->
      case WalTransfer.move_entity(source_node, source, destination) do
        {:ok, _files} ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt,
           {:error,
            "rescale aborted: moving #{inspect(source)} to #{inspect(destination)} failed " <>
              "(#{inspect(reason)}); the group is stopped and no member was started — " <>
              "re-run `flz operative scale` to complete the remaining moves"}}
      end
    end)
  end

  # An old member past the new count keeps only a fully-consumed input log once
  # its Entities have moved out. Removing it is what stops a stale class tree
  # from reading as durable evidence of an operative that no longer exists; its
  # registry row expires through the stopped-row TTL on its own.
  defp cleanup_vacated(old_members, new_member_ids) do
    old_members
    |> Enum.map(& &1.id)
    |> Enum.reject(&(&1 in new_member_ids))
    |> Enum.filter(&vacated?/1)
    |> Enum.map(fn class ->
      _ = File.rm_rf(Path.join(StateRegistry.state_root(), class))
      class
    end)
  end

  defp vacated?(class) do
    StateRegistry.entities_of(class) == [] and consumed?(class)
  end
end
