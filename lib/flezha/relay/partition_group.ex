defmodule Flezha.Relay.PartitionGroup do
  @moduledoc """
  The lifecycle of a partition group: starting one, reading its members, and
  rescaling it.

  A partition is an *ordinary* relay instance, so every member start and
  stop goes through `Flezha.Service.RelayManager` unchanged — this module
  owns only what is true of the group: the preflights that must all pass before
  the first member starts, the all-or-nothing rollback, and the
  quiesce → move → restart sequence a rescale is.

  There is no stored group record. Membership is read back from the members'
  own cspec opts (`Flezha.Partition.identity/1`), so a missing member is simply
  absent here and the CLI's coverage check is what makes that visible.
  """

  require Logger

  alias Flezha.AuthZ.Quotas
  alias Flezha.Config.Durations
  alias Flezha.Dlq
  alias Flezha.Identificator
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Partition
  alias Flezha.Plugin
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.SDK.KeyGroup
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Validator
  alias Flezha.Shutdown
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias Flezha.State.WalTransfer

  @doc """
  Start `relay` as a partition group of `partitions` members.

  The group is N ordinary starts through `start_relay/3` — `<id>#p0` …
  `<id>#p{N-1}`, each carrying its own key-group range in its builder opts and
  therefore in its durable cspec. There is no group record: every group-level
  view is recomputed from the member rows.

  Everything that can refuse runs before the first member starts, because a
  half-started group is worse than a refused one. On a mid-group failure the
  already-started members are stopped and the refusal is the lowest failing
  member's own.

  A start over a group that already has live members CONVERGES instead of
  refusing: only the missing members are started, and only when `partitions`
  matches the identity the survivors declare — converge never changes N,
  because changing N moves Cell state and a start never does (that is
  `rescale/4`).

  `plugins_json` is decoded once per member with that member's id and partition
  opts (`SpecDecoder.decode/4`), so each member's source knows
  which share of the stream is its own.

  A builder without an id is given a generated base, trimmed so its largest
  member id fits, and always starts a fresh group: a base that names a live
  group or singleton is drawn again rather than converged or refused.

  Returns `{:ok, %{group:, members:}}`. A converge additionally carries
  `already_running:` — the member ids it left untouched.
  """
  @spec start(String.t(), Flezha.Relay.Builder.t(), pos_integer(), binary()) ::
          {:ok, map()} | {:error, term()}
  def start(namespace, relay, partitions, plugins_json \\ "[]")

  def start(namespace, %{id: nil} = relay, partitions, plugins_json) do
    reserve = byte_size(Partition.member_suffix(max(partitions - 1, 0)))

    start_fresh = fn base ->
      if members(namespace, base) == [] and Validator.relay_unique(namespace, base) == :ok,
        do: start(namespace, %{relay | id: base}, partitions, plugins_json),
        else: {:error, :id_taken}
    end

    RelayManager.with_generated_id(relay.factory_id, start_fresh, reserve: reserve)
  end

  def start(namespace, relay, partitions, plugins_json) do
    plugins = fn member_id, partition_opts ->
      identity = Partition.identity(partition_opts)

      with {:ok, plugins} <- SpecDecoder.decode(plugins_json, namespace, member_id),
           {:ok, bindings} <- PluginManager.start_bindings(namespace, plugins),
           do: {:ok, Enum.map(bindings, &Plugin.for_partition(&1, member_id, identity))}
    end

    case members(namespace, relay.id) do
      [] ->
        with :ok <- check_partitionable(namespace, relay, partitions, 0) do
          start_members(namespace, relay, Partition.ranges(partitions), plugins)
        end

      members ->
        converge(namespace, relay, partitions, members, plugins)
    end
  end

  @doc """
  The member ids of `group_id` currently registered in `namespace`, in
  partition order, each with its partition identity. The one way "who is in
  this group" is answered — there is no stored group record, only the identity
  each member's registry row carries, so a member that is missing is simply
  absent here and the coverage check is what makes that visible.
  """
  @spec members(String.t(), String.t()) :: [%{id: String.t(), partition: map()}]
  def members(namespace, group_id), do: HubManager.partition_members(namespace, group_id)

  defp check_partitionable(namespace, relay, partitions, already_held) do
    known = ClassRegistry.partitioning(relay.factory_id)

    cond do
      partitions < 2 or partitions > KeyGroup.key_groups() ->
        {:error, {:partitions_out_of_range, partitions, KeyGroup.key_groups()}}

      # Copied into every member, the message would park at all but one.
      Keyword.get(relay.opts, :seed_input) != nil ->
        {:error, {:partitioned_first_message, Partition.member_id(relay.id, 0)}}

      # The base id is never claimed by a group — only member ids are — so a
      # live claim on it means an unpartitioned relay already runs there.
      Validator.relay_unique(namespace, relay.id) != :ok ->
        {:error, {:unpartitioned_running, relay.id}}

      byte_size(Partition.member_id(relay.id, partitions - 1)) > id_limit() ->
        suffix = byte_size(Partition.member_suffix(partitions - 1))
        {:error, {:group_id_too_long, relay.id, partitions, id_limit() - suffix}}

      known.each in [:once, :chained] ->
        {:error, {:partitions_without_per_input_loop, relay.factory_id, known.each, partitions}}

      known.keyless ->
        {:error, {:partitions_without_keyed_tasklet, relay.factory_id}}

      true ->
        check_group_quota(namespace, partitions - already_held)
    end
  end

  # One preflight for all N units. Allocation itself stays per-member (there is
  # no atomic N-reservation seam), so a concurrent starter can still win a slot
  # between here and member k — which the rollback below handles loudly.
  defp check_group_quota(_namespace, needed) when needed <= 0, do: :ok

  defp check_group_quota(namespace, needed) do
    case Quotas.check_allocation(namespace, :max_relays, needed) do
      :ok -> :ok
      {:error, :namespace_unknown} -> :ok
      {:error, _} = err -> err
    end
  end

  # Only the missing members start, after two preflights: the survivors must
  # declare exactly the identity the request recomputes, and the group-level
  # checks price quota on the missing count alone (the rescale's own seam).
  defp converge(namespace, relay, partitions, members, plugins_fun) do
    present = MapSet.new(members, & &1.partition.index)

    missing =
      Partition.ranges(partitions)
      |> Enum.with_index()
      |> Enum.reject(fn {_range, index} -> index in present end)

    with :ok <- check_converge_identity(members, partitions, relay.id),
         :ok <- check_partitionable(namespace, relay, partitions, length(members)),
         {:ok, started, skipped} <-
           run_member_starts(namespace, relay, missing, partitions, plugins_fun,
             on_duplicate: :skip
           ) do
      {:ok,
       %{
         group: relay.id,
         members: started,
         already_running: in_partition_order(skipped ++ Enum.map(members, & &1.id))
       }}
    end
  end

  defp in_partition_order(member_ids),
    do: Enum.sort_by(member_ids, &Partition.split_member_suffix/1)

  defp check_converge_identity(members, partitions, group_id) do
    ranges = Partition.ranges(partitions)

    members
    |> Enum.find(fn %{partition: p} ->
      p.count != partitions or p.range != Enum.at(ranges, p.index)
    end)
    |> case do
      nil ->
        :ok

      %{id: id, partition: p} ->
        {:error, {:group_partition_mismatch, group_id, partitions, id, p.count}}
    end
  end

  # A rescale's members are the group it moved, not relays started or
  # stopped: it starts them, as it stops them, with `announce: false`.
  defp start_members(namespace, relay, ranges, plugins_fun, start_opts \\ []) do
    with {:ok, started, _skipped} <-
           run_member_starts(
             namespace,
             relay,
             Enum.with_index(ranges),
             length(ranges),
             plugins_fun,
             [on_duplicate: :refuse] ++ start_opts
           ) do
      {:ok, %{group: relay.id, members: started}}
    end
  end

  # The one member-start loop. Every member starts at once — a group comes up
  # in one member's time, not N — and the lowest failing index names the error.
  # `on_duplicate: :refuse` is the fresh-group policy — a taken id is a failure
  # that rolls back this run's starts; `:skip` is converge's — a member racing
  # into existence is by id-shape a member of this same group. `announce:`
  # reaches each member's start.
  defp run_member_starts(namespace, relay, indexed_ranges, count, plugins_fun, policy) do
    on_duplicate = Keyword.fetch!(policy, :on_duplicate)
    start_opts = Keyword.take(policy, [:announce])

    outcomes =
      Flezha.Parallel.map(indexed_ranges, fn {range, index} ->
        member = start_member(namespace, relay, {index, count, range}, plugins_fun, start_opts)
        {index, member}
      end)

    started = for {_index, {:ok, member_id}} <- outcomes, do: member_id

    skipped =
      for {_index, {:duplicate, member_id}} <- outcomes, on_duplicate == :skip, do: member_id

    outcomes
    |> Enum.find(fn {_index, outcome} -> member_failure(outcome, on_duplicate) end)
    |> case do
      nil ->
        {:ok, started, skipped}

      {_index, outcome} ->
        rollback_group(namespace, started, member_failure(outcome, on_duplicate))
    end
  end

  defp member_failure({:error, reason}, _on_duplicate), do: reason
  defp member_failure({:duplicate, member_id}, :refuse), do: {:id_in_use, member_id}

  defp member_failure(_outcome, _on_duplicate), do: nil

  defp start_member(namespace, relay, {index, count, range}, plugins_fun, start_opts) do
    member_id = Partition.member_id(relay.id, index)
    partition_opts = Partition.member_opts(relay.id, index, count, range)

    with {:ok, plugins} <- plugins_fun.(member_id, partition_opts) do
      member = %{
        relay
        | id: member_id,
          opts: member_opts(relay.opts, partition_opts, plugins)
      }

      case RelayManager.start_relay(namespace, member, start_opts) do
        {:ok, _} -> {:ok, member_id}
        :duplicate -> {:duplicate, member_id}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp member_opts(base_opts, partition_opts, plugins) do
    base_opts
    |> Keyword.merge(partition_opts)
    |> then(fn opts ->
      if plugins == [], do: opts, else: Keyword.put(opts, :bindings, plugins)
    end)
  end

  # A group is all-or-nothing at start: a member that failed leaves the others
  # running as a relay that half-serves its key space, which is the one
  # shape partitioning must never produce silently. The refusal is the lowest
  # failing member's own, which says what to fix.
  defp rollback_group(namespace, started, reason) do
    Enum.each(started, &RelayManager.stop_relay(namespace, &1))
    {:error, reason}
  end

  @doc """
  Rescale the partition group `group_id` to `target` members.

  The order is quiesce → preflight → stop → move → seal → start, and it is
  what makes the operation safe rather than merely sequential: nothing durable
  moves until every member has consumed its input log and its DLQ is empty, and
  no member runs again until every owed move has landed. A crash between those two is
  recoverable by re-running the command — each move is atomic per Cell, so a
  re-run re-derives what is left from what is on disk.

  Exactly one rescale of a group runs at a time, cluster-wide. Two interleaved
  ones would stop the members the other just started — each reads the member set
  the other is rewriting — so the second is refused outright rather than
  serialized behind the first, whose outcome it was not asked for.

  Options: `:quiesce_timeout_ms` (default 30_000) bounds the catch-up wait.
  """
  @spec rescale(String.t(), String.t(), pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def rescale(namespace, group_id, target, opts \\ []) do
    # `:global` releases the lock when the holder dies, so a controller that
    # crashes mid-rescale does not leave the group permanently un-rescalable —
    # the re-run the crash recovery calls for is the next command.
    lock = {{:flezha_partition_rescale, namespace, group_id}, self()}

    case :global.trans(
           lock,
           fn -> do_rescale(namespace, group_id, target, opts) end,
           [node() | Node.list()],
           0
         ) do
      :aborted ->
        {:error, {:rescale_running, namespace, group_id}}

      result ->
        result
    end
  end

  defp do_rescale(namespace, group_id, target, opts) do
    with {:ok, members} <- rescale_members(namespace, group_id),
         {:ok, template, plugins} <- rescale_template(namespace, group_id, members),
         :ok <- check_partitionable(namespace, template, target, length(members)),
         :ok <- quiesce_group(namespace, members, template.run, opts),
         cell_moves = plan_moves(members, Partition.ranges(target), group_id),
         {survivors, leaving} = Enum.split_with(members, &(&1.partition.index < target)),
         sealed = sealed(template.run, survivors),
         :ok <- stop_members(namespace, members, sealed),
         :ok <- apply_moves(cell_moves ++ instance_moves(members)),
         {:ok, started} <-
           start_members(namespace, template, Partition.ranges(target), plugins, announce: false) do
      vacated = cleanup_vacated(leaving)

      {:ok,
       Map.merge(started, %{
         from: length(members),
         to: target,
         cells_moved: length(cell_moves),
         vacated: vacated,
         sealed: sealed
       })}
    end
  end

  defp rescale_members(namespace, group_id) do
    case members(namespace, group_id) do
      [] -> {:error, {:group_not_found, namespace, group_id}}
      members -> {:ok, members}
    end
  end

  # The base builder every new member is minted from, read off a live member
  # rather than re-supplied: a rescale must not silently change the factory,
  # the init values, or the plugin set the group was started with.
  defp rescale_template(namespace, group_id, [%{id: member_id} | _]) do
    case RelayManager.get_overwatch_state(namespace, member_id) do
      {:ok, %{builder: builder}} ->
        template = %{
          builder
          | id: group_id,
            opts:
              builder.opts
              |> Keyword.drop([:bindings, :seed_input | Partition.opt_keys()])
              # A rescale assembles the whole group's Cells on the node
              # running it; a member ProcessHub then places elsewhere pulls
              # its class tree from here before restoring.
              |> Keyword.put(:state_source_node, node())
        }

        {:ok, template, restamp_plugins(member_bindings(namespace, member_id, builder))}

      {:error, reason} ->
        {:error, {:member_unreadable, member_id, reason}}
    end
  end

  # The bindings to mint the new members from: the member's declared entry,
  # which a bind after the group start rewrites, so a group bound afterwards
  # (`flz plugin bind <name> --relay_id <group>`) does not rescale into
  # members with no plugins at all. The builder is the fallback for a member
  # whose row cannot be read.
  defp member_bindings(namespace, member_id, builder) do
    case HubManager.relay_bindings(namespace, member_id) do
      {:ok, bindings} -> Enum.map(bindings, &elem(&1, 1))
      {:error, _unreadable} -> Keyword.get(builder.opts, :bindings, [])
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

  # Sources off, every input log fully consumed, no parked entries anywhere.
  # Until all three hold, moving a Cell's files would move it away from
  # input it has not read yet.
  defp quiesce_group(namespace, members, run, opts) do
    _ = PluginManager.unbind_all(namespace, Enum.map(members, & &1.id))

    with :ok <- refuse_on_parked_entries(members) do
      deadline =
        System.monotonic_time(:millisecond) +
          Keyword.get(opts, :quiesce_timeout_ms, Durations.get(:partition_quiesce_timeout_ms))

      await_consumed(members, run, deadline)
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
        {:error, {:parked_entries, Enum.map(parked, fn {id, count} -> "#{id} (#{count})" end)}}
    end
  end

  defp await_consumed(members, run, deadline) do
    case Enum.find(members, &(not consumed?(&1.id))) do
      nil ->
        :ok

      laggard ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, {:quiesce_timeout, laggard.id, run, input_log_progress(laggard.id)}}
        else
          Process.sleep(Durations.get(:partition_quiesce_poll_ms))
          await_consumed(members, run, deadline)
        end
    end
  end

  # A log that refused a record answers no offset, so its class has not
  # consumed its inputs and the quiesce times out saying so. A steps relay
  # member's floor is its journal's, so a pass in flight holds it below the
  # log's end until the pass commits.
  defp consumed?(member_id) do
    case InputLog.tail(member_id) do
      {:ok, floor, last} -> last <= floor
      {:error, _refusal} -> false
    end
  end

  defp input_log_progress(member_id) do
    case InputLog.tail(member_id) do
      {:ok, floor, last} -> %{watermark: floor, last_offset: last}
      {:error, _refusal} -> %{}
    end
  end

  # Every Cell whose key group changes owner under the new ranges, as
  # `{source_node, source_ref, dest_ref}`. Derived from what is on disk, which
  # is what makes a re-run after a crash complete the remainder rather than
  # move anything twice.
  defp plan_moves(members, new_ranges, group_id) do
    reserved = Paths.reserved_cell_keys()

    for member <- members,
        {_class, tasklet, key} = cell <- cells_of_member(member),
        key not in reserved,
        new_index = Partition.owner(new_ranges, KeyGroup.group_of(key)),
        new_index != nil,
        new_class = Partition.member_id(group_id, new_index),
        # A move is owed when the owner changes — and also when the Cell
        # merely sits on another controller node, because the rescale gathers
        # the whole group here before the new members start.
        new_class != member.id or remote?(member),
        do: {member_node(member), {:cell, cell}, {:cell, {new_class, tasklet, key}}}
  end

  # A steps relay member's journal, input log and effect log, gathered here under its own id
  # when it sits on another node. They move together: the journal's floor
  # counts offsets in that one log. A vacated member's then go with its tree.
  defp instance_moves(members) do
    for member <- members,
        remote?(member),
        ref <- [
          {:cell, Paths.journal_id(member.id)},
          {:input_log, member.id},
          {:effects_log, member.id}
        ],
        do: {member_node(member), ref, ref}
  end

  defp cells_of_member(%{id: member_id} = member) do
    if remote?(member),
      do: peer_cells(member_node(member), member_id),
      else: Paths.cells_of(member_id)
  end

  # A peer that cannot be reached would silently contribute no moves, leaving
  # its Cells behind under a member that is about to stop. Say so.
  defp peer_cells(source_node, member_id) do
    case :erpc.call(
           source_node,
           Paths,
           :durable_refs,
           [member_id],
           Durations.get(:partition_peer_call_ms)
         ) do
      refs when is_list(refs) -> for {:cell, cell} <- refs, do: cell
      other -> log_unreachable_peer(source_node, member_id, other)
    end
  catch
    kind, reason -> log_unreachable_peer(source_node, member_id, {kind, reason})
  end

  defp log_unreachable_peer(source_node, member_id, reason) do
    Logger.error(
      "[PartitionGroup] #{member_id} lives on #{inspect(source_node)}, which did not answer " <>
        "(#{inspect(reason)}): its Cells cannot be moved by this rescale"
    )

    []
  end

  defp member_node(member), do: Map.get(member, :node)

  defp remote?(member), do: member_node(member) not in [nil, node()]

  # A member's `:stop_relay` call replies before ProcessHub has finished
  # unregistering the child, so a rescale that reuses an id — every rescale does,
  # `#p0` survives any N — can lose the claim to the corpse of the member it just
  # stopped. Wait for the very predicate the restart will check. Nothing durable
  # has moved yet, so giving up here costs only the stop.
  defp stop_members(namespace, members, sealed) do
    Enum.each(members, fn %{id: id} ->
      keep = if id in sealed, do: :sealed, else: true
      RelayManager.stop_relay(namespace, id, preserve_state: keep, announce: false)
    end)

    await_ids_released(
      namespace,
      Enum.map(members, & &1.id),
      System.monotonic_time(:millisecond) + Shutdown.stop_timeout_ms()
    )
  end

  defp await_ids_released(namespace, ids, deadline) do
    case Enum.find(ids, &(Validator.relay_unique(namespace, &1) != :ok)) do
      nil ->
        :ok

      held ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, {:member_id_held, held, Shutdown.stop_timeout_ms()}}
        else
          Process.sleep(Durations.get(:partition_quiesce_poll_ms))
          await_ids_released(namespace, ids, deadline)
        end
    end
  end

  # No member runs again until every owed move has landed. A move that fails
  # aborts here with the members still stopped — which is the same state a
  # crash mid-rescale leaves, and the same remedy: re-run `scale`, which
  # re-derives what is left from what is on disk.
  defp apply_moves(moves) do
    moves
    |> Enum.reduce_while(:ok, fn {source_node, source, destination}, :ok ->
      case WalTransfer.move_cell(source_node, source, destination) do
        {:ok, _files} ->
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, {:cell_move_failed, source, destination, reason}}}
      end
    end)
  end

  # Every surviving member of a steps relay group resumes on a journal cut to its
  # latest committed iteration, which its stop seals on its own node before the
  # journal moves. The run is the template's: a member on another node
  # recorded its own there.
  defp sealed(:steps, survivors), do: Enum.map(survivors, & &1.id)
  defp sealed(_stream, _survivors), do: []

  # An old member past the new count keeps only a fully-consumed input log,
  # and for a steps relay its journal, once its Cells have moved out. Removing it
  # is what stops a stale class tree from reading as durable evidence of an
  # relay the fleet does not hold; its stop already removed it from the
  # hub's declared set and deleted its registry row, so a returning node can
  # never resurrect it.
  defp cleanup_vacated(leaving) do
    leaving
    |> Enum.map(& &1.id)
    |> Enum.filter(&vacated?/1)
    |> Enum.map(fn class ->
      :ok = RelayManager.delete_state_dir(class)
      class
    end)
  end

  # The ceiling a member id's `#pK` suffix is budgeted against.
  defp id_limit(), do: Identificator.limit(:relay)

  defp vacated?(class) do
    Paths.cells_of(class) == [] and consumed?(class)
  end
end
