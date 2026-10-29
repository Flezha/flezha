defmodule Flezha.Service.Relay.Info do
  @moduledoc """
  What `Flezha.Service.Relay.info/2` answers for a namespace: every live
  relay as its Overwatch reports it, or by its published phase and health,
  marked `live_read`, when that Overwatch does not answer; every finished one
  on any controller whose completion record is still inside its retention
  window, with `expires_at` (unix seconds) the end of that window and the
  `node` holding it; and every partition group with its read-time key-group
  coverage.

  `rows/2` is the namespace's relay rows as every surface reads them: the
  registry's, read on a hub member, each with the health the controller
  hosting it judges. `list/2` is the relay list made of them, whole or one
  page with the namespace's summary.
  """

  alias Flezha.Cluster.Page
  alias Flezha.Cluster.Reads
  alias Flezha.Convergence.Snapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Relay.Calls.Inbound
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Overwatch
  alias Flezha.Partition
  alias Flezha.Plugin.Extension.Table.Health, as: TableHealth
  alias Flezha.Relay.KeyGroup
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.Steps
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal

  @sorts [id: :asc, severity: :asc]
  @filters [:health, :factory, :runner, :node, :group, :ids, :text]

  @doc """
  What `Flezha.Service.Relay.list/3` answers: every relay row of `namespace`,
  a partition group as one row exactly as healthy as its sickest member,
  sorted by id; or with `opts` one page of them (`Flezha.Cluster.Page`) with
  the namespace's `summary`.

  The page filters by `health` (health words, and `unhealthy` for any word
  ranked `degraded` or worse by `Flezha.Convergence.Verdict.rank/1`),
  `factory`, `runner`, `node`, `group` (a partition group's id), `ids` (at most
  500 relay or group ids) and `text` (a case-insensitive substring of the id or
  the factory), and sorts by `id` or by `severity` (worst first, then id). Its
  rows also carry their run kind (`run`) and bound-plugin count (`plugins`).

  The summary is computed over every row whatever the filter, in the pass that
  builds the page: the rows by health; `faulted_groups`, each partition group
  whose sickest member is `unhealthy` or whose key-group coverage is not
  complete; and `waiting`, each factory with relays waiting to be bound to a
  runner (`unbound` or `recovering`), with how many. A filter of `ids` alone
  reads those relays' registry entries directly instead of walking the
  namespace, and answers `summary: nil`.
  """
  @spec list(String.t(), keyword() | map()) :: {:ok, map()} | {:error, Flezha.Error.t()}
  def list(namespace, opts) do
    with {:ok, page} <- Page.parse(opts, :relay_list, sorts: @sorts, filters: @filters),
         {:ok, filter} <- filter(page && page.filter) do
      {:ok, if(page, do: page(namespace, page, filter), else: listed(namespace))}
    end
  end

  defp listed(namespace) do
    %{rows: rows} = answer = rows(namespace)
    %{answer | rows: rows |> collapse() |> Enum.sort_by(&id_key/1)}
  end

  defp page(namespace, page, filter) do
    {call, summarise?} =
      case Map.keys(filter) do
        [:ids] -> {{HubManager, :relay_rows_of, [namespace, filter.ids]}, false}
        _walk -> {{HubManager, :relay_rows, [namespace, true]}, true}
      end

    %{rows: rows, unreachable: unreachable} = rows(namespace, call)
    listed = collapse(rows)

    listed
    |> Enum.filter(&kept?(&1, filter))
    |> Page.slice(page, sort_key(page.sort))
    |> Map.merge(%{unreachable: unreachable, summary: if(summarise?, do: summary(rows, listed))})
  end

  defp collapse(rows) do
    plain = Enum.reject(rows, &(&1[:partition] != nil))
    plain ++ Enum.map(Partition.groups(rows), &group_row/1)
  end

  defp group_row(report) do
    members = report.members

    %{
      id: report.group,
      health: worst(members),
      runner: Enum.map_join(members, ", ", &"p#{&1.partition.index}→#{&1.runner || "-"}"),
      factory: members |> hd() |> Map.get(:factory),
      partitions: "#{length(members)}/#{report.declared}",
      node: members |> hd() |> Map.get(:node),
      members: members
    }
    |> Map.merge(Map.take(hd(members), [:run, :plugins]))
  end

  defp worst(members), do: members |> Enum.map(& &1.health) |> Enum.max_by(&Verdict.rank/1)

  defp summary(rows, listed) do
    relays = Enum.flat_map(listed, &(&1[:members] || [&1]))

    %{
      health: Enum.frequencies_by(listed, & &1.health),
      faulted_groups: rows |> Partition.groups() |> Enum.flat_map(&faulted/1),
      waiting:
        relays
        |> Enum.filter(&Verdict.waiting?(&1.health))
        |> Enum.frequencies_by(& &1.factory)
        |> Enum.map(fn {factory, count} -> %{factory: factory, relays: count} end)
        |> Enum.sort_by(&{-&1.relays, &1.factory})
    }
  end

  defp faulted(report) do
    health = worst(report.members)

    if Verdict.fault?(health) or report.coverage != :ok,
      do: [%{group: report.group, health: health, complete: report.coverage == :ok}],
      else: []
  end

  defp id_key(row), do: inspect(row.id)

  defp sort_key(:id), do: &id_key/1
  defp sort_key(:severity), do: &{-Verdict.rank(&1.health), id_key(&1)}

  # ---------- filters ----------

  defp filter(nil), do: {:ok, nil}

  defp filter(written) do
    Enum.reduce_while(written, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case filter_value(key, value) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        refused -> {:halt, refused}
      end
    end)
  end

  defp filter_value(key, value) when key in [:health, :ids] do
    case words(value) do
      nil -> Page.invalid("filter.#{key}", value)
      words -> Page.bounded(words, "filter.#{key}")
    end
  end

  defp filter_value(:text, value) when is_binary(value), do: {:ok, String.downcase(value)}
  defp filter_value(key, value) when is_binary(value) and key != :text, do: {:ok, value}
  defp filter_value(key, value), do: Page.invalid("filter.#{key}", value)

  defp words(value) when is_binary(value), do: String.split(value, ",", trim: true)

  defp words(value) when is_list(value) do
    if Enum.all?(value, &(is_binary(&1) or is_atom(&1))), do: Enum.map(value, &to_string/1)
  end

  defp words(_value), do: nil

  defp kept?(row, filter), do: Enum.all?(filter, &matches?(row, &1))

  defp matches?(row, {:health, words}) do
    to_string(row.health) in words or ("unhealthy" in words and Verdict.fault?(row.health))
  end

  defp matches?(row, {:ids, ids}), do: to_string(row.id) in ids
  defp matches?(row, {:factory, factory}), do: row.factory == factory
  defp matches?(row, {:group, group}), do: row[:members] != nil and row.id == group
  defp matches?(row, {:runner, runner}), do: any_member?(row, &(&1.runner == runner))
  defp matches?(row, {:node, node}), do: any_member?(row, &(to_string(&1.node) == node))

  defp matches?(row, {:text, text}) do
    Enum.any?(
      [row.id, row.factory],
      &(is_binary(&1) and String.contains?(String.downcase(&1), text))
    )
  end

  defp any_member?(row, fun), do: Enum.any?(row[:members] || [row], fun)

  @doc "The report for `namespace`, with the controllers that did not answer."
  @spec report(String.t()) :: map()
  def report(namespace) do
    %{rows: rows, unreachable: unreachable} = rows(namespace)

    {live, live_unreachable} =
      hub_read(namespace, {RelayManager, :get_relay_ids, [namespace]})

    # A live relay the rows do not name yet reads this controller's verdict.
    health =
      Map.merge(Snapshot.relay_health_of(namespace, live), Map.new(rows, &{&1.id, &1.health}))

    %{rows: finished, unreachable: retained_unreachable} =
      Reads.rows(Reads.controllers(), __MODULE__, :local_retained, [namespace, live])

    %{
      namespace: namespace,
      key_groups: KeyGroup.key_groups(),
      groups: rows |> Partition.groups() |> Enum.map(&group/1),
      relays: Enum.map(live, &row(namespace, &1, Map.get(health, &1, :unknown))) ++ finished,
      unreachable: Enum.uniq(unreachable ++ live_unreachable ++ retained_unreachable)
    }
  end

  @doc """
  The relay rows of `namespace` (`HubManager.relay_rows/1`, or the registry
  read `call` names), read on a member of its hub, each with `health` as the
  controller hosting it judges it. A row no controller hosts reads this
  controller's verdict; a row whose host does not answer reads `:unknown`, and
  that host is named in `unreachable`.
  """
  @spec rows(String.t(), {module(), atom(), list()} | nil) :: Reads.rows()
  def rows(namespace, call \\ nil) do
    {rows, unreachable} = hub_read(namespace, call || {HubManager, :relay_rows, [namespace]})
    {placed, unplaced} = Enum.split_with(rows, & &1.node)
    ids = Enum.map(placed, & &1.id)

    %{answers: answers, unreachable: hosts_unreachable} =
      placed
      |> Enum.map(& &1.node)
      |> Reads.ask(Snapshot, :relay_health_of, [namespace, ids])

    health = Map.new(answers)
    here = Snapshot.relay_health_of(namespace, Enum.map(unplaced, & &1.id))

    %{
      rows:
        Enum.map(rows, fn row ->
          verdicts = if row.node, do: Map.get(health, row.node, %{}), else: here
          Map.put(row, :health, Map.get(verdicts, row.id, :unknown))
        end),
      unreachable: unreachable ++ hosts_unreachable
    }
  end

  @doc """
  A registry read of `namespace` answered on a member of its hub
  (`RelayManager.on_hub_member/4`): its list, and the member that did not
  answer.
  """
  @spec hub_read(String.t(), {module(), atom(), list()}) :: {list(), [node()]}
  def hub_read(namespace, call) do
    case RelayManager.on_hub_member(namespace, namespace, call) do
      {:error, {:routed_call_failed, member, _kind, _reason}} -> {[], [member]}
      {:ok, list} -> {list, []}
      list -> {list, []}
    end
  end

  @doc """
  One relay of `namespace`: `{:live, state}` with its Overwatch state,
  `{:finished, row}` as `report/1` lists a completion record still inside its
  retention window, `{:unread, code, verdict}` for a relay its hub holds whose
  Overwatch did not answer (`code` names the read's error) but whose verdict
  the convergence loop published,
  `relay_not_found` when it is none of these, or the error the read answered.
  """
  @spec relay(String.t(), String.t()) ::
          {:live, map()}
          | {:finished, map()}
          | {:unread, String.t(), Verdict.t()}
          | {:error, term()}
  def relay(namespace, relay_id) do
    case RelayManager.get_overwatch_state(namespace, relay_id) do
      {:ok, state} ->
        {:live, state}

      {:error, reason} when elem(reason, 0) in [:relay_not_found, :hub_down] ->
        case Steps.outcome(namespace, relay_id) do
          {:ok, record} -> {:finished, finished(relay_id, record)}
          :error -> {:error, {:relay_not_found, namespace, relay_id}}
        end

      # A bare reason is the bounded call's refusal of a pid the hub resolved.
      {:error, reason} = refused when reason in [:timeout, :noproc, :other] ->
        case Snapshot.verdict(namespace, relay_id) do
          %Verdict{} = verdict ->
            {:unread, Flezha.Error.from_reason(reason, :"relay.list").code, verdict}

          nil ->
            refused
        end

      {:error, _reason} = refused ->
        refused
    end
  end

  # The row of a relay the hub lists, read once, with its listed `health`.
  defp row(namespace, relay_id, health) do
    case relay(namespace, relay_id) do
      {:live, state} ->
        live(relay_id, state, health)

      {:finished, row} ->
        row

      {:unread, code, verdict} ->
        %{
          id: relay_id,
          phase: to_string(verdict.phase || :unknown),
          health: health,
          live_read: code
        }

      {:error, reason} ->
        error = Flezha.Error.from_reason(reason, :"relay.list")
        %{id: relay_id, error: Jason.decode!(Flezha.Error.object(error))}
    end
  end

  defp live(relay_id, state, health) do
    settings = Map.get(state, :settings) || %{}

    %{
      id: relay_id,
      runner_id: state.runner_id,
      namespace: state.namespace,
      run: run(state),
      each: Map.get(settings, :each),
      on_failure: Map.get(settings, :on_failure),
      effectively_once: Map.get(state, :effectively_once) == true,
      durable: durable_level(relay_id, state),
      sleep_after_ms: ClassRegistry.sleep_after_ms(class_of(relay_id, state)),
      phase: to_string(Overwatch.phase(state) || :unknown),
      health: health,
      uptime_ms: uptime_ms(state),
      default_input_type: state.default_input_type,
      waiting_callers: Inbound.waiting(state.calls),
      failed_turns: failed_turns(relay_id, state),
      plugins: plugins(state.extensions)
    }
  end

  @doc false
  # The local half of `report/1`'s finished relays: this node's kept relays
  # still inside their retention window, of classes `live_ids` does not name.
  # A class dir is named for its relay's id.
  @spec local_retained(String.t(), [String.t()]) :: [map()]
  def local_retained(namespace, live_ids) do
    for relay_id <- Paths.relay_classes(),
        relay_id not in live_ids,
        record = Steps.local_kept(namespace, relay_id),
        record != nil,
        do: finished(relay_id, record)
  end

  defp finished(relay_id, record) do
    %{
      id: relay_id,
      namespace: Map.get(record, :namespace),
      run: Map.get(record, :run),
      phase: "finished",
      status: Map.get(record, :status),
      error: nil_if_empty(Map.get(record, :error)),
      result: encode_result(Map.get(record, :result)),
      finished_at: Map.get(record, :finished_at),
      expires_at: Map.get(record, :expires_at),
      state_kept: Map.get(record, :state_kept, false)
    }
    |> Map.merge(Map.take(record, [:node]))
  end

  defp group(report) do
    %{
      group: report.group,
      partitions: report.declared,
      members:
        Enum.map(report.members, fn m ->
          %{
            id: m.id,
            partition: m.partition.index,
            key_groups: Tuple.to_list(m.partition.range),
            runner: m.runner,
            health: m.health
          }
        end),
      coverage: coverage(report.coverage)
    }
  end

  defp coverage(:ok), do: %{complete: true}

  defp coverage({:error, %{gaps: gaps, overlaps: overlaps}}) do
    %{
      complete: false,
      gaps: Enum.map(gaps, &Tuple.to_list/1),
      overlaps: Enum.map(overlaps, fn {group, ids} -> %{key_group: group, owners: ids} end)
    }
  end

  defp encode_result(r) when is_binary(r) and byte_size(r) > 0, do: Base.encode64(r)
  defp encode_result(_), do: nil

  defp nil_if_empty(s) when is_binary(s) and byte_size(s) > 0, do: s
  defp nil_if_empty(_), do: nil

  # The turns a steps session dead-lettered, from its journal; `nil` for any
  # other relay and for a session none of whose turns failed.
  defp failed_turns(relay_id, %{builder: %{run: :steps}}) do
    case TaskJournal.failed_turns(relay_id) do
      {count, last} -> %{count: count, last_iteration: last}
      _none -> nil
    end
  end

  defp failed_turns(_relay_id, _state), do: nil

  defp run(%{builder: %{run: :steps}}), do: "steps"
  defp run(_state), do: "flow"

  @doc "The class an instance's durable records live under."
  @spec class_of(String.t(), map()) :: String.t()
  def class_of(relay_id, state),
    do:
      state
      |> Map.get(:builder, %{})
      |> Map.get(:opts, [])
      |> Keyword.get(:relay_class, relay_id)

  # Where the class keeps its work, from what its runner announced.
  defp durable_level(relay_id, state),
    do: relay_id |> class_of(state) |> ClassRegistry.durable_level() |> Atom.to_string()

  defp uptime_ms(%{started_at: started}) when is_integer(started),
    do: max(0, System.system_time(:millisecond) - started)

  defp uptime_ms(_), do: nil

  defp plugins(%{plugins: plugins} = extensions) do
    Enum.map(plugins, fn {plugin_id, plugin} ->
      %{
        id: plugin_id,
        provider: inspect(plugin.provider),
        hooks:
          Enum.map(plugin.hooks, fn {name, opts} ->
            %{name: Atom.to_string(name), opts: Map.new(opts)}
          end),
        state: TableHealth.inspect_state(extensions, plugin_id)
      }
    end)
  end

  defp plugins(_), do: []
end
