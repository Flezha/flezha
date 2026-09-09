defmodule Flezha.Runner.FleetManager.Load do
  @moduledoc """
  How many operatives each runner holds, and which candidate the next one goes
  to.

  Everything the count is: `counts/2` folds a hub registry dump into one count
  per runner, `refreshed/3` is the cache `FleetManager` keeps of it, `place/3`
  hands a runner out of that cache, and `select/3` is the ranking underneath,
  which the node path uses on its own. The only inputs none of them takes as an
  argument are the two declared knobs, which are table reads.

  `FleetManager` holds the cache in its state and calls in, the same way it
  holds the runner index for `Roster` and the factory table for `Factories`.
  The one thing it does not delegate is the registry read a rebuild folds: this
  layer may not name the hub's service, which is also why the rule lives here
  rather than beside its caller in `service/`. It has to, because the manager
  reads, chooses and counts inside one `handle_call` — split that and two
  members of a recovery batch are handed the same slot.
  `Flezha.Service.OperativeManager.Placement` calls down into it for the node
  case, so the ranking has one implementation and not two.

  The count is a fairness input and never a correctness one. Candidates arrive
  from `Flezha.Runner.FleetManager.Factories.placeable_runners/3`, the single
  placement filter, so whatever the counts say, the selected runner is active,
  advertises the factory and matches its established language. A wrong count
  costs an even spread and nothing else.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations

  @typedoc "Operatives bound to each runner, one entry per active runner."
  @type counts :: %{binary() => non_neg_integer()}

  @typedoc """
  One namespace's counts, the runner each operative was handed since the
  rebuild, and when that rebuild was.
  """
  @type entry :: %{
          built_at: integer(),
          counts: counts(),
          handed: %{binary() => binary()}
        }

  @typedoc "The cache `FleetManager` holds, one entry per namespace."
  @type cache :: %{String.t() => entry()}

  @doc """
  Operatives per runner, from a hub registry dump.

  Reads each row's `metadata.tag`, the runner it records as owning the
  operative, and nothing else. Every id in `runner_ids` is present in the
  answer, so a runner holding nothing reads as `0` rather than as absent: to an
  operator and to the cap alike those are different answers. A row tagged with a
  runner outside the set is counted nowhere — it names a runner that has gone.
  """
  @spec counts(map(), [binary()]) :: counts()
  def counts(registry_dump, runner_ids) do
    Enum.reduce(registry_dump, Map.new(runner_ids, &{&1, 0}), fn
      {_operative_id, {_spec, _node_pids, %{tag: runner_id}}}, acc
      when is_map_key(acc, runner_id) ->
        Map.update!(acc, runner_id, &(&1 + 1))

      _row, acc ->
        acc
    end)
  end

  @doc """
  `namespace`'s entry, rebuilt through `rebuild` once the cached one has aged
  past `:placement_load_refresh_ms`.

  The rebuild is what stops the handouts drifting: a placement that never
  becomes a running operative inflates a count until the next one, and no
  further. It is passed in rather than performed here because folding it means
  reading the hub registry, which this layer may not do.
  """
  @spec refreshed(cache(), String.t(), (-> counts())) :: {cache(), entry()}
  def refreshed(cache, namespace, rebuild) do
    cached = Map.get(cache, namespace)

    if fresh?(cached) do
      {cache, cached}
    else
      entry = %{
        built_at: System.monotonic_time(:millisecond),
        counts: rebuild.(),
        handed: %{}
      }

      {Map.put(cache, namespace, entry), entry}
    end
  end

  @doc """
  Hand `operative_id` one of `candidates`, counting the choice.

  Counting on handout rather than on the registry write is what a recovery batch
  needs: its members are all placed before any of their rows is written, so a
  count that only advanced with the registry would be identical for every one of
  them and the batch would spread exactly as an uncapped hash spread it.

  An operative already handed a runner since the last rebuild is handed the same
  one again, and counted once. Binding one operative resolves its runner more
  than once — `Lifecycle.bind/3` awaits one and `complete_init` re-resolves
  before its attempt — and without this each of those would see the previous
  one's own increment, pick a different runner, and count both. The memory
  lapses the moment the runner stops being a candidate, which is the only case
  in which anything wants a different answer.
  """
  @spec place(entry(), [binary()], binary()) :: {entry(), binary() | nil}
  def place(entry, candidates, operative_id) do
    handed = Map.get(entry.handed, operative_id)

    if handed in candidates do
      {entry, handed}
    else
      chosen = select(candidates, operative_id, entry.counts)
      {count(entry, operative_id, chosen), chosen}
    end
  end

  defp count(entry, _operative_id, nil), do: entry

  defp count(entry, operative_id, runner_id) do
    %{
      entry
      | counts: Map.update(entry.counts, runner_id, 1, &(&1 + 1)),
        handed: Map.put(entry.handed, operative_id, runner_id)
    }
  end

  defp fresh?(nil), do: false

  defp fresh?(%{built_at: built_at}),
    do: System.monotonic_time(:millisecond) - built_at < Durations.get(:placement_load_refresh_ms)

  @doc """
  The candidate `operative_id` is placed on, or `nil` when there is none.

  Candidates are ranked by a rendezvous hash of the operative id against the
  candidate, and the answer is the highest-ranked one whose count is below the
  fairness cap. When every candidate is at cap the ranking head wins, so a cap
  never refuses a placement the filter allowed.

  An empty `load` is no cap at all and reproduces plain rendezvous hashing
  exactly. That is what the node-selection path passes: a node has no operative
  count, and repeated convergence ticks have to agree on one destination.
  """
  @spec select([candidate], term(), %{candidate => non_neg_integer()}) :: candidate | nil
        when candidate: term()
  def select([], _operative_id, _load), do: nil

  def select(candidates, operative_id, load) do
    ranked = Enum.sort_by(candidates, &rank(operative_id, &1), :desc)
    head = hd(ranked)

    case cap(candidates, load) do
      :none -> head
      cap -> Enum.find(ranked, head, &(Map.get(load, &1, 0) < cap))
    end
  end

  # Highest wins, which is what `max_by` over the same hash answered before the
  # cap existed. Descending sort is stable, so the ranking head is that same
  # candidate down to a hash collision.
  defp rank(operative_id, candidate), do: :erlang.phash2({operative_id, candidate})

  # No counts is no cap: the node path and any caller that has not built a map
  # get plain rendezvous hashing rather than a cap computed from zeroes.
  defp cap(_candidates, load) when map_size(load) == 0, do: :none

  # `max(1, …)`: a fleet with more runners than operatives has a fair share
  # below one, and a cap of nothing would place nothing.
  defp cap(candidates, load) do
    placed = Enum.reduce(candidates, 0, &(Map.get(load, &1, 0) + &2))

    max(
      1,
      ceil(Config.get(:placement_load_cap_percent) * (placed + 1) / (100 * length(candidates)))
    )
  end
end
