defmodule Flezha.Convergence.Intent do
  @moduledoc """
  Pure reader of declared, durable state — "what the system should look
  like". Derived from the hub's declared set (`HubManager.declared_relays/1`):
  every running namespace's declared relays with the factory their start
  args recorded, plus each one's declared plugins. Intent, not runtime — a
  wiped runtime registry does not empty this list, and a deliberate stop
  removes its entry for good. Read-only; never mutates the declared set.

  Bindings come from the durable registry cspec (`relay_bindings/2`), the
  record `flz plugin bind` and `unbind` keep current; the declared entry holds
  the same list, which only a total registry loss falls back to (via the
  reconcile replaying it).

  The Loop diffs this against `Probe.snapshot/0` (reality) each tick. Starting
  a declared-but-absent relay is the hub's own reconcile's job, through the
  idempotent start authority; the Loop starts one only as a safety net, once it
  has run nowhere for longer than the unmet-intent grace (`detect_unplaced/3`).
  """

  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Service.HubManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths

  require Logger

  @type relay :: %{
          namespace: atom(),
          relay_id: binary(),
          factory_id: binary() | nil,
          plugins: [{binary(), Plugin.t()}],
          disabled_plugins: MapSet.t(binary())
        }
  @type t :: %__MODULE__{relays: [relay()]}

  defstruct relays: []

  @spec declared() :: t()
  def declared() do
    relays =
      HubManager.get_running_namespaces()
      |> Enum.flat_map(&declared_for_namespace/1)

    %__MODULE__{relays: relays}
  end

  defp declared_for_namespace(namespace) do
    relays =
      namespace
      |> intent_entries()
      |> Enum.map(fn %{id: relay_id, factory_id: factory_id} ->
        %{
          namespace: namespace,
          relay_id: relay_id,
          factory_id: factory_id,
          plugins: declared_bindings(namespace, relay_id)
        }
      end)

    gone = unresolved_plugins(namespace, relays)

    Enum.map(relays, fn relay ->
      disabled =
        for {id, _} <- relay.plugins, Plugin.base_id(id) in gone, into: MapSet.new(), do: id

      Map.put(relay, :disabled_plugins, disabled)
    end)
  catch
    :exit, _ -> []
  end

  # Under the auto-restore kill-switch nothing is ever declared (a hub
  # without the reconcile refuses durable starts), so an empty declared set
  # means "durable intent is off", not "nothing should run" — intent degrades
  # to the live-derived registry, the pre-declared-set behavior, and the loop
  # keeps healing what actually runs.
  defp intent_entries(namespace) do
    if HubManager.durable_starts?() do
      HubManager.declared_relays(namespace)
    else
      namespace
      |> HubManager.get_all_relays()
      |> Enum.map(fn {relay_id, _pid} -> %{id: relay_id, factory_id: nil} end)
    end
  end

  # The relay's bindings, as the durable cspec start args hold them: the
  # one record `flz plugin bind` and `unbind` write. Empty on any read failure,
  # so a transient registry hiccup never spuriously stops a plugin.
  defp declared_bindings(namespace, relay_id) do
    case HubManager.relay_bindings(to_string(namespace), relay_id) do
      {:ok, bindings} when is_list(bindings) -> bindings
      _ -> []
    end
  end

  # The bound plugins that no longer resolve here: a disable keeps their
  # bindings, and a stale copy of one can bring a binding up after the disable
  # ended. Read once per namespace and only while the hub is up, so a hub that
  # cannot answer disables none.
  defp unresolved_plugins(namespace, relays) do
    namespace = to_string(namespace)

    names =
      for %{plugins: plugins} <- relays, {id, _} <- plugins, uniq: true, do: Plugin.base_id(id)

    HubManager.with_live_hub_id(Identificator.hub(namespace), MapSet.new(), fn _hub_id ->
      for name <- names,
          HubManager.plugin_entry(namespace, name) == :error,
          into: MapSet.new(),
          do: name
    end)
  end

  @doc """
  Relays whose durable state evidence has had no live intent for longer than
  `convergence_unmet_intent_grace_ms`, and the refreshed evidence cache. A stop
  takes its relay out of the intent before its state is gone, so a younger
  mismatch is a stop still running. `{prev_mismatch, since}` is the last answer
  and when each relay was first seen without intent; the caller keeps it and
  the cache. A failure answers with what it was given, so the check can never
  cost the tick its heals or its publish.
  """
  @spec detect_mismatch(t(), {[binary()], %{binary() => integer()}}, term(), integer()) ::
          {{[binary()], %{binary() => integer()}}, term()}
  def detect_mismatch(intent, {prev_mismatch, since} = prev, cache, now) do
    {candidates, cache} = intent_mismatch(intent, cache)
    {mismatch, since} = outlived_grace(candidates, since, now)
    note_intent_mismatch_transition(prev_mismatch, mismatch)
    {{mismatch, since}, cache}
  rescue
    e ->
      Logger.warning("[convergence] intent-mismatch check skipped: #{Exception.message(e)}")
      {prev, cache}
  catch
    _, _ -> {prev, cache}
  end

  @doc """
  Declared relays that no hub member has held for longer than
  `convergence_unmet_intent_grace_ms`, as `{namespace, relay_id}`, and the
  refreshed map of when each was first seen unplaced. A relay is placed while
  its registry row is bound to a hub member, connected or held through a
  node-down grace, so a member that is only out of reach keeps its relays.
  Only the declared set is read, so a relay a stop removed, or one known only
  by a state directory, is never named; under the auto-restore kill-switch the
  intent is what runs, so none is. A hub that cannot be read names none of its
  relays, and a failure answers `{[], since}`.

  Only the namespaces in `watched` (`:all` for every one) and those with a
  relay already waiting are read, and only the hub's first member
  (`HubManager.leads?/1`) is answered the relays past the grace, so one node
  starts each.
  """
  @spec detect_unplaced(t(), %{{atom(), binary()} => integer()}, integer(), MapSet.t() | :all) ::
          {[{atom(), binary()}], %{{atom(), binary()} => integer()}}
  def detect_unplaced(%__MODULE__{relays: relays}, since, now, watched) do
    waiting = MapSet.new(Map.keys(since), &elem(&1, 0))

    placed =
      for namespace <- Enum.uniq(Enum.map(relays, & &1.namespace)),
          watched == :all or namespace in watched or namespace in waiting,
          into: %{},
          do: {namespace, HubManager.placed_relay_ids(namespace, :unreadable)}

    unplaced =
      for %{namespace: ns, relay_id: id} <- relays,
          (ids = Map.get(placed, ns, :unreadable)) != :unreadable,
          not MapSet.member?(ids, id),
          do: {ns, id}

    {due, since} = outlived_grace(unplaced, since, now)
    leads = due |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.filter(&HubManager.leads?/1)
    {Enum.filter(due, &(elem(&1, 0) in leads)), since}
  catch
    _, _ -> {[], since}
  end

  # The candidates first seen more than the unmet-intent grace ago, and when
  # each candidate was first seen; one that left the candidates starts over.
  defp outlived_grace(candidates, since, now) do
    since = Map.new(candidates, &{&1, Map.get(since, &1, now)})
    grace = Durations.get(:convergence_unmet_intent_grace_ms)
    {Enum.filter(candidates, &(now - since[&1] > grace)), since}
  end

  @doc false
  @spec intent_mismatch(Intent.t(), term()) :: {[binary()], term()}
  def intent_mismatch(%__MODULE__{relays: relays}, cache) do
    live = MapSet.new(relays, & &1.relay_id)
    {dirs, cache} = state_root_dirs(cache)
    evidence = MapSet.union(MapSet.new(Map.keys(dirs)), HubManager.durable_relay_ids())

    # A finished steps relay's state is retained for inspection on purpose
    # (`Flezha.State.Completion`): intent fulfilled, not intent lost. Its record
    # lives in the class directory, so a class with none on disk holds none.
    mismatch =
      evidence
      |> MapSet.difference(live)
      |> Enum.reject(&(Map.has_key?(dirs, &1) and Completion.exists_in?(dirs[&1])))
      |> Enum.sort()

    {mismatch, cache}
  end

  @doc false
  def note_intent_mismatch_transition(same, same), do: :ok

  def note_intent_mismatch_transition(_prev, []) do
    Logger.info("[convergence] intent mismatch cleared")
  end

  def note_intent_mismatch_transition(_prev, mismatch) do
    :telemetry.execute(
      [:flezha, :convergence, :intent_mismatch],
      %{count: length(mismatch)},
      %{relays: mismatch}
    )
  end

  # Relay-class directories under the namespaced `<root>/<ns>/<class>` tree,
  # by class, re-read only when the newest relevant mtime changes: the root's (a
  # namespace added/removed) or any namespace dir's (a class added/removed);
  # deeper writes touch neither. A cache entry younger than 2 s is never
  # trusted — POSIX mtime has 1 s granularity, so a same-second add+remove
  # would otherwise pin a stale listing forever.
  defp state_root_dirs(cache) do
    root = Paths.state_root()

    case File.stat(root, time: :posix) do
      {:ok, %File.Stat{mtime: root_mtime}} ->
        mtime = Enum.max([root_mtime | namespace_dir_mtimes()])
        now_s = System.os_time(:second)

        case cache do
          {^mtime, dirs} when now_s - mtime > 1 ->
            {dirs, cache}

          _ ->
            dirs = Map.new(Paths.class_dirs())
            {dirs, {mtime, dirs}}
        end

      _ ->
        {%{}, nil}
    end
  end

  defp namespace_dir_mtimes() do
    for namespace <- Paths.namespaces(),
        {:ok, %File.Stat{mtime: mtime}} <-
          [File.stat(Paths.namespace_dir(namespace), time: :posix)],
        do: mtime
  end
end
