defmodule Flezha.Convergence.Intent do
  @moduledoc """
  Pure reader of declared, durable state — "what the system should look
  like". Derived from the hub's declared set (`HubManager.declared_operators/1`):
  every running namespace's declared operators with the factory their start
  args recorded, plus each one's declared plugins. Intent, not runtime — a
  wiped runtime registry does not empty this list, and a deliberate stop
  removes its entry for good. Read-only; never mutates the declared set.

  Bindings come from the durable registry cspec (`operator_bindings/2`), the
  record `flz plugin bind` and `unbind` keep current; the declared entry holds
  the same list, which only a total registry loss falls back to (via the
  reconcile replaying it).

  The Loop diffs this against `Probe.snapshot/0` (reality) each tick. Starting
  a declared-but-absent operator is NOT this pipeline's job — the hub's own
  reconcile converges running children toward the declared set through the
  idempotent start authority.
  """

  alias Flezha.Plugin
  alias Flezha.Service.HubManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths

  require Logger

  @type operator :: %{
          namespace: atom(),
          operator_id: binary(),
          factory_id: binary() | nil,
          plugins: [{binary(), Plugin.t()}]
        }
  @type t :: %__MODULE__{operators: [operator()]}

  defstruct operators: []

  @spec declared() :: t()
  def declared() do
    operators =
      HubManager.get_running_namespaces()
      |> Enum.flat_map(&declared_for_namespace/1)

    %__MODULE__{operators: operators}
  end

  defp declared_for_namespace(namespace) do
    namespace
    |> intent_entries()
    |> Enum.map(fn %{id: operator_id, factory_id: factory_id} ->
      %{
        namespace: namespace,
        operator_id: operator_id,
        factory_id: factory_id,
        plugins: declared_bindings(namespace, operator_id)
      }
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
      HubManager.declared_operators(namespace)
    else
      namespace
      |> HubManager.get_all_operators()
      |> Enum.map(fn {operator_id, _pid} -> %{id: operator_id, factory_id: nil} end)
    end
  end

  # The operator's bindings, as the durable cspec start args hold them: the
  # one record `flz plugin bind` and `unbind` write. Empty on any read failure,
  # so a transient registry hiccup never spuriously stops a plugin.
  defp declared_bindings(namespace, operator_id) do
    case HubManager.operator_bindings(to_string(namespace), operator_id) do
      {:ok, bindings} when is_list(bindings) -> bindings
      _ -> []
    end
  end

  @doc """
  Operators with durable state evidence but no live intent, and the refreshed
  evidence cache. `{prev_mismatch, cache}` in, `{mismatch, cache}` out; the
  caller keeps both. A failure answers with what it was given, so the check can
  never cost the tick its heals or its publish.
  """
  @spec detect_mismatch(t(), [binary()], term()) :: {[binary()], term()}
  def detect_mismatch(intent, prev_mismatch, cache) do
    {mismatch, cache} = intent_mismatch(intent, cache)
    note_intent_mismatch_transition(prev_mismatch, mismatch)
    {mismatch, cache}
  rescue
    e ->
      Logger.warning("[convergence] intent-mismatch check skipped: #{Exception.message(e)}")
      {prev_mismatch, cache}
  catch
    _, _ -> {prev_mismatch, cache}
  end

  @doc false
  @spec intent_mismatch(Intent.t(), term()) :: {[binary()], term()}
  def intent_mismatch(%__MODULE__{operators: operators}, cache) do
    live = MapSet.new(operators, & &1.operator_id)
    {dirs, cache} = state_root_dirs(cache)
    evidence = MapSet.union(MapSet.new(dirs), HubManager.durable_operator_ids())

    # A finished steps operator's state is retained for inspection on purpose
    # (`Flezha.State.Completion`): intent fulfilled, not intent lost.
    mismatch =
      evidence
      |> MapSet.difference(live)
      |> Enum.reject(&Completion.exists?/1)
      |> Enum.sort()

    {mismatch, cache}
  end

  @doc false
  def note_intent_mismatch_transition(same, same), do: :ok

  def note_intent_mismatch_transition(_prev, []) do
    Logger.info("[convergence] intent mismatch cleared")
  end

  def note_intent_mismatch_transition(_prev, mismatch) do
    Logger.warning(
      "[convergence] intent mismatch — durable state evidence without live intent: #{inspect(mismatch)}"
    )

    :telemetry.execute(
      [:flezha, :convergence, :intent_mismatch],
      %{count: length(mismatch)},
      %{operators: mismatch}
    )
  end

  # Operator-class listing under the namespaced `<root>/<ns>/<class>` tree,
  # re-read only when the newest relevant mtime changes — the root's (a
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
            dirs = Paths.operator_classes()
            {dirs, {mtime, dirs}}
        end

      _ ->
        {[], nil}
    end
  end

  defp namespace_dir_mtimes() do
    for namespace <- Paths.namespaces(),
        {:ok, %File.Stat{mtime: mtime}} <-
          [File.stat(Paths.namespace_dir(namespace), time: :posix)],
        do: mtime
  end
end
