defmodule Flezha.State.Adoption do
  @moduledoc """
  An arriving operator's durable state replaced by the newest copy there is:
  the copy on the node it moves from, or else the cut another controller
  shipped to the durability backend, compared by the writer epoch and top seq
  each reference carries (`Artifact.copy_supersedes?/2`).

  A local copy the newer one replaces is set aside with one rename
  (`Paths.set_aside/1`), so the pull (`WalTransfer.pull_operator/3`) or the
  hydration (`DurabilityTier.hydrate_operator/1`) runs as on a node that holds
  nothing. Once every reference of the newer copy has landed, what only the old
  copy held (a reference the newer one lacks, a class-level sidecar) is copied
  back and the old copy is removed; otherwise the old copy comes back whole.
  The landing is recorded with the files to copy back (`Paths.mark_landed/2`)
  before any is copied, so a crash leaves either a landed copy whose copy-back
  the next arrival finishes, or an unmarked set-aside copy it puts back. Nothing is replaced
  while another operator bound on this node owns the class.

  It runs once per arrival (`Flezha.Operator.Replay.adopt_moved_state/3`),
  before any writer of the operator opens on this node.
  """

  require Logger

  alias Flezha.Operator.Lookup
  alias Flezha.State.{DurabilityTier, Paths, Supervisor, WalTransfer}
  alias Flezha.State.DurabilityTier.Artifact

  @doc """
  Replace `operator_class`'s local copy with the newest one: the copy on
  `source` when a node is named, else the backend's. A copy that is not newer,
  or that does not land whole, leaves the local one as it is.
  """
  @spec adopt_newest(binary(), node() | nil) :: :ok
  def adopt_newest(operator_class, source) do
    :ok = recover(operator_class)

    with theirs when theirs != %{} <- newest(operator_class, source),
         ours = Artifact.freshness_of(operator_class),
         true <- Artifact.copy_supersedes?(theirs, ours),
         :unbound <- bound_here(operator_class) do
      kept = set_aside(operator_class, ours, theirs)
      fetch(operator_class, source)
      settle(operator_class, kept, landed?(operator_class, theirs))
      # Whatever the replaced copy had open (an operator stopped before its
      # writers closed, a late frame admitted during the fetch) closes before
      # the bind opens the class's writers.
      Supervisor.close_writers(operator_class)
    end

    :ok
  end

  defp bound_here(operator_class) do
    case Lookup.overwatch(operator_class) do
      {:ok, pid} when pid != self() ->
        Logger.warning(
          "[state] #{operator_class}: a newer copy is not adopted, the operator is bound " <>
            "on this node; the local copy stays"
        )

        :bound

      _none_or_self ->
        :unbound
    end
  end

  defp newest(operator_class, nil), do: DurabilityTier.shipped_freshness(operator_class)
  defp newest(operator_class, source), do: WalTransfer.freshness(source, operator_class)

  defp fetch(operator_class, nil), do: DurabilityTier.hydrate_operator(operator_class)
  defp fetch(operator_class, source), do: WalTransfer.pull_operator(source, operator_class)

  # A node holding no reference fetches over what it has, as it always did.
  defp set_aside(_operator_class, ours, _theirs) when ours == %{}, do: []

  defp set_aside(operator_class, ours, theirs) do
    live = Paths.class_dir(operator_class)
    only_here = Map.keys(ours) -- Map.keys(theirs)

    kept =
      for(ref <- only_here, path <- Paths.durable_files(ref), File.exists?(path), do: path) ++
        for name <- File.ls!(live), path = Path.join(live, name), File.regular?(path), do: path

    :ok = Paths.set_aside(operator_class)
    Enum.map(kept, &Path.relative_to(&1, live))
  end

  defp landed?(operator_class, theirs) do
    landed = Artifact.freshness_of(operator_class)

    Enum.all?(Map.keys(theirs), &Map.has_key?(landed, &1)) and
      Artifact.copy_supersedes?(landed, theirs)
  end

  # A set-aside copy marked landed only lacks its copy-back; an unmarked one
  # is an adoption that never completed.
  defp recover(operator_class) do
    case Paths.landed_kept(operator_class) do
      {:ok, kept} -> keep(operator_class, kept)
      :none -> Paths.restore_set_aside(operator_class)
    end
  end

  defp settle(operator_class, kept, true = _landed) do
    :ok = Paths.mark_landed(operator_class, kept)
    keep(operator_class, kept)
  end

  defp settle(operator_class, _kept, false) do
    if File.exists?(Paths.set_aside_dir(operator_class)) do
      Logger.warning(
        "[state] #{operator_class}: a newer copy did not land whole; the local copy stays"
      )
    end

    Paths.restore_set_aside(operator_class)
  end

  # Idempotent: a file the newer copy or an earlier pass already placed stays.
  defp keep(operator_class, kept) do
    live = Paths.class_dir(operator_class)
    aside = Paths.set_aside_dir(operator_class)

    for rel <- kept,
        from = Path.join(aside, rel),
        to = Path.join(live, rel),
        File.exists?(from) and not File.exists?(to) do
      File.mkdir_p!(Path.dirname(to))
      File.cp!(from, to)
    end

    Paths.drop_set_aside(operator_class)
  end
end
