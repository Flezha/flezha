defmodule Flezha.State.Adoption do
  @moduledoc """
  An operator's durable state replaced whole by a newer copy when the operator
  comes to this node: the copy on the node it moves from, or the cut another
  controller shipped to the durability backend.

  The copy is staged beside the class directory (`Paths.staging_class/1`), every
  reference at once; one that does not land discards the stage, so the Cells,
  the task-journal and the input log never come from two copies. It replaces
  the local copy only when `Artifact.copy_supersedes?/2` says so, and never
  while an operator bound on this node owns the class: writers an operator
  left open when it moved away are closed first, under the lock every writer
  start takes (`Supervisor.without_new_writers/2`). A refusal is logged and
  emitted as `[:flezha, :state, :adoption_refused]`. The switch is two directory
  renames, the live directory aside and the staged one into its place, and
  `settle/0` resolves one a crash interrupted before anything reads the tree:
  the old copy comes back unless the switch had completed.

  It runs once per arrival, where the operator is adopted
  (`Flezha.Operator.Replay.adopt_moved_state/3`), before anything of it is open.
  """

  require Logger

  alias Flezha.Operator.Lookup
  alias Flezha.State.{DurabilityTier, Paths, Supervisor, WalTransfer}
  alias Flezha.State.DurabilityTier.Artifact

  @doc """
  Replace `operator_class`'s local copy with the newest one available: the
  copy on `source` when a node is named, else the backend's. `:ok` whatever
  the outcome; a copy that is not newer, or cannot be read, leaves the local
  one as it is and says why.
  """
  @spec adopt_newest(binary(), node() | nil) :: :ok
  def adopt_newest(operator_class, source) do
    if source != nil or DurabilityTier.enabled?() do
      discard_staged(operator_class)

      case stage(operator_class, source) do
        {:ok, seeds} -> switch_if_newer(operator_class, seeds)
        :none -> :ok
        :not_newer -> refuse(operator_class, :not_newer)
        {:error, reason} -> refuse(operator_class, {:not_staged, reason})
      end

      discard_staged(operator_class)
    end

    :ok
  end

  defp discard_staged(operator_class),
    do: Paths.remove_adoption_dir(Paths.class_dir(Paths.staging_class(operator_class)))

  defp stage(operator_class, nil), do: DurabilityTier.stage_operator(operator_class)

  defp stage(operator_class, source) do
    with :ok <- WalTransfer.stage_operator(source, operator_class), do: {:ok, []}
  end

  defp switch_if_newer(operator_class, seeds) do
    staged = Artifact.freshness_of(Paths.staging_class(operator_class), operator_class)

    if Artifact.copy_supersedes?(staged, Artifact.freshness_of(operator_class)),
      do: switch(operator_class, seeds),
      else: refuse(operator_class, :not_newer)
  end

  # Under the class's start lock, so no writer opens between the close and the
  # renames. Writers still open belong to an operator that is no longer bound
  # here (one that moved away, or a sweep's input log) and close flushed; an
  # operator another Overwatch here is bound with keeps its copy.
  defp switch(operator_class, seeds) do
    Supervisor.without_new_writers(operator_class, fn ->
      if bound_here?(operator_class) do
        refuse(operator_class, :bound)
      else
        :ok = Supervisor.close_writers(operator_class)
        with :ok <- switch_in(operator_class), do: DurabilityTier.seed_adopted(seeds)
      end
    end)
  end

  defp bound_here?(operator_class),
    do: match?({:ok, pid} when pid != self(), Lookup.overwatch(operator_class))

  defp refuse(operator_class, reason) do
    Logger.warning(
      "[state] #{operator_class}: a newer copy is not adopted, #{why(reason)}; " <>
        "the local copy stays"
    )

    :telemetry.execute([:flezha, :state, :adoption_refused], %{count: 1}, %{
      operator_class: operator_class,
      reason: inspect(reason)
    })
  end

  defp why(:not_newer), do: "the incoming copy is not newer than this node's"
  defp why(:bound), do: "the operator is bound on this node"
  defp why({:not_staged, reason}), do: "it could not be staged (#{inspect(reason)})"

  # What the staged copy lacks is carried over from the live directory: the
  # references only this node holds (a key written since the other copy was
  # taken, a Cell no cut names yet), then the class-level sidecars. Adoption
  # replaces references, never deletes one.
  defp switch_in(operator_class) do
    live = Paths.class_dir(operator_class)
    staged_class = Paths.staging_class(operator_class)
    staged = Paths.class_dir(staged_class)
    aside = Paths.class_dir(Paths.set_aside_class(operator_class))
    held = MapSet.new(Paths.durable_refs(staged_class), &Paths.ref_under(&1, operator_class))

    for ref <- Paths.durable_refs(operator_class),
        not MapSet.member?(held, ref),
        path <- Paths.durable_files(ref),
        File.exists?(path),
        dest = Paths.durable_file_path(Paths.ref_under(ref, staged_class), Path.basename(path)) do
      File.mkdir_p!(Path.dirname(dest))
      File.cp!(path, dest)
    end

    for name <- Paths.list_dir(live),
        File.regular?(Path.join(live, name)),
        not File.exists?(Path.join(staged, name)),
        do: File.cp!(Path.join(live, name), Path.join(staged, name))

    with :ok <- rename_present(live, aside),
         :ok <- rename_or_restore(staged, live, aside) do
      Paths.remove_adoption_dir(aside)
    end
  end

  defp rename_present(from, to), do: if(File.exists?(from), do: File.rename(from, to), else: :ok)

  defp rename_or_restore(staged, live, aside) do
    with {:error, reason} = err <- File.rename(staged, live) do
      _ = rename_present(aside, live)
      Logger.error("[state] could not switch #{live} to a newer copy: #{inspect(reason)}")
      err
    end
  end

  @doc """
  Resolve every adoption a crash interrupted. Runs at boot, before anything
  reads the state tree: a staged copy is discarded, and a directory set aside
  comes back when its class has none, or goes when the switch had completed.
  """
  @spec settle() :: :ok
  def settle() do
    for namespace <- Paths.namespaces(),
        dir = Paths.namespace_dir(namespace),
        name <- Paths.list_dir(dir),
        class = Paths.adopting_for(name),
        class != nil do
      path = Path.join(dir, name)

      cond do
        name == Paths.staging_class(class) -> Paths.remove_adoption_dir(path)
        File.dir?(Path.join(dir, class)) -> Paths.remove_adoption_dir(path)
        true -> File.rename!(path, Path.join(dir, class))
      end
    end

    :ok
  end
end
