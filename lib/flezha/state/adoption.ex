defmodule Flezha.State.Adoption do
  @moduledoc """
  An arriving relay's durable state replaced by the newest copy there is:
  the copy on the node it moves from, or else the cut another controller
  shipped to the durability backend, compared by the writer epoch and top seq
  each reference carries (`Artifact.copy_supersedes?/2`).

  A local copy the newer one replaces, an empty one on a node that held
  nothing, is set aside with one rename (`Paths.set_aside/1`), so the pull
  (`WalTransfer.pull_relay/3`) or the hydration
  (`DurabilityTier.hydrate_relay/1`) runs as on a node that holds nothing.
  Once every reference of the newer copy has landed, what only the old
  copy held (a reference the newer one lacks, a class-level sidecar) is copied
  back and the old copy is removed; otherwise the old copy comes back whole.
  The landing is recorded with the files to copy back (`Paths.mark_landed/2`)
  before any is copied, so a crash leaves either a landed copy whose copy-back
  the next arrival finishes, or an unmarked set-aside copy it puts back. Nothing is replaced
  while another relay bound on this node owns the class.

  It runs on arrival (`Flezha.Relay.Replay.adopt_moved_state/4`) and, while
  the class holds nothing, again before the runner starts; the class's writers
  close before its copy is set aside.
  """

  require Logger

  alias Flezha.Relay.Lookup
  alias Flezha.State.{DurabilityTier, Paths, Supervisor, WalTransfer}
  alias Flezha.State.DurabilityTier.Artifact

  @doc """
  Replace `relay_class`'s local copy with the newest one: the copy on
  `source` when a node is named, else the backend's. A copy that is not newer,
  or that does not land whole (`:not_landed`), leaves the local one as it is.
  `owner` is the Overwatch the adoption runs for.
  """
  @spec adopt_newest(binary(), node() | nil, pid()) :: :ok | :not_landed
  def adopt_newest(relay_class, source, owner \\ self()) do
    :ok = recover(relay_class)

    with theirs when theirs != %{} <- newest(relay_class, source),
         ours = Artifact.freshness_of(relay_class),
         true <- Artifact.copy_supersedes?(theirs, ours),
         :unbound <- bound_here(relay_class, owner) do
      # Closed before the rename (a stopped relay's or a restarted runner's
      # writers) and after the fetch (one a late frame opened meanwhile).
      Supervisor.close_writers(relay_class)
      kept = set_aside(relay_class, ours, theirs)
      fetch(relay_class, source)
      verdict = settle(relay_class, kept, landed?(relay_class, theirs))
      Supervisor.close_writers(relay_class)
      verdict
    else
      _current_or_bound -> :ok
    end
  end

  defp bound_here(relay_class, owner) do
    case Lookup.overwatch(relay_class) do
      {:ok, pid} when pid != owner ->
        Logger.warning(
          "[state] #{relay_class}: a newer copy is not adopted, the relay is bound " <>
            "on this node; the local copy stays"
        )

        :bound

      _none_or_self ->
        :unbound
    end
  end

  defp newest(relay_class, nil), do: DurabilityTier.shipped_freshness(relay_class)
  defp newest(relay_class, source), do: WalTransfer.freshness(source, relay_class)

  defp fetch(relay_class, nil), do: DurabilityTier.hydrate_relay(relay_class)
  defp fetch(relay_class, source), do: WalTransfer.pull_relay(source, relay_class)

  # A node that held nothing sets aside an empty copy, so a newer one that
  # does not land whole is removed with it and the node stays cold.
  defp set_aside(relay_class, ours, theirs) do
    live = Paths.class_dir(relay_class)
    only_here = Map.keys(ours) -- Map.keys(theirs)
    File.mkdir_p!(live)

    kept =
      for(ref <- only_here, path <- Paths.durable_files(ref), File.exists?(path), do: path) ++
        for name <- File.ls!(live), path = Path.join(live, name), File.regular?(path), do: path

    :ok = Paths.set_aside(relay_class)
    Enum.map(kept, &Path.relative_to(&1, live))
  end

  defp landed?(relay_class, theirs) do
    landed = Artifact.freshness_of(relay_class)

    Enum.all?(Map.keys(theirs), &Map.has_key?(landed, &1)) and
      Artifact.copy_supersedes?(landed, theirs)
  end

  # A set-aside copy marked landed only lacks its copy-back; an unmarked one
  # is an adoption that never completed.
  defp recover(relay_class) do
    case Paths.landed_kept(relay_class) do
      {:ok, kept} -> keep(relay_class, kept)
      :none -> Paths.restore_set_aside(relay_class)
    end
  end

  defp settle(relay_class, kept, true = _landed) do
    :ok = Paths.mark_landed(relay_class, kept)
    keep(relay_class, kept)
    :ok
  end

  defp settle(relay_class, _kept, false) do
    Logger.warning(
      "[state] #{relay_class}: a newer copy did not land whole; the local copy stays"
    )

    Paths.restore_set_aside(relay_class)
    :not_landed
  end

  # Idempotent: a file the newer copy or an earlier pass already placed stays.
  defp keep(relay_class, kept) do
    live = Paths.class_dir(relay_class)
    aside = Paths.set_aside_dir(relay_class)

    for rel <- kept,
        from = Path.join(aside, rel),
        to = Path.join(live, rel),
        File.exists?(from) and not File.exists?(to) do
      File.mkdir_p!(Path.dirname(to))
      File.cp!(from, to)
    end

    Paths.drop_set_aside(relay_class)
  end
end
