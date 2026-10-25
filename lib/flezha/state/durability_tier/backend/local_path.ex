defmodule Flezha.State.DurabilityTier.Backend.LocalPath do
  @moduledoc """
  Remote durability backend over a filesystem directory. Dependency-free.

  Only protects against whole-cluster loss when `:path` points at storage that
  outlives the cluster — an NFS mount, a volume provisioned outside the
  cluster's disks. A path on a node's own disk shares that disk's fate.

  Each key maps to one file under the root; the claim counter lives in a
  `.claim` sidecar so an unconditional `put` preserves it, matching the Postgres
  backend's insert-0 / update-preserves semantics. Writes are atomic (fsynced
  temp file + rename). A filesystem offers no compare-and-set, so `put_if/4` is
  read-then-write: two simultaneous claimants can race it, which the
  single-writer-per-lineage shipping model tolerates and a fencing-critical
  deployment should answer with the Postgres or S3 backend instead.

  Option: `:path`, the artifact root directory, created if missing.
  """

  @behaviour Flezha.State.DurabilityTier.Backend

  alias Flezha.State.Durable
  alias Flezha.State.DurabilityTier.Backend
  alias Flezha.State.Paths

  @claim_suffix ".claim"

  @impl true
  def child_spec(_opts), do: :ignore

  # Every key is a file under `:path`; without one there is no root to join.
  @impl true
  def required_opts(), do: [:path]

  @impl true
  def put(key, blob, opts), do: write_atomic(file_path(key, opts), blob)

  @impl true
  def get(key, opts) do
    case File.read(file_path(key, opts)) do
      {:ok, blob} -> {:ok, blob}
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def delete(key, opts) do
    path = file_path(key, opts)

    with :ok <- rm_idempotent(path),
         :ok <- rm_idempotent(path <> @claim_suffix) do
      :ok
    end
  end

  # Keys are relative paths, so a prefix names a directory plus a partial entry
  # name in it: only that directory is walked, never the rest of the store.
  @impl true
  def list(prefix, opts) do
    root = root(opts)
    now = System.system_time(:millisecond)
    {dirs, [partial]} = prefix |> String.split("/") |> Enum.split(-1)

    keys =
      Path.join([root | dirs])
      |> walk_files(partial)
      |> Enum.reject(&(String.ends_with?(&1, @claim_suffix) or Durable.in_progress?(&1)))
      |> Enum.map(&{Path.relative_to(&1, root), age_of(&1, now)})

    {:ok, keys}
  rescue
    error -> {:error, error}
  end

  @impl true
  def put_if(key, blob, guard, opts) do
    path = file_path(key, opts)
    claim = elem(guard, 1)

    if Backend.guard_passes?(guard, stored_claim(path)) do
      with :ok <- write_atomic(path, blob) do
        write_atomic(path <> @claim_suffix, Integer.to_string(claim))
      end
    else
      {:error, {:claim_held, stored_claim(path)}}
    end
  end

  # ---------- files ----------

  defp stored_claim(path) do
    with {:ok, content} <- File.read(path <> @claim_suffix),
         {claim, ""} <- Integer.parse(String.trim(content)) do
      claim
    else
      # No sidecar (or an unreadable one) reads as claim 0 — the same value a
      # fresh Postgres row carries, so `{:claim, 1}` wins a virgin key on both.
      _ -> 0
    end
  end

  # The rename is only atomic-durable if the bytes it points at are already on
  # the platter — this tier's whole contract — which is what the declaration's
  # `durability: :inline` buys. The suffix is unique per write because two
  # claimants may race one key.
  defp write_atomic(path, content) do
    Paths.atomic_install(path, &:file.write(&1, content), :tier_object)
  rescue
    error -> {:error, error}
  end

  defp rm_idempotent(path) do
    case File.rm(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp walk_files(dir, partial) do
    case File.ls(dir) do
      {:ok, entries} ->
        entries
        |> Enum.filter(&String.starts_with?(&1, partial))
        |> Enum.flat_map(fn entry ->
          path = Path.join(dir, entry)
          if File.dir?(path), do: walk_files(path, ""), else: [path]
        end)

      {:error, _} ->
        []
    end
  end

  defp age_of(path, now) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> max(0, now - mtime * 1_000)
      # Unreadable stat reports age 0: the artifact reads as young and is left
      # alone. Collection may leak, never lose.
      {:error, _} -> 0
    end
  end

  defp file_path(key, opts), do: Path.join(root(opts), key)
  defp root(opts), do: Keyword.fetch!(opts, :path)
end
