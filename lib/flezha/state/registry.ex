defmodule Flezha.State.Registry do
  @moduledoc """
  Path resolution for per-Partition WAL, snapshot, and outbox-dedup files.

  All persistence files live under
  `<state_root>/<relay_class>/<shard_id>/` where `<state_root>` is
  configured via `:flezha, :state_root` (default `app_data/state`).

  `partition_key` is URL-safe-base64-encoded when it contains characters
  that would be unsafe on POSIX filesystems (anything outside `[A-Za-z0-9_.-]`).
  `decode_partition_key/1` reverses the encoding.
  """

  @type partition_id ::
          {relay_class :: binary(), shard_id :: binary(), partition_key :: binary()}

  @type scan_result :: %{
          wal_files: [{partition_key :: binary(), path :: binary()}],
          snapshot_files: [
            {partition_key :: binary(), seq :: non_neg_integer(), path :: binary()}
          ],
          outbox_files: [{partition_key :: binary(), path :: binary()}]
        }

  @safe_chars ~r/^[A-Za-z0-9_.-]+$/

  @spec state_root() :: binary()
  def state_root() do
    Application.get_env(:flezha, :state_root, "app_data/state")
  end

  @spec state_dir(partition_id()) :: binary()
  def state_dir({relay_class, shard_id, _partition_key}) do
    Path.join([state_root(), relay_class, shard_id])
  end

  @spec wal_path(partition_id()) :: binary()
  def wal_path({_, _, partition_key} = pid) do
    Path.join(state_dir(pid), encode_partition_key(partition_key) <> ".wal")
  end

  @spec snapshot_paths(partition_id()) :: [{non_neg_integer(), binary()}]
  def snapshot_paths({_, _, partition_key} = pid) do
    encoded = encode_partition_key(partition_key)
    prefix = encoded <> ".snap."

    pid
    |> state_dir()
    |> list_dir()
    |> Enum.flat_map(fn name ->
      case String.split(name, prefix, parts: 2) do
        ["", seq_str] ->
          case Integer.parse(seq_str) do
            {seq, ""} -> [{seq, Path.join(state_dir(pid), name)}]
            _ -> []
          end

        _ ->
          []
      end
    end)
    |> Enum.sort_by(fn {seq, _} -> seq end)
  end

  @spec outbox_dedup_path(partition_id()) :: binary()
  def outbox_dedup_path({_, _, partition_key} = pid) do
    Path.join(state_dir(pid), encode_partition_key(partition_key) <> ".outbox.dedup")
  end

  @spec snapshot_path(partition_id(), non_neg_integer()) :: binary()
  def snapshot_path({_, _, partition_key} = pid, seq) do
    Path.join(
      state_dir(pid),
      encode_partition_key(partition_key) <> ".snap." <> Integer.to_string(seq)
    )
  end

  @doc """
  Enumerate Partition files under a Relay tree.

  Pass `{relay_class, shard_id, ""}` to scan the whole Shard. Returns lists
  of `{partition_key, ...}` tuples grouped by file type. Files matching
  `.tmp` / `.rewriting` suffixes are intentionally skipped — they
  represent crash-residue from atomic-rename workflows.
  """
  @spec scan_dir(partition_id()) :: scan_result()
  def scan_dir({_, _, _} = pid) do
    dir = state_dir(pid)

    dir
    |> list_dir()
    |> Enum.reduce(%{wal_files: [], snapshot_files: [], outbox_files: []}, fn name, acc ->
      cond do
        String.ends_with?(name, ".tmp") or String.ends_with?(name, ".rewriting") ->
          acc

        String.ends_with?(name, ".wal") ->
          key = name |> String.trim_trailing(".wal") |> decode_partition_key()
          %{acc | wal_files: [{key, Path.join(dir, name)} | acc.wal_files]}

        String.ends_with?(name, ".outbox.dedup") ->
          key = name |> String.trim_trailing(".outbox.dedup") |> decode_partition_key()
          %{acc | outbox_files: [{key, Path.join(dir, name)} | acc.outbox_files]}

        true ->
          case parse_snapshot_name(name) do
            {:ok, encoded_key, seq} ->
              key = decode_partition_key(encoded_key)

              %{
                acc
                | snapshot_files: [{key, seq, Path.join(dir, name)} | acc.snapshot_files]
              }

            :error ->
              acc
          end
      end
    end)
  end

  @doc """
  Enumerate every Partition that has any persistence file on disk under
  `state_root()`. Walks the two-level directory tree
  `<state_root>/<relay_class>/<shard_id>/`.
  """
  @spec all_partitions() :: [partition_id()]
  def all_partitions() do
    root = state_root()

    for rc <- list_dir(root),
        sh <- list_dir(Path.join(root, rc)),
        partition_key <- partition_keys_under({rc, sh, ""}),
        uniq: true,
        do: {rc, sh, partition_key}
  end

  @doc """
  Snapshot files for a single Partition, sorted by seq ascending.
  """
  @spec snapshot_files(partition_id()) :: [{seq :: non_neg_integer(), path :: binary()}]
  def snapshot_files(pid), do: snapshot_paths(pid)

  @spec encode_partition_key(binary()) :: binary()
  def encode_partition_key(key) do
    if Regex.match?(@safe_chars, key) and not String.starts_with?(key, "_") do
      key
    else
      "_" <> Base.url_encode64(key, padding: false)
    end
  end

  @spec decode_partition_key(binary()) :: binary()
  def decode_partition_key("_" <> encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, decoded} -> decoded
      :error -> "_" <> encoded
    end
  end

  def decode_partition_key(key), do: key

  defp parse_snapshot_name(name) do
    case Regex.run(~r/^(.+)\.snap\.(\d+)$/, name) do
      [_, encoded_key, seq_str] ->
        case Integer.parse(seq_str) do
          {seq, ""} -> {:ok, encoded_key, seq}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp partition_keys_under({_, _, _} = pid) do
    %{wal_files: w, snapshot_files: s, outbox_files: o} = scan_dir(pid)
    keys = Enum.map(w, &elem(&1, 0)) ++ Enum.map(s, &elem(&1, 0)) ++ Enum.map(o, &elem(&1, 0))
    Enum.uniq(keys)
  end

  defp list_dir(dir) do
    case File.ls(dir) do
      {:ok, names} -> names
      {:error, _} -> []
    end
  end
end
