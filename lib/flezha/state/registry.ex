defmodule Flezha.State.Registry do
  @moduledoc """
  Path resolution for per-Segment WAL, snapshot, and outbox-dedup files.

  All persistence files live under
  `<state_root>/<operative_class>/<operation_id>/` where `<state_root>` is the
  `:flezha, :state_root` application env (set by tests for isolation), else
  the `FLEZHA_STATE_DIR` OS env (the operator-facing deployment knob), else
  `app_data/state`.

  `segment_key` is URL-safe-base64-encoded when it contains characters
  that would be unsafe on POSIX filesystems (anything outside `[A-Za-z0-9_.-]`).
  `decode_segment_key/1` reverses the encoding.
  """

  @type segment_id ::
          {operative_class :: binary(), operation_id :: binary(), segment_key :: binary()}

  @type scan_result :: %{
          wal_files: [{segment_key :: binary(), path :: binary()}],
          snapshot_files: [
            {segment_key :: binary(), seq :: non_neg_integer(), path :: binary()}
          ],
          outbox_files: [{segment_key :: binary(), path :: binary()}]
        }

  @safe_chars ~r/^[A-Za-z0-9_.-]+$/

  @spec state_root() :: binary()
  def state_root() do
    case Application.fetch_env(:flezha, :state_root) do
      {:ok, root} -> root
      :error -> System.get_env("FLEZHA_STATE_DIR") || "app_data/state"
    end
  end

  @spec state_dir(segment_id()) :: binary()
  def state_dir({operative_class, operation_id, _segment_key}) do
    Path.join([state_root(), operative_class, operation_id])
  end

  @spec wal_path(segment_id()) :: binary()
  def wal_path({_, _, segment_key} = pid) do
    Path.join(state_dir(pid), encode_segment_key(segment_key) <> ".wal")
  end

  @spec snapshot_paths(segment_id()) :: [{non_neg_integer(), binary()}]
  def snapshot_paths({_, _, segment_key} = pid) do
    encoded = encode_segment_key(segment_key)
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

  @spec outbox_dedup_path(segment_id()) :: binary()
  def outbox_dedup_path({_, _, segment_key} = pid) do
    Path.join(state_dir(pid), encode_segment_key(segment_key) <> ".outbox.dedup")
  end

  @doc """
  Path of a per-Segment saga compensation marker (`bounded-operative-saga`
  §4.3). `:intent` marks that the Segment's operation completed forward and may
  need undo; `:done` marks that its compensation already ran. Presence is the
  signal (empty file). Inert to `scan_dir/1` (unknown suffix), so a marker never
  registers a phantom Segment; reclaimed with the operative-class dir on
  retention sweep.
  """
  @spec compensation_marker_path(segment_id(), :intent | :done) :: binary()
  def compensation_marker_path({_, _, segment_key} = pid, kind) when kind in [:intent, :done] do
    suffix = if kind == :intent, do: ".comp_intent", else: ".comp_done"
    Path.join(state_dir(pid), encode_segment_key(segment_key) <> suffix)
  end

  @doc """
  Write one compensation marker for a Segment (presence is the signal — empty
  body). Best-effort; returns the `File.write/2` result so the caller can log.
  """
  @spec write_compensation_marker(segment_id(), :intent | :done) :: :ok | {:error, term()}
  def write_compensation_marker({_, _, _} = pid, kind) when kind in [:intent, :done] do
    path = compensation_marker_path(pid, kind)
    File.mkdir_p(Path.dirname(path))
    File.write(path, "")
  end

  @doc """
  Read a Segment's saga compensation markers, shipped back on `RestoreInitState`
  so a respawned durable bounded driver finishes the remaining hooks: `intent` ⇒
  its operation completed forward and may need undo; `done` ⇒ its compensation
  already ran (skip it).
  """
  @spec compensation_markers(segment_id()) :: %{intent: boolean(), done: boolean()}
  def compensation_markers({_, _, _} = pid) do
    %{
      intent: File.exists?(compensation_marker_path(pid, :intent)),
      done: File.exists?(compensation_marker_path(pid, :done))
    }
  end

  @doc """
  Remove every on-disk file for one Segment (WAL, all snapshots, the
  outbox-dedup file). Idempotent: missing files are ignored. Used by the
  dynamic-mode hard-delete path (`Flezha.State.Supervisor.delete_segment/1`).
  """
  @spec delete_files(segment_id()) :: :ok
  def delete_files({_, _, _} = pid) do
    [
      wal_path(pid),
      outbox_dedup_path(pid),
      compensation_marker_path(pid, :intent),
      compensation_marker_path(pid, :done)
    ]
    |> Enum.concat(Enum.map(snapshot_paths(pid), fn {_seq, path} -> path end))
    |> Enum.each(&File.rm/1)

    :ok
  end

  @spec snapshot_path(segment_id(), non_neg_integer()) :: binary()
  def snapshot_path({_, _, segment_key} = pid, seq) do
    Path.join(
      state_dir(pid),
      encode_segment_key(segment_key) <> ".snap." <> Integer.to_string(seq)
    )
  end

  @doc """
  Enumerate Segment files under a Operative tree.

  Pass `{operative_class, operation_id, ""}` to scan the whole Operation. Returns lists
  of `{segment_key, ...}` tuples grouped by file type. Files matching
  `.tmp` / `.rewriting` suffixes are intentionally skipped — they
  represent crash-residue from atomic-rename workflows.
  """
  @spec scan_dir(segment_id()) :: scan_result()
  def scan_dir({_, _, _} = pid) do
    dir = state_dir(pid)

    dir
    |> list_dir()
    |> Enum.reduce(%{wal_files: [], snapshot_files: [], outbox_files: []}, fn name, acc ->
      cond do
        String.ends_with?(name, ".tmp") or String.ends_with?(name, ".rewriting") ->
          acc

        String.ends_with?(name, ".wal") ->
          key = name |> String.trim_trailing(".wal") |> decode_segment_key()
          %{acc | wal_files: [{key, Path.join(dir, name)} | acc.wal_files]}

        String.ends_with?(name, ".outbox.dedup") ->
          key = name |> String.trim_trailing(".outbox.dedup") |> decode_segment_key()
          %{acc | outbox_files: [{key, Path.join(dir, name)} | acc.outbox_files]}

        true ->
          case parse_snapshot_name(name) do
            {:ok, encoded_key, seq} ->
              key = decode_segment_key(encoded_key)

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
  Enumerate every Segment that has any persistence file on disk under
  `state_root()`. Walks the two-level directory tree
  `<state_root>/<operative_class>/<operation_id>/`.
  """
  @spec all_segments() :: [segment_id()]
  def all_segments() do
    root = state_root()

    for rc <- list_dir(root),
        sh <- list_dir(Path.join(root, rc)),
        segment_key <- segment_keys_under({rc, sh, ""}),
        uniq: true,
        do: {rc, sh, segment_key}
  end

  @doc """
  Snapshot files for a single Segment, sorted by seq ascending.
  """
  @spec snapshot_files(segment_id()) :: [{seq :: non_neg_integer(), path :: binary()}]
  def snapshot_files(pid), do: snapshot_paths(pid)

  @spec encode_segment_key(binary()) :: binary()
  def encode_segment_key(key) do
    if Regex.match?(@safe_chars, key) and not String.starts_with?(key, "_") do
      key
    else
      "_" <> Base.url_encode64(key, padding: false)
    end
  end

  @spec decode_segment_key(binary()) :: binary()
  def decode_segment_key("_" <> encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, decoded} -> decoded
      :error -> "_" <> encoded
    end
  end

  def decode_segment_key(key), do: key

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

  defp segment_keys_under({_, _, _} = pid) do
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
