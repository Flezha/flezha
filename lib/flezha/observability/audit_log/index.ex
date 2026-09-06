defmodule Flezha.Observability.AuditLog.Index do
  @moduledoc """
  On-disk catalog of rotated audit files.

  The file `INDEX` under the audit dir is `:erlang.term_to_binary/1`
  of a list of `entry()` tuples. It is rewritten atomically
  (`.tmp + rename`) on every rotation. On boot, if the file is
  missing or fails to decode, the catalog is rebuilt by walking the
  audit directory and reading every rotated `audit-*.jsonl[.gz]`
  file.

  Entries are sorted by `:first_ts_us` ascending so `flz audit list`
  shows oldest-first.
  """

  require Logger

  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Observability.AuditLog.Reader
  alias Flezha.State.Paths

  @index_filename "INDEX"

  @type entry :: %{
          filename: String.t(),
          first_ts_us: integer() | nil,
          last_ts_us: integer() | nil,
          record_count: non_neg_integer(),
          size_bytes: non_neg_integer(),
          sha256: String.t() | nil
        }

  @doc "Absolute path to the INDEX file under `dir`."
  @spec path(String.t()) :: String.t()
  def path(dir), do: Path.join(dir, @index_filename)

  @doc """
  Load the catalog. Returns `{:ok, [entry]}` or `{:error, reason}`.
  """
  @spec load(String.t()) :: {:ok, [entry()]} | {:error, term()}
  def load(dir) when is_binary(dir) do
    file = path(dir)

    with {:ok, blob} <- File.read(file),
         {:ok, term} <- safe_decode(blob),
         {:ok, list} <- validate_list(term) do
      {:ok, list}
    end
  end

  @doc """
  Write the catalog atomically through `Flezha.State.Paths.atomic_install/3`:
  the bytes land under the declared in-progress suffix, are fsynced and then
  renamed over `INDEX`, so a crash mid-write never leaves a partial file as the
  current catalog.
  """
  @spec write(String.t(), [entry()]) :: :ok | {:error, term()}
  def write(dir, entries) when is_binary(dir) and is_list(entries) do
    bin = :erlang.term_to_binary(entries)
    Paths.atomic_install(path(dir), &:file.write(&1, bin), :audit_index)
  end

  @doc """
  Rebuild the catalog by walking `dir` and reading every rotated
  audit file. Skips the active file (`audit-current.jsonl`). After
  the walk the result is written atomically.
  """
  @spec rebuild(String.t()) :: {:ok, [entry()]} | {:error, term()}
  def rebuild(dir) when is_binary(dir) do
    :ok = Layout.claim(:audit, dir)

    entries =
      dir
      |> list_rotated_files()
      |> Enum.map(fn filename -> rebuild_entry(dir, filename) end)
      |> Enum.reject(&is_nil/1)
      |> Enum.sort_by(fn e -> e.first_ts_us || 0 end)

    case write(dir, entries) do
      :ok -> {:ok, entries}
      {:error, _} = err -> err
    end
  end

  @doc """
  Returns a list of rotated filenames (relative to `dir`), excluding
  the active file and any other non-rotation files.
  """
  @spec list_rotated_files(String.t()) :: [String.t()]
  def list_rotated_files(dir) do
    case File.ls(dir) do
      {:ok, names} ->
        names
        |> Enum.filter(&rotated?/1)
        |> Enum.sort()

      {:error, _} ->
        []
    end
  end

  @doc "True for files matching `audit-YYYYMMDDTHHMMSSZ-N.jsonl[.gz]`."
  @spec rotated?(String.t()) :: boolean()
  def rotated?(name) when is_binary(name) do
    String.starts_with?(name, "audit-") and
      name != "audit-current.jsonl" and
      (String.ends_with?(name, ".jsonl") or String.ends_with?(name, ".jsonl.gz"))
  end

  defp rebuild_entry(dir, filename) do
    path = Path.join(dir, filename)

    case File.stat(path) do
      {:ok, %File.Stat{size: size}} ->
        {first_ts, last_ts, count} = scan_metadata(path)
        sha = sha256_of_file(path)

        %{
          filename: filename,
          first_ts_us: first_ts,
          last_ts_us: last_ts,
          record_count: count,
          size_bytes: size,
          sha256: sha
        }

      {:error, reason} ->
        Logger.warning(
          "[audit_log.index] could not stat #{filename}: #{inspect(reason)} — skipping"
        )

        nil
    end
  end

  defp scan_metadata(path) do
    Reader.stream(path, [])
    |> Enum.reduce({nil, nil, 0}, fn record, {first, _last, count} ->
      ts = Map.get(record, "ts_us")
      first = if is_nil(first), do: ts, else: first
      {first, ts, count + 1}
    end)
  rescue
    _ -> {nil, nil, 0}
  end

  defp sha256_of_file(path) do
    case File.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          ctx = :crypto.hash_init(:sha256)
          ctx = hash_loop(fd, ctx)
          :crypto.hash_final(ctx) |> Base.encode16(case: :lower)
        after
          :file.close(fd)
        end

      {:error, _} ->
        nil
    end
  end

  defp hash_loop(fd, ctx) do
    case :file.read(fd, 65_536) do
      {:ok, chunk} -> hash_loop(fd, :crypto.hash_update(ctx, chunk))
      :eof -> ctx
      {:error, _} -> ctx
    end
  end

  defp safe_decode(blob) do
    try do
      {:ok, :erlang.binary_to_term(blob, [:safe])}
    rescue
      _ -> {:error, :corrupt_index}
    catch
      _, _ -> {:error, :corrupt_index}
    end
  end

  defp validate_list(list) when is_list(list), do: {:ok, list}
  defp validate_list(_), do: {:error, :corrupt_index}
end
