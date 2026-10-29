defmodule Flezha.Logs.Index do
  @moduledoc """
  The on-disk catalog of one kind's rotated files.

  The file `INDEX` under the kind's directory is `:erlang.term_to_binary/1`
  of a list of `entry()` maps, rewritten atomically on every rotation. On boot,
  if it is missing or fails to decode, the catalog is rebuilt by reading every
  rotated file.

  An entry's `first_ts_us` and `last_ts_us` are wall-clock when it carries
  `clock: :wall`; an entry without it was catalogued before records carried
  wall-clock times, and its range orders nothing. A rebuilt entry is marked
  `:wall`, its range read through `Flezha.Logs.Codec.order_us/1`. Entries are
  sorted oldest first.
  """

  require Logger

  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Logs.{Kind, Reader}
  alias Flezha.State.Paths

  @index_filename "INDEX"

  @type entry :: %{
          required(:filename) => String.t(),
          required(:first_ts_us) => integer() | nil,
          required(:last_ts_us) => integer() | nil,
          required(:record_count) => non_neg_integer(),
          required(:size_bytes) => non_neg_integer(),
          required(:sha256) => String.t() | nil,
          optional(:clock) => :wall
        }

  @doc "Absolute path to the INDEX file under `dir`."
  @spec path(String.t()) :: String.t()
  def path(dir), do: Path.join(dir, @index_filename)

  @doc "Load the catalog. Returns `{:ok, [entry]}` or `{:error, reason}`."
  @spec load(String.t()) :: {:ok, [entry()]} | {:error, term()}
  def load(dir) when is_binary(dir) do
    with {:ok, blob} <- File.read(path(dir)),
         {:ok, list} when is_list(list) <- safe_decode(blob) do
      {:ok, list}
    else
      {:ok, _not_a_list} -> {:error, :corrupt_index}
      {:error, _} = err -> err
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
    Paths.atomic_install(path(dir), &:file.write(&1, bin), :log_index)
  end

  @doc """
  Rebuild `kind`'s catalog by reading every rotated file in `dir`, then write
  it atomically.
  """
  @spec rebuild(Kind.t(), String.t()) :: {:ok, [entry()]} | {:error, term()}
  def rebuild(%Kind{} = kind, dir) when is_binary(dir) do
    :ok = Layout.claim(kind.layout, dir)

    entries =
      kind
      |> list_rotated_files(dir)
      |> Enum.map(&rebuild_entry(dir, &1))
      |> Enum.reject(&is_nil/1)
      |> Enum.sort_by(&(&1.first_ts_us || 0))

    with :ok <- write(dir, entries), do: {:ok, entries}
  end

  @doc "`kind`'s rotated files in `dir`, by name; the active file is not one."
  @spec list_rotated_files(Kind.t(), String.t()) :: [String.t()]
  def list_rotated_files(%Kind{} = kind, dir) do
    case File.ls(dir) do
      {:ok, names} -> names |> Enum.filter(&Kind.rotated?(kind, &1)) |> Enum.sort()
      {:error, _} -> []
    end
  end

  defp rebuild_entry(dir, filename) do
    path = Path.join(dir, filename)

    case File.stat(path) do
      {:ok, %File.Stat{size: size}} ->
        {first_ts, last_ts, count} = Reader.head_metadata(path)

        %{
          filename: filename,
          first_ts_us: first_ts,
          last_ts_us: last_ts,
          record_count: count,
          size_bytes: size,
          sha256: sha256_of_file(path),
          clock: :wall
        }

      {:error, reason} ->
        Logger.warning("[logs.index] could not stat #{filename}: #{inspect(reason)}; skipping")
        nil
    end
  end

  defp sha256_of_file(path) do
    File.stream!(path, 65_536)
    |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
    |> :crypto.hash_final()
    |> Base.encode16(case: :lower)
  rescue
    _ -> nil
  end

  defp safe_decode(blob) do
    {:ok, :erlang.binary_to_term(blob, [:safe])}
  rescue
    _ -> {:error, :corrupt_index}
  end
end
