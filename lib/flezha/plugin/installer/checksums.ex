defmodule Flezha.Plugin.Installer.Checksums do
  @moduledoc """
  `CHECKSUMS.sha256` of a plugin package, in GNU `sha256sum` format: one
  `<hex>  <relative_path>` line per file. Coverage is exact: `manifest.exs` and
  every file under `lib/` has a line, and every line names a file whose digest
  matches.
  """

  alias Flezha.Plugin.Installer.Package

  @file_name "CHECKSUMS.sha256"

  @type entry :: %{digest: binary(), path: binary()}

  @doc "Check the `CHECKSUMS.sha256` of the package extracted in `dir`."
  @spec check(Path.t()) :: :ok | {:error, term()}
  def check(dir) do
    with {:ok, entries} <- parse(File.read!(Path.join(dir, @file_name))) do
      verify(entries, dir)
    end
  end

  @doc """
  Write the `CHECKSUMS.sha256` covering `manifest.exs` and `lib/` under `dir`,
  and answer its bytes, which a signature signs.
  """
  @spec write(Path.t()) :: binary()
  def write(dir) do
    body = for rel <- covered(dir), into: "", do: "#{file_digest(Path.join(dir, rel))}  #{rel}\n"
    File.write!(Path.join(dir, @file_name), body)
    body
  end

  @doc """
  Parse the text of a `CHECKSUMS.sha256` file. Blank lines and `#` comments are
  skipped.
  """
  @spec parse(binary()) :: {:ok, [entry()]} | {:error, term()}
  def parse(body) when is_binary(body) do
    body
    |> String.split("\n")
    |> Enum.with_index(1)
    |> Enum.reduce_while({:ok, []}, fn {line, n}, {:ok, acc} ->
      trimmed = String.trim(line)

      cond do
        trimmed == "" -> {:cont, {:ok, acc}}
        String.starts_with?(trimmed, "#") -> {:cont, {:ok, acc}}
        true -> parse_line(trimmed, n, acc)
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      other -> other
    end
  end

  @doc """
  Every entry's file exists under `dir` and hashes to its digest, and every
  covered file has an entry.
  """
  @spec verify([entry()], Path.t()) :: :ok | {:error, term()}
  def verify(entries, dir) do
    declared = MapSet.new(entries, & &1.path)

    with :ok <- Enum.find_value(entries, :ok, &entry_problem(&1, dir)) do
      case Enum.find(covered(dir), &(not MapSet.member?(declared, &1))) do
        nil -> :ok
        path -> invalid(:checksum_line_missing, path)
      end
    end
  end

  defp parse_line(line, n, acc) do
    with [hex, path] <- String.split(line, ~r/\s+/, parts: 2),
         {:ok, _} <- Base.decode16(hex, case: :mixed),
         64 <- byte_size(hex) do
      {:cont, {:ok, [%{digest: String.downcase(hex), path: path} | acc]}}
    else
      _ -> {:halt, {:error, {:package_invalid, :checksums_malformed, %{line: n}}}}
    end
  end

  defp entry_problem(%{digest: expected, path: rel}, dir) do
    full = Path.join(dir, rel)

    cond do
      Path.type(rel) != :relative or ".." in Path.split(rel) -> invalid(:unsafe_entry, rel)
      not File.regular?(full) -> invalid(:checksum_file_missing, rel)
      file_digest(full) != expected -> invalid(:checksum_mismatch, rel)
      true -> nil
    end
  end

  defp covered(dir) do
    lib = dir |> Path.join("lib") |> Package.files() |> Enum.map(&Path.relative_to(&1, dir))
    ["manifest.exs" | Enum.sort(lib)]
  end

  defp file_digest(path),
    do: :crypto.hash(:sha256, File.read!(path)) |> Base.encode16(case: :lower)

  defp invalid(problem, path), do: {:error, {:package_invalid, problem, %{path: path}}}
end
