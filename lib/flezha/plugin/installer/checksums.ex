defmodule Flezha.Plugin.Installer.Checksums do
  @moduledoc """
  SHA-256 checksum parsing and verification against an extracted package
  tree.

  The `CHECKSUMS.sha256` format matches GNU `sha256sum`: one
  `<hex>  <relative_path>` entry per line. Blank lines and `#` comments are
  tolerated. The installer requires coverage to be exact: every file under
  `src/` plus `manifest.exs` must be listed, and every listed file must
  exist and hash-match.
  """

  @type entry :: %{digest: binary(), path: binary()}

  @doc """
  Parses the text body of a `CHECKSUMS.sha256` file. Returns a list of
  `%{digest: hex, path: rel}` on success, or `{:error, {:malformed_line, n, line}}`
  on the first bad line.
  """
  @spec parse(binary()) :: {:ok, [entry()]} | {:error, {:malformed_line, pos_integer(), binary()}}
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
  Verifies every entry in `entries` against the filesystem rooted at
  `staging_dir`. Returns `:ok` when every declared file exists and its
  recomputed SHA-256 matches. Returns the first error encountered.

  Also verifies coverage: any file present under `src/` or at
  `manifest.exs` that is NOT listed in `entries` is reported as
  `{:error, {:checksum_line_missing, path}}`.
  """
  @spec verify([entry()], Path.t()) ::
          :ok
          | {:error,
             {:checksum_mismatch, binary()}
             | {:checksum_file_missing, binary()}
             | {:checksum_line_missing, binary()}}
  def verify(entries, staging_dir) do
    declared = MapSet.new(entries, & &1.path)

    with :ok <- verify_entries(entries, staging_dir),
         :ok <- verify_coverage(declared, staging_dir) do
      :ok
    end
  end

  ## Private

  defp parse_line(line, n, acc) do
    case String.split(line, ~r/\s+/, parts: 2) do
      [hex, path] when byte_size(hex) == 64 ->
        if valid_hex?(hex) do
          {:cont, {:ok, [%{digest: String.downcase(hex), path: path} | acc]}}
        else
          {:halt, {:error, {:malformed_line, n, line}}}
        end

      _ ->
        {:halt, {:error, {:malformed_line, n, line}}}
    end
  end

  defp valid_hex?(<<>>), do: true
  defp valid_hex?(<<c, rest::binary>>) when c in ?0..?9, do: valid_hex?(rest)
  defp valid_hex?(<<c, rest::binary>>) when c in ?a..?f, do: valid_hex?(rest)
  defp valid_hex?(<<c, rest::binary>>) when c in ?A..?F, do: valid_hex?(rest)
  defp valid_hex?(_), do: false

  defp verify_entries(entries, staging_dir) do
    Enum.reduce_while(entries, :ok, fn %{digest: expected, path: rel}, _ ->
      full = Path.join(staging_dir, rel)

      cond do
        not File.regular?(full) ->
          {:halt, {:error, {:checksum_file_missing, rel}}}

        true ->
          case file_digest(full) do
            {:ok, actual} when actual == expected -> {:cont, :ok}
            {:ok, _actual} -> {:halt, {:error, {:checksum_mismatch, rel}}}
            {:error, reason} -> {:halt, {:error, {:checksum_io_error, rel, reason}}}
          end
      end
    end)
  end

  defp verify_coverage(declared, staging_dir) do
    actual = collect_covered_paths(staging_dir)

    case Enum.find(actual, fn rel -> not MapSet.member?(declared, rel) end) do
      nil -> :ok
      missing -> {:error, {:checksum_line_missing, missing}}
    end
  end

  defp collect_covered_paths(staging_dir) do
    manifest = if File.regular?(Path.join(staging_dir, "manifest.exs")), do: ["manifest.exs"], else: []
    src_root = Path.join(staging_dir, "src")

    src =
      if File.dir?(src_root) do
        walk_regular(src_root)
        |> Enum.map(&Path.relative_to(&1, staging_dir))
      else
        []
      end

    manifest ++ src
  end

  defp walk_regular(dir) do
    File.ls!(dir)
    |> Enum.flat_map(fn entry ->
      full = Path.join(dir, entry)

      cond do
        File.dir?(full) -> walk_regular(full)
        File.regular?(full) -> [full]
        true -> []
      end
    end)
  end

  defp file_digest(path) do
    case File.read(path) do
      {:ok, data} -> {:ok, :crypto.hash(:sha256, data) |> Base.encode16(case: :lower)}
      {:error, reason} -> {:error, reason}
    end
  end
end
