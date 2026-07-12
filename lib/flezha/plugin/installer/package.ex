defmodule Flezha.Plugin.Installer.Package do
  @moduledoc """
  Archive extraction + layout validation for `.fpz` plugin packages.

  The archive is a gzipped tar with this canonical root:

      manifest.exs
      src/<any tree of .ex/.erl>
      CHECKSUMS.sha256
      signature.sig?            # optional

  Nothing else is allowed at the root — no `.beam`, no symbolic links, no
  `..` traversal, no device or setuid files. We reject at extraction time
  so staging never ends up with a file we're about to refuse later.
  """

  @allowed_roots ~w(manifest.exs src CHECKSUMS.sha256 signature.sig)

  @type extract_error ::
          {:package_too_large, pos_integer(), pos_integer()}
          | {:too_many_source_files, pos_integer(), pos_integer()}
          | {:unsafe_entry, binary(), atom()}
          | {:tar_failed, term()}
          | {:invalid_layout, atom() | {atom(), any()}}

  @doc """
  Extracts `fpz_path` into `staging_dir`, enforcing size limits and rejecting
  unsafe entries up front. Returns `:ok` on success; on failure returns
  `{:error, reason}` and leaves `staging_dir` empty (caller-owned cleanup).
  """
  @spec extract(Path.t(), Path.t(), keyword()) :: :ok | {:error, extract_error()}
  def extract(fpz_path, staging_dir, opts \\ []) do
    max_bytes = Keyword.fetch!(opts, :max_package_bytes)
    max_files = Keyword.fetch!(opts, :max_source_files)

    with {:ok, entries} <- list_entries(fpz_path),
         :ok <- enforce_size_limits(entries, max_bytes, max_files),
         :ok <- reject_unsafe_entries(entries),
         :ok <- do_extract(fpz_path, staging_dir) do
      :ok
    end
  end

  @doc """
  Enforce that `staging_dir` contains exactly `manifest.exs`, a non-empty
  `src/` directory, `CHECKSUMS.sha256`, and optionally `signature.sig` at
  the root — nothing else.
  """
  @spec validate_layout(Path.t()) :: :ok | {:error, {:invalid_layout, term()}}
  def validate_layout(staging_dir) do
    with {:ok, entries} <- list_root(staging_dir),
         :ok <- check_required(staging_dir, entries),
         :ok <- check_no_unknown(entries),
         :ok <- check_src_nonempty(Path.join(staging_dir, "src")),
         :ok <- check_src_files(Path.join(staging_dir, "src")) do
      :ok
    end
  end

  @doc """
  Enforces `max_package_bytes` (sum of uncompressed sizes) and
  `max_source_files` (count of `src/**`) over a list of tar entries as
  returned by `:erl_tar.table/2`.
  """
  @spec enforce_size_limits([tuple()], pos_integer(), pos_integer()) ::
          :ok | {:error, extract_error()}
  def enforce_size_limits(entries, max_bytes, max_files) do
    total_bytes =
      Enum.reduce(entries, 0, fn entry, acc -> acc + entry_size(entry) end)

    src_file_count =
      Enum.count(entries, fn entry ->
        name = entry_name(entry)
        String.starts_with?(name, "src/") and entry_kind(entry) == :regular
      end)

    cond do
      total_bytes > max_bytes ->
        {:error, {:package_too_large, total_bytes, max_bytes}}

      src_file_count > max_files ->
        {:error, {:too_many_source_files, src_file_count, max_files}}

      true ->
        :ok
    end
  end

  ## Private

  defp list_entries(fpz_path) do
    case :erl_tar.table(String.to_charlist(fpz_path), [:compressed, :verbose]) do
      {:ok, entries} -> {:ok, entries}
      {:error, reason} -> {:error, {:tar_failed, reason}}
    end
  end

  defp reject_unsafe_entries(entries) do
    Enum.reduce_while(entries, :ok, fn entry, _ ->
      name = entry_name(entry)
      kind = entry_kind(entry)

      cond do
        String.contains?(name, "..") ->
          {:halt, {:error, {:unsafe_entry, name, :path_traversal}}}

        String.starts_with?(name, "/") ->
          {:halt, {:error, {:unsafe_entry, name, :absolute_path}}}

        kind == :symlink ->
          {:halt, {:error, {:unsafe_entry, name, :symlink}}}

        kind == :device ->
          {:halt, {:error, {:unsafe_entry, name, :device}}}

        kind == :other ->
          {:halt, {:error, {:unsafe_entry, name, :unsupported_type}}}

        String.ends_with?(name, ".beam") ->
          {:halt, {:error, {:invalid_layout, :beam_forbidden}}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp do_extract(fpz_path, staging_dir) do
    :ok = File.mkdir_p!(staging_dir)

    case :erl_tar.extract(String.to_charlist(fpz_path), [
           :compressed,
           {:cwd, String.to_charlist(staging_dir)}
         ]) do
      :ok -> :ok
      {:error, reason} -> {:error, {:tar_failed, reason}}
    end
  end

  defp list_root(staging_dir) do
    case File.ls(staging_dir) do
      {:ok, entries} -> {:ok, entries}
      {:error, reason} -> {:error, {:invalid_layout, {:cannot_list, reason}}}
    end
  end

  defp check_required(staging_dir, entries) do
    cond do
      "manifest.exs" not in entries or
          not File.regular?(Path.join(staging_dir, "manifest.exs")) ->
        {:error, {:invalid_layout, :manifest_missing}}

      "CHECKSUMS.sha256" not in entries or
          not File.regular?(Path.join(staging_dir, "CHECKSUMS.sha256")) ->
        {:error, {:invalid_layout, :checksums_missing}}

      "src" not in entries or not File.dir?(Path.join(staging_dir, "src")) ->
        {:error, {:invalid_layout, :src_missing}}

      true ->
        :ok
    end
  end

  defp check_no_unknown(entries) do
    unexpected = Enum.reject(entries, &(&1 in @allowed_roots))

    case unexpected do
      [] -> :ok
      [first | _] -> {:error, {:invalid_layout, {:unexpected_entry, first}}}
    end
  end

  defp check_src_nonempty(src_dir) do
    case File.ls(src_dir) do
      {:ok, [_ | _]} -> :ok
      {:ok, []} -> {:error, {:invalid_layout, :src_empty}}
      {:error, reason} -> {:error, {:invalid_layout, {:src_unreadable, reason}}}
    end
  end

  # Walk every regular file under src/ and refuse anything that isn't .ex or
  # .erl (no `.beam`, no binaries, no stray build artifacts).
  defp check_src_files(src_dir) do
    Enum.reduce_while(collect_files(src_dir, src_dir), :ok, fn rel, _ ->
      cond do
        String.contains?(rel, "..") ->
          {:halt, {:error, {:invalid_layout, {:path_traversal, rel}}}}

        String.ends_with?(rel, ".beam") ->
          {:halt, {:error, {:invalid_layout, :beam_forbidden}}}

        String.ends_with?(rel, ".ex") or String.ends_with?(rel, ".erl") ->
          {:cont, :ok}

        true ->
          {:halt, {:error, {:invalid_layout, {:unexpected_source, rel}}}}
      end
    end)
  end

  defp collect_files(root, dir) do
    File.ls!(dir)
    |> Enum.flat_map(fn entry ->
      full = Path.join(dir, entry)

      cond do
        File.dir?(full) -> collect_files(root, full)
        File.regular?(full) -> [Path.relative_to(full, root)]
        true -> [Path.relative_to(full, root)]
      end
    end)
  end

  # :erl_tar.table/2 with :verbose returns {name, type, size, mtime, mode, uid, gid}.
  # Without :verbose it returns just the name. We always pass :verbose so
  # matching is deterministic.
  defp entry_name({name, _type, _size, _mtime, _mode, _uid, _gid}) when is_list(name),
    do: List.to_string(name)

  defp entry_name(name) when is_list(name), do: List.to_string(name)
  defp entry_name(name) when is_binary(name), do: name

  defp entry_kind({_name, type, _size, _mtime, _mode, _uid, _gid}), do: type
  defp entry_kind(_), do: :regular

  defp entry_size({_name, _type, size, _mtime, _mode, _uid, _gid}) when is_integer(size),
    do: size

  defp entry_size(_), do: 0
end
