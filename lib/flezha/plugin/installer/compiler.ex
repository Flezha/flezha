defmodule Flezha.Plugin.Installer.Compiler do
  @moduledoc """
  Compile every `.ex` / `.erl` source under an extracted package's `src/`
  into in-memory BEAM binaries.

  Compilation happens at *install* time, not boot time. Warnings are
  treated as errors so bad plugins surface at the moment the operator is
  paying attention. The set of produced modules must exactly match the
  manifest's `:modules` list — any discrepancy aborts the install.

  Neither the installer nor the Loader writes files here; that is the
  installer's job after it accepts the compile output.
  """

  require Logger

  @type beam :: {module(), binary()}
  @type compile_error ::
          {:compile_failed, Path.t(), term()}
          | {:manifest_invalid, {:module_mismatch, Keyword.t()}}

  @doc """
  Walks `src_dir` for `.ex` and `.erl` files, compiles them all, and
  returns `{:ok, [{module, beam}]}` or `{:error, ...}`.

  When `expected_modules` is supplied, the produced set is cross-checked
  against it and mismatches abort.
  """
  @spec compile(Path.t(), [atom()] | nil) :: {:ok, [beam()]} | {:error, compile_error()}
  def compile(src_dir, expected_modules \\ nil) do
    with {:ok, beams} <- compile_all(src_dir),
         :ok <- check_modules(beams, expected_modules) do
      {:ok, beams}
    end
  end

  ## Private

  defp compile_all(src_dir) do
    {ex_files, erl_files} = collect_sources(src_dir)

    with {:ok, ex_beams} <- compile_ex(ex_files),
         {:ok, erl_beams} <- compile_erl(erl_files) do
      {:ok, ex_beams ++ erl_beams}
    end
  end

  defp collect_sources(src_dir) do
    files =
      if File.dir?(src_dir) do
        walk(src_dir)
      else
        []
      end

    Enum.reduce(files, {[], []}, fn path, {ex, erl} ->
      cond do
        String.ends_with?(path, ".ex") -> {[path | ex], erl}
        String.ends_with?(path, ".erl") -> {ex, [path | erl]}
        true -> {ex, erl}
      end
    end)
    |> then(fn {ex, erl} -> {Enum.sort(ex), Enum.sort(erl)} end)
  end

  defp walk(dir) do
    File.ls!(dir)
    |> Enum.flat_map(fn entry ->
      full = Path.join(dir, entry)

      cond do
        File.dir?(full) -> walk(full)
        File.regular?(full) -> [full]
        true -> []
      end
    end)
  end

  defp compile_ex(files) do
    Enum.reduce_while(files, {:ok, []}, fn file, {:ok, acc} ->
      try do
        {produced, diagnostics} =
          Code.with_diagnostics(fn -> Code.compile_file(file) end)

        warnings = Enum.filter(diagnostics, fn d -> Map.get(d, :severity) == :warning end)

        cond do
          produced == [] ->
            {:halt, {:error, {:compile_failed, file, :no_modules_produced}}}

          warnings != [] ->
            # Purge what we just loaded before returning error; the Loader
            # owns module installation, not us.
            Enum.each(produced, fn {mod, _} -> purge(mod) end)
            msg = Enum.map(warnings, &Map.get(&1, :message, "")) |> Enum.join("\n")
            {:halt, {:error, {:compile_failed, file, {:warning, msg}}}}

          true ->
            # Code.compile_file/1 loads modules as a side effect. Purge
            # them so the Loader's collision check (which consults the
            # code server) does not trip on our own just-compiled beams.
            Enum.each(produced, fn {mod, _} -> purge(mod) end)
            {:cont, {:ok, produced ++ acc}}
        end
      rescue
        e in CompileError -> {:halt, {:error, {:compile_failed, file, Exception.message(e)}}}
        e -> {:halt, {:error, {:compile_failed, file, Exception.message(e)}}}
      catch
        kind, reason -> {:halt, {:error, {:compile_failed, file, {kind, reason}}}}
      end
    end)
  end

  defp purge(module) do
    _ = :code.purge(module)
    _ = :code.delete(module)
    _ = :code.purge(module)
    :ok
  end

  defp compile_erl(files) do
    Enum.reduce_while(files, {:ok, []}, fn file, {:ok, acc} ->
      opts = [
        :binary,
        :report_errors,
        :report_warnings,
        :warnings_as_errors,
        :return_errors,
        :return_warnings
      ]

      case :compile.file(String.to_charlist(file), opts) do
        {:ok, module_name, binary} when is_binary(binary) ->
          {:cont, {:ok, [{module_name, binary} | acc]}}

        {:ok, module_name, binary, _warnings} when is_binary(binary) ->
          {:cont, {:ok, [{module_name, binary} | acc]}}

        {:error, errors, _warnings} ->
          {:halt, {:error, {:compile_failed, file, errors}}}

        :error ->
          {:halt, {:error, {:compile_failed, file, :erlang_compile_failed}}}

        other ->
          {:halt, {:error, {:compile_failed, file, other}}}
      end
    end)
  end

  defp check_modules(_beams, nil), do: :ok

  defp check_modules(beams, expected) when is_list(expected) do
    produced = Enum.map(beams, fn {mod, _} -> mod end) |> Enum.sort()
    expected_sorted = Enum.sort(expected)

    if produced == expected_sorted do
      :ok
    else
      {:error,
       {:manifest_invalid, {:module_mismatch, expected: expected_sorted, produced: produced}}}
    end
  end
end
