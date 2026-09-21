defmodule Flezha.Service.Cli.MemTop do
  @moduledoc """
  `flz controller mem-top` — the operator-invoked memory inspector and its
  text and JSON renderers.
  """

  alias Flezha.Layout.Table, as: Layout

  @top_task_count 20

  @doc """
  `flz controller mem-top` RPC. Captures a single snapshot of the
  Controller's memory hot spots: top-20 processes by `:memory`, ETS
  tables by `:memory`, DETS files under the configured ProcessHub registry root, and
  the `:recon_alloc.memory(:allocated_types)` allocator breakdown.

  `output_format` is `"text"` or `"json"`.
  """
  def controller_mem_top(output_format, _call) do
    snapshot = build_mem_top_snapshot()

    case output_format do
      "json" -> IO.puts(Jason.encode!(snapshot))
      _ -> print_mem_top_text(snapshot)
    end

    :ok
  end

  @doc false
  def build_mem_top_snapshot do
    %{
      captured_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      node: Atom.to_string(node()),
      erlang_memory: erlang_memory_map(),
      top_processes: top_processes(@top_task_count),
      ets_tables: top_ets_tables(),
      dets_files: dets_files_under_process_hub(),
      recon_alloc_allocated_types: recon_alloc_allocated_types()
    }
  end

  defp erlang_memory_map do
    :erlang.memory()
    |> Enum.into(%{})
    |> Map.take([:total, :binary, :processes, :ets, :atom, :code, :system])
  end

  defp top_processes(limit) do
    Process.list()
    |> Enum.map(&process_info_row/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(& &1.memory, :desc)
    |> Enum.take(limit)
  end

  defp process_info_row(pid) do
    case Process.info(pid, [
           :memory,
           :registered_name,
           :initial_call,
           :current_function,
           :message_queue_len,
           :heap_size,
           :total_heap_size
         ]) do
      nil ->
        nil

      info ->
        %{
          pid: :erlang.pid_to_list(pid) |> List.to_string(),
          memory: Keyword.get(info, :memory, 0),
          registered_name: format_registered_name(Keyword.get(info, :registered_name)),
          initial_call: inspect(Keyword.get(info, :initial_call)),
          current_function: inspect(Keyword.get(info, :current_function)),
          message_queue_len: Keyword.get(info, :message_queue_len, 0),
          heap_size: Keyword.get(info, :heap_size, 0),
          total_heap_size: Keyword.get(info, :total_heap_size, 0)
        }
    end
  catch
    # process_info/2 raises `badarg` if the pid dies between Process.list/0
    # and the introspection call. Treat as "gone" rather than crashing the
    # whole snapshot.
    :error, :badarg -> nil
  end

  defp format_registered_name([]), do: nil
  defp format_registered_name(name) when is_atom(name), do: Atom.to_string(name)
  defp format_registered_name(other), do: inspect(other)

  defp top_ets_tables do
    :ets.all()
    |> Enum.map(&ets_table_row/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(& &1.memory_bytes, :desc)
  end

  defp ets_table_row(tab) do
    memory_words = :ets.info(tab, :memory)
    size = :ets.info(tab, :size)
    name = :ets.info(tab, :name)

    if is_integer(memory_words) do
      %{
        name: inspect(name),
        size: size || 0,
        memory_bytes: memory_words * :erlang.system_info(:wordsize)
      }
    end
  catch
    :error, :badarg -> nil
  end

  defp dets_files_under_process_hub do
    root = Layout.dir(:process_hub)

    if File.exists?(root) do
      root
      |> list_files_recursive()
      |> Enum.filter(&String.ends_with?(&1, ".dets"))
      |> Enum.map(fn path ->
        size =
          case File.stat(path) do
            {:ok, %File.Stat{size: s}} -> s
            _ -> 0
          end

        %{path: Path.relative_to_cwd(path), size_bytes: size}
      end)
      |> Enum.sort_by(& &1.size_bytes, :desc)
    else
      []
    end
  end

  defp list_files_recursive(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn entry ->
          path = Path.join(dir, entry)

          cond do
            File.dir?(path) -> list_files_recursive(path)
            File.regular?(path) -> [path]
            true -> []
          end
        end)

      _ ->
        []
    end
  end

  defp recon_alloc_allocated_types do
    :recon_alloc.memory(:allocated_types)
    |> Enum.into(%{}, fn {k, v} -> {Atom.to_string(k), v} end)
  end

  defp print_mem_top_text(snapshot) do
    IO.puts("Controller memory snapshot — #{snapshot.captured_at} (#{snapshot.node})")

    Enum.each(
      [
        &print_erlang_memory/1,
        &print_top_processes/1,
        &print_ets_tables/1,
        &print_dets_files/1,
        &print_recon_alloc/1
      ],
      fn section ->
        IO.puts("")
        section.(snapshot)
      end
    )
  end

  defp print_erlang_memory(snapshot) do
    IO.puts("erlang_memory (bytes):")

    Enum.each(snapshot.erlang_memory, fn {k, v} ->
      IO.puts("  #{String.pad_trailing(Atom.to_string(k), 12)} #{v}")
    end)
  end

  defp print_top_processes(snapshot) do
    IO.puts("Top #{length(snapshot.top_processes)} processes by :memory:")

    IO.puts(
      "  " <>
        String.pad_trailing("memory", 12) <>
        String.pad_trailing("msgq", 8) <>
        String.pad_trailing("name / initial_call", 60)
    )

    Enum.each(snapshot.top_processes, fn p ->
      label = p.registered_name || p.initial_call

      IO.puts(
        "  " <>
          String.pad_trailing(Integer.to_string(p.memory), 12) <>
          String.pad_trailing(Integer.to_string(p.message_queue_len), 8) <>
          to_string(label)
      )
    end)
  end

  defp print_ets_tables(snapshot) do
    IO.puts("ETS tables by :memory (top 20):")

    snapshot.ets_tables
    |> Enum.take(20)
    |> Enum.each(fn t ->
      IO.puts(
        "  " <>
          String.pad_trailing(Integer.to_string(t.memory_bytes), 12) <>
          String.pad_trailing("size=#{t.size}", 14) <>
          t.name
      )
    end)
  end

  defp print_dets_files(snapshot) do
    IO.puts("DETS files under #{Layout.dir(:process_hub)}:")

    if snapshot.dets_files == [] do
      IO.puts("  (none)")
    else
      Enum.each(snapshot.dets_files, fn d ->
        IO.puts("  " <> String.pad_trailing(Integer.to_string(d.size_bytes), 12) <> d.path)
      end)
    end
  end

  defp print_recon_alloc(snapshot) do
    IO.puts(":recon_alloc.memory(:allocated_types):")

    snapshot.recon_alloc_allocated_types
    |> Enum.sort_by(fn {_, v} -> v end, :desc)
    |> Enum.each(fn {k, v} ->
      IO.puts("  #{String.pad_trailing(k, 24)} #{v}")
    end)
  end
end
