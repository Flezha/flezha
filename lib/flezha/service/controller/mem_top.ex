defmodule Flezha.Service.Controller.MemTop do
  @moduledoc """
  The memory snapshot `Flezha.Service.Controller.mem_top/1` answers: the top
  processes by `:memory`, the ETS tables by `:memory`, the DETS files under the
  ProcessHub registry root, and the allocator breakdown.
  """

  alias Flezha.Layout.Table, as: Layout

  @top_task_count 20

  @doc "One snapshot of this node's memory hot spots."
  @spec snapshot() :: map()
  def snapshot do
    %{
      captured_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      node: Atom.to_string(node()),
      erlang_memory: erlang_memory_map(),
      top_processes: top_processes(@top_task_count),
      ets_tables: top_ets_tables(),
      dets_root: Layout.dir(:process_hub),
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
end
