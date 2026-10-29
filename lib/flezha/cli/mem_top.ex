defmodule Flezha.Service.Cli.MemTop do
  @moduledoc """
  `flz controller mem-top` — the text form of each controller's memory snapshot.
  """

  alias Flezha.Service.Cli.Context

  @doc "`flz controller mem-top`: one snapshot per controller."
  def mem_top(answer, _args, _opts), do: Context.per_controller(answer, &snapshot/1)

  defp snapshot(snapshot) do
    IO.puts("Controller memory snapshot — #{snapshot.captured_at}")

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
      IO.puts("  #{String.pad_trailing(to_string(k), 12)} #{v}")
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
    IO.puts("DETS files under #{snapshot.dets_root}:")

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
