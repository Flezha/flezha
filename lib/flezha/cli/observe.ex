defmodule Flezha.Service.Cli.Observe do
  @moduledoc """
  `flz observe` — the text forms of sources, sinks, enable and disable, and the
  tail, which prints a source's live signals until its window closes.
  """

  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Context

  @doc "`flz observe sources`: one line per source, its status on each controller."
  def sources(answer, _args, _opts) do
    tsv(answer, ~w(name status kinds budget title description), fn source ->
      [
        source.name,
        Enum.map_join(source.statuses, ",", fn {node, status} -> "#{node}=#{status}" end),
        Enum.join(source.kinds, ","),
        source.cardinality_budget,
        Map.get(source, :title),
        Map.get(source, :description)
      ]
    end)
  end

  @doc "`flz observe sinks`: every controller's sinks, each with its controller."
  def sinks(answer, _args, _opts) do
    tsv(answer, ~w(name node module overflow watermark), fn sink ->
      [sink.name, sink.node, inspect(sink.module), sink.overflow, sink.mailbox_high_watermark]
    end)
  end

  @doc "`flz observe enable`."
  def enable(switched, [source | _], _opts), do: switched(switched, source, "enabled")

  @doc "`flz observe disable`."
  def disable(switched, [source | _], _opts), do: switched(switched, source, "disabled")

  defp switched(%{applied: applied, unreachable: unreachable}, source, done) do
    IO.puts("source #{source} #{done} on #{Enum.join(applied, ", ")}")
    Context.unreachable(unreachable)
  end

  @doc """
  `flz observe tail <source>`: print each signal as one line (one JSON
  document under `--json`) until the window closes.
  """
  def tail(args_json, context, opts) do
    format = Context.format(opts)

    context =
      Map.merge(context, %{emit: &print_signal(&1, format), window_ms: Context.window(opts)})

    case Admin.run("Observability.tail", args_json, context, :given) do
      {:ok, _outcome} -> :ok
      refused -> Cli.render(refused, "Observability.tail", args_json, opts)
    end
  end

  # A header line and one tab-separated line per row.
  defp tsv(answer, header, fields) do
    Context.rows(answer, fn rows ->
      IO.puts(Enum.join(header, "\t"))

      Enum.each(
        rows,
        &IO.puts(Enum.map_join(fields.(&1), "\t", fn field -> to_string(field) end))
      )
    end)
  end

  defp print_signal(signal, "json") do
    {"signal", object} = Admin.event(signal, %{source: :cli})
    IO.puts(Jason.encode!(object))
  end

  defp print_signal(signal, _text) do
    IO.puts(
      "[#{signal.at_us} #{signal.node}] #{signal.source}.#{signal.name} kind=#{signal.kind} " <>
        "measurements=#{inspect(signal.measurements)} metadata=#{inspect(signal.metadata)}"
    )
  end
end
