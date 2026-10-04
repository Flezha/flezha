defmodule Flezha.Service.Cli.Observe do
  @moduledoc """
  `flz observe` — the text forms of sources, sinks, enable and disable, and the
  tail, which prints a source's live signals until its window closes.
  """

  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Context

  @doc "`flz observe sources`."
  def sources(sources, _args, _opts) do
    IO.puts(Enum.join(["name", "status", "kinds", "budget"], "\t"))

    Enum.each(sources, fn source ->
      IO.puts(
        Enum.map_join(
          [source.name, source.status, Enum.join(source.kinds, ","), source.cardinality_budget],
          "\t",
          &to_string/1
        )
      )
    end)
  end

  @doc "`flz observe sinks`."
  def sinks(sinks, _args, _opts) do
    IO.puts(Enum.join(["name", "module", "overflow", "watermark"], "\t"))

    Enum.each(sinks, fn sink ->
      IO.puts(
        Enum.map_join(
          [sink.name, inspect(sink.module), sink.overflow, sink.mailbox_high_watermark],
          "\t",
          &to_string/1
        )
      )
    end)
  end

  @doc "`flz observe enable`."
  def enable(:ok, [source | _], _opts), do: IO.puts("source #{source} enabled")

  @doc "`flz observe disable`."
  def disable(:ok, [source | _], _opts), do: IO.puts("source #{source} disabled")

  @doc """
  `flz observe tail <source>`: subscribe, print each signal as one line (one
  JSON document under `--json`) until the window closes, then unsubscribe.
  """
  def tail(args_json, context, opts) do
    case Admin.run("Observability.tail", args_json, context, :given) do
      {:ok, _subscribed} ->
        Context.receive_for(Context.window(opts), &tail_message(&1, Context.format(opts)))
        Admin.run("Observability.unsubscribe", args_json, context, :given)

      refused ->
        Cli.render(refused, "Observability.tail", args_json, opts)
    end
  end

  defp tail_message({:flezha_signal, signal}, "json") do
    %{
      kind: signal.kind,
      source: signal.source,
      name: signal.name,
      ts: signal.ts,
      measurements: signal.measurements,
      metadata: signal.metadata
    }
    |> Admin.json()
    |> IO.puts()

    :continue
  end

  defp tail_message({:flezha_signal, signal}, _text) do
    IO.puts(
      "[#{signal.ts}] #{signal.source}.#{signal.name} kind=#{signal.kind} " <>
        "measurements=#{inspect(signal.measurements)} metadata=#{inspect(signal.metadata)}"
    )

    :continue
  end

  defp tail_message(_message, _format), do: :continue
end
