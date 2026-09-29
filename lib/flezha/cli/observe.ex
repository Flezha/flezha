defmodule Flezha.Service.Cli.Observe do
  @moduledoc """
  `flz observe` — sources, sinks, enable, disable and tail, over the effects
  `Flezha.Service.Observability` shares with the console.
  """

  alias Flezha.Observability.Signal
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Observability, as: ObsSvc

  @doc """
  CLI entry point for `flz observe sources`. Renders text or JSON.
  """
  def observability_render_sources(%{opts: opts}) do
    with {:ok, sources} <- ObsSvc.sources(),
         do: render_sources_output(sources, Context.format(opts))
  end

  def observability_render_sinks(%{opts: opts}) do
    with {:ok, sinks} <- ObsSvc.sinks(), do: render_sinks_output(sinks, Context.format(opts))
  end

  @doc """
  CLI entry point for `flz observe tail <source>`. Subscribes and
  streams signals as NDJSON (one signal per line) until the calling
  RPC connection closes.
  """
  def observability_render_tail(source_str, %{opts: opts}) do
    source = registered_source!(source_str)

    with :ok <- ObsSvc.tail_source(source) do
      Context.receive_for(Context.window(opts), &tail_message(&1, Context.format(opts)))
      :ok
    end
  end

  def observability_render_enable(source_str, _call) do
    source = registered_source!(source_str)

    with :ok <- ObsSvc.enable(source) do
      IO.puts("source #{source} enabled")
      :ok
    end
  end

  def observability_render_disable(source_str, _call) do
    source = registered_source!(source_str)

    with :ok <- ObsSvc.disable(source) do
      IO.puts("source #{source} disabled")
      :ok
    end
  end

  defp render_sources_output(sources, "json") do
    sources
    |> Enum.map(&source_to_map/1)
    |> Jason.encode!()
    |> IO.puts()
  end

  defp render_sources_output(sources, "text") do
    header = ["name", "status", "kinds", "budget"]
    IO.puts(Enum.map_join(header, "\t", & &1))

    Enum.each(sources, fn source ->
      IO.puts(
        Enum.map_join(
          [
            source.name,
            source.status,
            Enum.map_join(source.kinds, ",", &Atom.to_string/1),
            source.cardinality_budget
          ],
          "\t",
          &to_string/1
        )
      )
    end)
  end

  defp render_sinks_output(sinks, "json") do
    sinks
    |> Enum.map(&sink_to_map/1)
    |> Jason.encode!()
    |> IO.puts()
  end

  defp render_sinks_output(sinks, "text") do
    header = ["name", "module", "overflow", "watermark"]
    IO.puts(Enum.map_join(header, "\t", & &1))

    Enum.each(sinks, fn sink ->
      IO.puts(
        Enum.map_join(
          [
            sink.name,
            inspect(sink.module),
            sink.overflow,
            sink.mailbox_high_watermark
          ],
          "\t",
          &to_string/1
        )
      )
    end)
  end

  defp source_to_map(source) do
    %{
      name: source.name,
      status: source.status,
      flavour: source.flavour,
      kinds: source.kinds,
      cardinality_budget: source.cardinality_budget
    }
  end

  defp sink_to_map(sink) do
    %{
      name: sink.name,
      module: inspect(sink.module),
      overflow: sink.overflow,
      mailbox_high_watermark: sink.mailbox_high_watermark
    }
  end

  defp tail_message({:flezha_signal, signal}, format) do
    render_tail_signal(signal, format)
    :continue
  end

  defp tail_message(_message, _format), do: :continue

  defp render_tail_signal(signal, "json") do
    %{
      kind: signal.kind,
      source: signal.source,
      name: signal.name,
      ts: signal.ts,
      measurements: Signal.metadata_json(signal.measurements),
      metadata: Signal.metadata_json(signal.metadata)
    }
    |> Jason.encode!()
    |> IO.puts()
  end

  defp render_tail_signal(signal, "text") do
    IO.puts(
      "[#{signal.ts}] #{signal.source}.#{signal.name} kind=#{signal.kind} " <>
        "measurements=#{inspect(signal.measurements)} metadata=#{inspect(signal.metadata)}"
    )
  end

  # The registry IS the allow-list: a source name the caller typed is read
  # against what is registered, never with `String.to_atom/1`, which would mint
  # an atom the node then keeps for ever for every typo.
  defp registered_source!(written) do
    case ObsSvc.sources() do
      {:ok, sources} ->
        Context.atom!(written, Enum.map(sources, & &1.name), "source")

      {:error, reason} ->
        Context.fail!(reason)
    end
  end
end
