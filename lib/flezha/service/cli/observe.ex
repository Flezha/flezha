defmodule Flezha.Service.Cli.Observe do
  @moduledoc """
  `flz observe` — sources, sinks, enable, disable and tail.
  """

  alias Flezha.Observability.Signal
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Observability, as: ObsSvc

  def observability_list_sources(opts \\ []) do
    ObsSvc.list_sources(Context.cli_context(opts))
  end

  def observability_enable_source(source, opts \\ []) when is_atom(source) do
    ObsSvc.enable_source(source, Context.cli_context(opts))
  end

  def observability_disable_source(source, opts \\ []) when is_atom(source) do
    ObsSvc.disable_source(source, Context.cli_context(opts))
  end

  @doc """
  CLI entry point for `flz observe sources`. Renders text or JSON.
  """
  def observability_render_sources(format) when format in ["text", "json"] do
    case ObsSvc.list_sources(Context.cli_context([])) do
      {:ok, sources} -> render_sources_output(sources, format)
      {:error, reason} -> render_error(reason)
    end
  end

  def observability_render_sinks(format) when format in ["text", "json"] do
    case ObsSvc.list_sinks(Context.cli_context([])) do
      {:ok, sinks} -> render_sinks_output(sinks, format)
      {:error, reason} -> render_error(reason)
    end
  end

  @doc """
  CLI entry point for `flz observe tail <source>`. Subscribes and
  streams signals as NDJSON (one signal per line) until the calling
  RPC connection closes.
  """
  def observability_render_tail(source_str, format, opts \\ []) when format in ["text", "json"] do
    source = String.to_atom(source_str)
    duration_ms = Keyword.get(opts, :duration_ms, 30_000)

    case ObsSvc.tail(source, Context.cli_context(opts)) do
      :ok ->
        tail_loop(source, format, duration_ms)

      {:error, reason} ->
        render_error(reason)
    end
  end

  def observability_render_enable(source_str) do
    source = String.to_atom(source_str)

    case ObsSvc.enable_source(source, Context.cli_context([])) do
      :ok ->
        IO.puts("source #{source} enabled")
        :ok

      {:error, reason} ->
        render_error(reason)
    end
  end

  def observability_render_disable(source_str) do
    source = String.to_atom(source_str)

    case ObsSvc.disable_source(source, Context.cli_context([])) do
      :ok ->
        IO.puts("source #{source} disabled")
        :ok

      {:error, reason} ->
        render_error(reason)
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

  defp tail_loop(source, format, duration_ms) do
    deadline = System.monotonic_time(:millisecond) + duration_ms
    do_tail_loop(source, format, deadline)
  end

  defp do_tail_loop(source, format, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      :ok
    else
      receive do
        {:flezha_signal, signal} ->
          render_tail_signal(signal, format)
          do_tail_loop(source, format, deadline)
      after
        min(remaining, 1_000) ->
          if System.monotonic_time(:millisecond) >= deadline,
            do: :ok,
            else: do_tail_loop(source, format, deadline)
      end
    end
  end

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

  defp render_error(:pubsub_sink_disabled) do
    Context.cli_error!(
      "error: pubsub sink is disabled; enable :observability, :pubsub_sink_enabled to use `flz observe tail`",
      :stderr
    )
  end

  defp render_error(:observability_disabled) do
    Context.cli_error!("error: observability is disabled", :stderr)
  end

  defp render_error(reason) do
    Context.cli_error!("error: #{inspect(reason)}", :stderr)
  end
end
