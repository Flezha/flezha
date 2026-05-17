defmodule Mix.Tasks.Flezha.Bench.Observability do
  @moduledoc """
  Microbenchmark for `Flezha.Observability.Bus.emit/1` against zero
  interested sinks — the hot-path no-op case.

  Reports nanoseconds per emit. The bench writes a single line to
  stdout and exits.

  Run: `mix flezha.bench.observability [iterations]`
  Default iterations: 1_000_000.
  """

  use Mix.Task

  alias Flezha.Observability.{Bus, Registry, Signal}

  @ns_per_emit_threshold 1_000
  @default_iterations 1_000_000

  @impl Mix.Task
  def run(args) do
    # Boot just the observability foundation — the rest of the
    # Controller (transports, executor fleet, BootLoader) is not
    # needed for an isolated bus microbenchmark and pulls in
    # external dependencies (test executors, sockets).
    Application.load(:flezha)
    {:ok, _} = Application.ensure_all_started(:telemetry)
    {:ok, _} = Application.ensure_all_started(:phoenix_pubsub)
    {:ok, _} = Flezha.Observability.Supervisor.start_link([])

    iterations =
      case args do
        [n | _] -> String.to_integer(n)
        _ -> @default_iterations
      end

    source = :"bench_observability_#{:erlang.unique_integer([:positive])}"

    :ok =
      Registry.register_source(%{
        name: source,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 1_000_000
      })

    # Per task 14.1, the bench measures hot-path emit against
    # zero interested sinks — unregister the default ring-buffer
    # and pubsub sinks so the dispatcher exits early.
    Registry.unregister_sink(:ring_buffer)
    Registry.unregister_sink(:pubsub)

    signal =
      Signal.new(%{
        kind: :metric,
        source: source,
        name: :bench,
        measurements: %{count: 1},
        metadata: %{constant_shape: true}
      })

    # Warm-up — JIT, cache the signal shape with the cardinality
    # tracker, prime the bus.
    Enum.each(1..1_000, fn _ -> Bus.emit(signal) end)

    {wall_us, :ok} =
      :timer.tc(fn ->
        loop(signal, iterations)
        :ok
      end)

    ns_per_emit = div(wall_us * 1_000, iterations)

    Mix.shell().info(
      "[bench] observability emit n=#{iterations} wall=#{wall_us}us " <>
        "per_emit=#{ns_per_emit}ns threshold=#{@ns_per_emit_threshold}ns"
    )

    if ns_per_emit > @ns_per_emit_threshold do
      Mix.shell().info(
        "[bench] WARN: per-emit cost exceeded informational threshold " <>
          "(#{ns_per_emit}ns > #{@ns_per_emit_threshold}ns)"
      )
    end

    Registry.unregister_source(source)
    :ok
  end

  defp loop(_signal, 0), do: :ok

  defp loop(signal, n) do
    Bus.emit(signal)
    loop(signal, n - 1)
  end
end
