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
    boot()

    iterations =
      case args do
        [n | _] -> String.to_integer(n)
        _ -> @default_iterations
      end

    source = :"bench_observability_#{:erlang.unique_integer([:positive])}"
    signal = bench_signal(source)

    # Warm-up — JIT, cache the signal shape with the cardinality
    # tracker, prime the bus.
    Enum.each(1..1_000, fn _ -> Bus.emit(signal) end)

    {wall_us, :ok} =
      :timer.tc(fn ->
        loop(signal, iterations)
        :ok
      end)

    report(iterations, wall_us)

    Registry.unregister_source(source)
    :ok
  end

  # Boot just the observability foundation — the rest of the Controller
  # (transports, runner fleet, BootLoader) is not needed for an isolated bus
  # microbenchmark and pulls in external dependencies (test runners, sockets).
  defp boot do
    Application.load(:flezha)
    {:ok, _} = Application.ensure_all_started(:telemetry)
    {:ok, _} = Application.ensure_all_started(:phoenix_pubsub)
    {:ok, _} = Flezha.Observability.Supervisor.start_link([])
  end

  # The bench measures hot-path emit against zero interested sinks, so the
  # default ring-buffer and pubsub sinks are unregistered and the dispatcher
  # exits early.
  defp bench_signal(source) do
    :ok =
      Registry.register_source(%{
        name: source,
        kinds: [:metric],
        flavour: :declarative,
        cardinality_budget: 1_000_000
      })

    Registry.unregister_sink(:ring_buffer)
    Registry.unregister_sink(:pubsub)

    Signal.new(%{
      kind: :metric,
      source: source,
      name: :bench,
      measurements: %{count: 1},
      metadata: %{constant_shape: true}
    })
  end

  defp report(iterations, wall_us) do
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
  end

  defp loop(_signal, 0), do: :ok

  defp loop(signal, n) do
    Bus.emit(signal)
    loop(signal, n - 1)
  end
end
