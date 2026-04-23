defmodule Mix.Tasks.Bench.Extension do
  use Mix.Task

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host

  @shortdoc "Microbenchmark for Flezha.Plugin.Extension.run/3"
  @moduledoc """
  Microbenchmark for `Flezha.Plugin.Extension.run/3` on the hot
  `:on_input` / `:on_output` path. Measures wall-clock throughput at three
  subscriber counts (0, 1, 5) for both the extension system and an
  equivalent `Enum.reduce_while` over a list of closures — the shape the
  legacy `exec_bindings/2` had.

      mix bench.extension
      mix bench.extension --iterations 500000

  N=0 is the dominant production case: processors without any subscriber
  attached to `:on_input` take the bitmask short-circuit.
  """

  defmodule BenchProvider do
    @moduledoc false
    alias Flezha.Plugin.Extension.Ctx

    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  @impl Mix.Task
  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args, strict: [iterations: :integer], aliases: [i: :iterations])

    iterations = Keyword.get(opts, :iterations, 200_000)

    # Compile but don't start the full app supervision tree — the bench only
    # exercises Extension.run/3 and needs no live processes.
    Mix.Task.run("loadpaths")
    Mix.Task.run("compile")

    catalog =
      Host.build_catalog(
        [
          {:on_input, :transform, Ctx.IO},
          {:on_output, :transform, Ctx.IO}
        ],
        %{file: __ENV__.file, line: __ENV__.line}
      )

    ctx = %Ctx.IO{processor_id: "p", namespace: :n, message: "msg", metadata: %{}}

    IO.puts("\nExtension.run/3 microbenchmark — #{iterations} iterations each")
    IO.puts(String.duplicate("=", 78))

    for n <- [0, 1, 5] do
      table = build_table(catalog, n)
      baseline = build_baseline(n)

      measure(
        "Extension.run(:on_input, ctx)   N=#{n}",
        fn -> Extension.run(table, :on_input, ctx) end,
        iterations
      )

      measure(
        "exec_bindings-style reduce_while N=#{n}",
        fn -> exec_baseline(baseline, {"msg", %{}}) end,
        iterations
      )

      IO.puts(String.duplicate("-", 78))
    end

    IO.puts("\nNote: the N=0 extension case short-circuits on the bitmask check; the")
    IO.puts("reduce_while baseline still walks an empty list. This is the main win.")
  end

  defp build_table(catalog, 0), do: Table.new(catalog)

  defp build_table(catalog, n) when n > 0 do
    Enum.reduce(0..(n - 1)//1, Table.new(catalog), fn i, acc ->
      plugin = %Plugin{
        id: "p#{i}",
        provider: BenchProvider,
        opts: %{},
        subscriptions: [{:on_input, [position: i]}]
      }

      {:ok, t} = Table.subscribe(acc, "p#{i}", plugin)
      t
    end)
  end

  defp build_baseline(0), do: []

  defp build_baseline(n) when n > 0 do
    for _i <- 1..n//1, do: {:plugin, fn {msg, md} -> {:ok, {msg, md}} end}
  end

  defp exec_baseline(bindings, payload) do
    Enum.reduce_while(bindings, payload, fn {_tag, f}, acc ->
      case f.(acc) do
        {:ok, new} -> {:cont, new}
        {:skip, :normal} -> {:halt, nil}
        {:error, _} -> {:cont, acc}
      end
    end)
  end

  defp measure(label, fun, iterations) do
    Enum.each(1..10_000, fn _ -> fun.() end)

    {micros, _} = :timer.tc(fn -> Enum.each(1..iterations, fn _ -> fun.() end) end)

    per_call_ns = div(micros * 1000, iterations)
    ops_per_sec = div(iterations * 1_000_000, max(micros, 1))

    IO.puts(
      :io_lib.format("~-48s  ~10w ns/call    ~12w ops/sec", [
        label,
        per_call_ns,
        ops_per_sec
      ])
      |> IO.iodata_to_binary()
    )
  end
end
