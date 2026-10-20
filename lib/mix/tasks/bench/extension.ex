defmodule Mix.Tasks.Bench.Extension do
  use Mix.Task

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.File.Sink, as: FileSink
  alias Flezha.Plugin.Provider.File.WriteWorker
  alias Flezha.Plugin.Provider.HttpPush
  alias Flezha.Plugin.Provider.HttpSend
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias Mix.Tasks.Flezha.Bench.Observability

  @shortdoc "Microbenchmark for Flezha.Plugin.Extension.run/3 and the sink paths"
  @moduledoc """
  Microbenchmark for `Flezha.Plugin.Extension.run/3` on the hot
  `:on_input` / `:on_output` path. Measures wall-clock throughput at three
  subscriber counts (0, 1, 5) for both the extension system and an
  equivalent `Enum.reduce_while` over a list of closures — the
  hand-rolled-fold baseline.

      mix bench.extension
      mix bench.extension --iterations 500000

  N=0 is the dominant production case: relays without any subscriber
  attached to `:on_input` take the bitmask short-circuit.

  `--case` picks what is measured; `run` (the default) is the table above.
  The others drive one real sink path each and print one line,
  `[bench] extension case=<case> n=<N> wall=<us>us per_record=<ns>ns`:

    * `admission:http_push` - `--iterations` outputs (default 20000) through
      `http_push`'s dispatch into a real `HttpSend` worker, under
      `drop_nothing` with a queue of 100 that fills and drains, against a
      local origin answering 200; the wall ends once the queue is empty.
    * `admission:file_sink` - `--iterations` records (default 20000) through
      the file sink's dispatch, the enqueue call that waits for room, into a
      real write worker appending to a temporary file; the wall ends once
      every batch is synced.
    * `telemetry` - `--iterations` executes (default 200000) of
      `[:flezha, :plugin, :relay_forward, :shipped]` with the telemetry bridge
      attached and no sink registered.
  """

  @record "bench-record-0123456789abcdef"

  defmodule Origin do
    @moduledoc false
    # A keep-alive HTTP/1.1 origin on loopback answering every request 200 with
    # no body, and recording nothing.

    @spec start() :: :inet.port_number()
    def start do
      opts = [:binary, packet: :http_bin, active: false, reuseaddr: true, ip: {127, 0, 0, 1}]
      {:ok, listen} = :gen_tcp.listen(0, opts)
      {:ok, port} = :inet.port(listen)
      spawn_link(fn -> accept(listen) end)
      port
    end

    defp accept(listen) do
      {:ok, socket} = :gen_tcp.accept(listen)
      pid = spawn(fn -> receive(do: (:owned -> serve(socket, 0))) end)
      :ok = :gen_tcp.controlling_process(socket, pid)
      send(pid, :owned)
      accept(listen)
    end

    defp serve(socket, length) do
      case :gen_tcp.recv(socket, 0) do
        {:ok, {:http_header, _, :"Content-Length", _, value}} ->
          serve(socket, String.to_integer(value))

        {:ok, :http_eoh} ->
          :ok = skip_body(socket, length)
          :ok = :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
          serve(socket, 0)

        {:ok, _request_line_or_header} ->
          serve(socket, length)

        {:error, _closed} ->
          :gen_tcp.close(socket)
      end
    end

    defp skip_body(_socket, 0), do: :ok

    defp skip_body(socket, length) do
      :ok = :inet.setopts(socket, packet: :raw)
      {:ok, _body} = :gen_tcp.recv(socket, length)
      :inet.setopts(socket, packet: :http_bin)
    end
  end

  defmodule BenchProvider do
    @moduledoc false
    alias Flezha.Plugin.Extension.Ctx
    use Flezha.Plugin.ProviderBehaviour

    @impl true
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  @impl Mix.Task
  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [iterations: :integer, case: :string],
        aliases: [i: :iterations]
      )

    iterations = &Keyword.get(opts, :iterations, &1)

    # Compile but don't start the full app supervision tree — each case
    # starts only the processes it measures.
    Mix.Task.run("loadpaths")
    Mix.Task.run("compile")
    {:ok, _} = Application.ensure_all_started(:telemetry)

    case Keyword.get(opts, :case, "run") do
      "run" -> run_table(iterations.(200_000))
      "admission:http_push" -> http_push(iterations.(20_000))
      "admission:file_sink" -> file_sink(iterations.(20_000))
      "telemetry" -> telemetry(iterations.(200_000))
      other -> Mix.raise("Unknown --case #{inspect(other)}")
    end
  end

  defp run_table(iterations) do
    catalog =
      Host.build_catalog(
        [
          {:on_input, :transform, Ctx.IO},
          {:on_output, :transform, Ctx.IO}
        ],
        %{file: __ENV__.file, line: __ENV__.line}
      )

    ctx = %Ctx.IO{relay_id: "p", namespace: :n, message: "msg", metadata: %{}}

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

  defp http_push(records) do
    port = __MODULE__.Origin.start()

    opts =
      provider_opts(HttpPush, %{
        url: "http://127.0.0.1:#{port}/in",
        queue_max: 100,
        queue_block_timeout_ms: :timer.minutes(10)
      })

    {key, worker} = start_worker(HttpPush, HttpSend, opts)
    opts = Map.merge(opts, %{worker_key: key, sender: WorkerSup.worker(HttpPush, key)})

    timed("admission:http_push", records, fn ->
      Enum.each(1..records, fn _ -> {:ok, _} = HttpPush.on_output(output(), opts) end)
      wait_until(fn -> match?(%{queued: 0, waiting: 0, in_flight: 0}, HttpSend.stats(worker)) end)
    end)
  end

  defp file_sink(records) do
    dir = Path.join(System.tmp_dir!(), "flezha_bench_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    try do
      opts = provider_opts(FileSink, %{path: Path.join(dir, "out.log"), queue_max: 1_000})
      {key, worker} = start_worker(FileSink, WriteWorker, opts)
      opts = %{opts | worker_key: key}

      timed("admission:file_sink", records, fn ->
        Enum.each(1..records, fn _ -> {:ok, _} = FileSink.on_output(output(), opts) end)
        wait_until(fn -> WriteWorker.stats(worker).pending == 0 end)
      end)
    after
      File.rm_rf!(dir)
    end
  end

  defp telemetry(executes) do
    Observability.boot()
    event = [:flezha, :plugin, :relay_forward, :shipped]

    labels = %{
      plugin_id: "bench_forward",
      namespace: :bench,
      source_relay_id: "bench_source",
      target_relay_id: "bench_target",
      outcome: :delivered
    }

    execute = fn n ->
      Enum.each(1..n, fn _ -> :telemetry.execute(event, %{count: 1}, labels) end)
    end

    execute.(10_000)
    timed("telemetry", executes, fn -> execute.(executes) end)
  end

  # A provider's declared defaults with the host's identity, as a table
  # prepares them, and no channel filter.
  defp provider_opts(provider, overrides) do
    provider
    |> Options.defaults()
    |> Map.merge(%{
      plugin_id: "bench_#{provider.__provider__().name}",
      source_plugin_id: "bench_#{provider.__provider__().name}",
      source_namespace: :bench,
      source_relay_id: "bench",
      filter_channel: nil,
      worker_key: nil
    })
    |> Map.merge(overrides)
  end

  # The provider's worker, started under its own family as activation starts it.
  defp start_worker(provider, worker, opts) do
    family = WorkerSup.family(provider)
    {:ok, _} = WorkerSup.start_link(family)
    [{key, spec}] = provider.worker_specs(opts)
    {:ok, pid} = WorkerSup.ensure_worker(family, key, {worker, spec})
    {key, pid}
  end

  defp output, do: %Ctx.IO{relay_id: "bench", namespace: :bench, message: @record, metadata: %{}}

  defp wait_until(done?) do
    unless done?.() do
      Process.sleep(1)
      wait_until(done?)
    end
  end

  defp timed(name, n, fun) do
    {wall_us, _} = :timer.tc(fun)

    Mix.shell().info(
      "[bench] extension case=#{name} n=#{n} wall=#{wall_us}us " <>
        "per_record=#{div(wall_us * 1_000, max(n, 1))}ns"
    )
  end

  defp build_table(catalog, 0), do: Table.new(catalog)

  defp build_table(catalog, n) when n > 0 do
    Enum.reduce(0..(n - 1)//1, Table.new(catalog), fn i, acc ->
      plugin = %Plugin{
        id: "p#{i}",
        provider: BenchProvider,
        opts: %{},
        hooks: [{:on_input, [position: i]}]
      }

      {:ok, t} = Table.bulk_subscribe(acc, [{"p#{i}", plugin}])
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
