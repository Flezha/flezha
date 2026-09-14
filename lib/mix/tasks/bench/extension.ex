defmodule Mix.Tasks.Bench.Extension do
  use Mix.Task

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.Kafka.ProducerWorker

  @shortdoc "Microbenchmark for Flezha.Plugin.Extension.run/3"
  @moduledoc """
  Microbenchmark for `Flezha.Plugin.Extension.run/3` on the hot
  `:on_input` / `:on_output` path. Measures wall-clock throughput at three
  subscriber counts (0, 1, 5) for both the extension system and an
  equivalent `Enum.reduce_while` over a list of closures — the
  hand-rolled-fold baseline.

  Also runs a "Kafka chain mailbox boundedness" scenario that drives a
  transform chain with one Kafka producer subscriber against a stub
  Kayrock returning `:ok` after a 50 ms simulated RTT. Asserts the host
  process's mailbox stays bounded (depth ≤ 1) under sustained 80 ev/s,
  which is the regression test for the
  `make-kafka-producer-non-blocking` change. Pre-change (synchronous
  `enqueue/4` on the chain) the host's mailbox grew without bound at this
  rate; post-change (`cast_enqueue/3`) it stays at 0–1.

      mix bench.extension
      mix bench.extension --iterations 500000

  N=0 is the dominant production case: operatives without any subscriber
  attached to `:on_input` take the bitmask short-circuit.
  """

  defmodule BenchProvider do
    @moduledoc false
    alias Flezha.Plugin.Extension.Ctx

    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule KafkaBenchProvider do
    @moduledoc false
    alias Flezha.Plugin.Extension.Ctx
    alias Flezha.Plugin.Provider.Kafka.ProducerWorker

    def on_input(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
      :ok = ProducerWorker.cast_enqueue(opts.producer_pid, msg, md)
      {:ok, ctx}
    end
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

    ctx = %Ctx.IO{operative_id: "p", namespace: :n, message: "msg", metadata: %{}}

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

    run_kafka_chain_scenario(catalog, ctx)
  end

  defp run_kafka_chain_scenario(catalog, ctx) do
    IO.puts("\nKafka chain mailbox boundedness — 80 ev/s for 5 s")
    IO.puts(String.duplicate("=", 78))

    fake_kayrock = spawn(fn -> Process.sleep(:infinity) end)
    {:ok, worker} = start_fake_producer(fake_kayrock)

    {:ok, table} =
      Table.bulk_subscribe(Table.new(catalog), [{"kafka-bench", kafka_plugin(worker)}])

    {:ok, _} = Application.ensure_all_started(:telemetry)

    host = spawn(fn -> chain_host_loop(table, ctx) end)
    {total, send_us, final_qlen} = drive_host(host)

    Process.exit(host, :kill)
    GenServer.stop(worker)
    Process.exit(fake_kayrock, :kill)

    report_chain(total, send_us, final_qlen)
  end

  # A producer worker wired to a broker that answers every Produce after 50 ms
  # and never fails: the point is the mailbox in front of it, not the network.
  defp start_fake_producer(fake_kayrock) do
    send_fn = fn _w, _r, _ns ->
      Process.sleep(50)

      {:ok,
       %Kayrock.Produce.V5.Response{
         responses: [
           %{
             topic: "bench",
             partition_responses: [
               %{
                 partition: 0,
                 error_code: 0,
                 base_offset: 0,
                 log_append_time: -1,
                 log_start_offset: -1
               }
             ]
           }
         ],
         throttle_time_ms: 0
       }}
    end

    ProducerWorker.start_link(%{
      plugin_id: "bench",
      topic: "bench",
      partition: 0,
      worker_pid: fake_kayrock,
      send_fn: send_fn,
      queue_max: 10_000,
      queue_overflow: :drop_oldest,
      linger_ms: 10,
      idempotent: false,
      init_producer_id_fn: fn _w, _t -> {:ok, %{producer_id: 1, producer_epoch: 0}} end,
      metadata_refresh_fn: fn _w, _t -> :ok end,
      create_topic_fn: fn _w, _t, _o, _to -> :ok end,
      name: nil
    })
  end

  defp kafka_plugin(worker) do
    %Plugin{
      id: "kafka-bench",
      provider: KafkaBenchProvider,
      opts: %{producer_pid: worker},
      hooks: [{:on_input, [position: 0]}]
    }
  end

  defp drive_host(host) do
    duration_ms = 5_000
    rate_ev_s = 80
    interval_us = div(1_000_000, rate_ev_s)
    total = div(duration_ms * rate_ev_s, 1_000)

    {sample_pid, sample_ref} = spawn_monitor(fn -> sample_mailbox(host, duration_ms) end)

    {send_us, _} =
      :timer.tc(fn ->
        Enum.each(1..total, fn i ->
          send(host, {:io_in, "msg-#{i}"})
          busy_wait_until(System.monotonic_time(:microsecond) + interval_us)
        end)
      end)

    Process.sleep(100)
    {:message_queue_len, final_qlen} = Process.info(host, :message_queue_len)

    receive do
      {:DOWN, ^sample_ref, :process, ^sample_pid, _} -> :ok
    after
      duration_ms + 500 -> :ok
    end

    {total, send_us, final_qlen}
  end

  defp report_chain(total, send_us, final_qlen) do
    IO.puts(
      :io_lib.format("~-48s  ~10w events    ~12w µs total", [
        "drove host @ 80 ev/s",
        total,
        send_us
      ])
      |> IO.iodata_to_binary()
    )

    IO.puts("Final host mailbox depth: #{final_qlen}")

    if final_qlen <= 1 do
      IO.puts("PASS: host mailbox bounded (≤ 1) — chain dispatch is non-blocking.")
    else
      IO.puts(
        "FAIL: host mailbox is #{final_qlen} — chain dispatch is blocking " <>
          "(would have grown without bound under sustained input)."
      )
    end
  end

  defp chain_host_loop(table, ctx_template) do
    receive do
      {:io_in, msg} ->
        ctx = %{ctx_template | message: msg}
        _ = Extension.run(table, :on_input, ctx)
        chain_host_loop(table, ctx_template)
    end
  end

  defp sample_mailbox(host, duration_ms) do
    deadline = System.monotonic_time(:millisecond) + duration_ms
    sample_loop(host, deadline, 0)
  end

  defp sample_loop(host, deadline, peak) do
    if System.monotonic_time(:millisecond) >= deadline do
      IO.puts("Peak host mailbox depth observed during run: #{peak}")
    else
      qlen =
        case Process.info(host, :message_queue_len) do
          {:message_queue_len, n} -> n
          nil -> 0
        end

      Process.sleep(50)
      sample_loop(host, deadline, max(peak, qlen))
    end
  end

  defp busy_wait_until(target_us) do
    if System.monotonic_time(:microsecond) < target_us do
      busy_wait_until(target_us)
    else
      :ok
    end
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
      :io_lib.format("~-48s  ~10w ns/call    ~12w procs/sec", [
        label,
        per_call_ns,
        ops_per_sec
      ])
      |> IO.iodata_to_binary()
    )
  end
end
