defmodule Mix.Tasks.Bench.Workflow do
  use Mix.Task

  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager
  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Tally

  @shortdoc "Workflow operative benchmark: spawn, commit, turn, sleep, wake"
  @moduledoc """
  Measures workflow operatives on the paths a stream never takes: the instance
  lifecycle (spawn → chain → completion), the durable proc barrier, the chat
  turn (iteration commit), suspension, and the wake paths.

      mix bench.workflow --scenario chain -n 1000 --procs 4 -c 100
      mix bench.workflow --scenario chain -n 2000 --procs 4 --rate 200
      mix bench.workflow --scenario turns -n 100 -m 100 --rate 1000
      mix bench.workflow --scenario turns -n 100 --duration 600 --rate 500 --kill-runner-at 120
      mix bench.workflow --scenario sleep -n 10000 --wake-after-ms 15000
      mix bench.workflow --scenario sleep -n 10000 --keep
      mix bench.workflow --scenario sleep-boot -n 10000

  ## Scenarios

    * `chain` — spawn `-n` run-once workflows of `bench_chain_<procs>`
      (`--tier optout` picks the persistence-off twin of the 4-proc chain), each
      seeded with `"<seq>,<send_ts_ns>"`, which the chain passes through to the
      sink. `-c N` keeps N in flight (closed loop), `--rate R` spawns R/s open
      loop; neither = one burst.
    * `turns` — spawn `-n` `bench_loop_workflow` instances, send `-m` turns each
      round-robin (`--rate` total turns/s, else unpaced under an in-flight cap)
      and measure the reply. `--duration S` sizes `-m` from the rate.
      `--kill-runner-at S` SIGKILLs one runner S seconds into the turns.
    * `sleep` — spawn `-n` loops, one turn each, wait until every instance has
      parked; report the controller's resting cost per sleeper and the
      convergence tick time, then a message wake storm (one turn to every
      sleeper at once) and, with `--wake-after-ms`, a timer wake storm.
      `--keep` leaves the namespace on disk for `sleep-boot`.
    * `sleep-boot` — boot over the namespace `sleep --keep` left: time until
      every sleeper is back as a suspended Overwatch, then the same wake storm.

  `--park-idle-ms` (runner-side idle hysteresis before a loop parks) defaults
  to 500 for the sleep scenarios and the SDK's 5000 otherwise.
  """

  @namespace "bench_wf"
  @sink_id "bench_sink"
  @waiters :bench_wf_waiters

  @switches [
    scenario: :string,
    num: :integer,
    turns: :integer,
    procs: :integer,
    tier: :string,
    concurrency: :integer,
    rate: :integer,
    runners: :integer,
    park_idle_ms: :integer,
    wake_after_ms: :integer,
    duration: :integer,
    kill_runner_at: :integer,
    status_every: :integer,
    stall_timeout: :integer,
    keep: :boolean,
    warmup: :integer,
    spawn_concurrency: :integer,
    wake_rate: :integer,
    warm_turn: :boolean,
    no_wake: :boolean
  ]

  def run(args) do
    {opts, _, invalid} =
      OptionParser.parse(args,
        switches: @switches,
        aliases: [n: :num, m: :turns, k: :procs, c: :concurrency]
      )

    if invalid != [], do: raise("unknown options: #{inspect(invalid)}")

    cfg = config(opts)

    log_level = String.to_atom(System.get_env("BENCH_LOG_LEVEL", "warning"))
    Logger.configure(level: log_level)
    Application.put_env(:logger, :level, log_level)

    if cfg.scenario != :sleep_boot, do: reset_namespace()

    # The handler is attached before the controller boots so a sleeper restored
    # during boot (`sleep-boot`) is counted like one that parked at run time.
    Mix.Task.run("loadpaths")
    {:ok, _} = Application.ensure_all_started(:telemetry)
    :ets.new(@waiters, [:named_table, :public, read_concurrency: true])
    counters = attach_telemetry(self())
    Process.flag(:trap_exit, true)

    boot_t0 = now_ns()
    Mix.Task.run("app.start")
    Logger.configure(level: log_level)
    Fleet.preload_code()
    Process.put(:bench_runtime, :rust)

    print_header(cfg)

    case cfg.scenario do
      :chain -> run_chain(cfg, counters)
      :turns -> run_turns(cfg, counters)
      :sleep -> run_sleep(cfg, counters)
      :sleep_boot -> run_sleep_boot(cfg, counters, boot_t0)
    end
  end

  # ---------------------------------------------------------------- config --

  defp config(opts) do
    scenario =
      case opts[:scenario] || "chain" do
        "chain" -> :chain
        "turns" -> :turns
        "sleep" -> :sleep
        "sleep-boot" -> :sleep_boot
        other -> raise "unknown --scenario #{inspect(other)}"
      end

    procs = opts[:procs] || 4
    tier = opts[:tier] || "durable"

    factory =
      case {scenario, tier} do
        {:chain, "durable"} when procs in [1, 4, 16] -> "bench_chain_#{procs}"
        {:chain, "optout"} when procs == 4 -> "bench_chain_optout_4"
        {:chain, _} -> raise "chain fixtures: --procs 1|4|16 --tier durable, or --procs 4 --tier optout"
        _ -> "bench_loop_workflow"
      end

    n = opts[:num] || 100
    rate = opts[:rate] || 0

    turns =
      cond do
        opts[:duration] && rate > 0 -> max(div(opts[:duration] * rate, n), 1)
        true -> opts[:turns] || 10
      end

    %{
      scenario: scenario,
      n: n,
      factory: factory,
      procs: procs,
      tier: tier,
      concurrency: opts[:concurrency] || 0,
      rate: rate,
      turns: turns,
      runners: opts[:runners] || 2,
      park_idle_ms:
        opts[:park_idle_ms] || if(scenario in [:sleep, :sleep_boot], do: 500, else: 5_000),
      wake_after_ms: opts[:wake_after_ms],
      duration: opts[:duration],
      kill_runner_at: opts[:kill_runner_at],
      status_every: opts[:status_every] || 0,
      stall_timeout: (opts[:stall_timeout] || 60) * 1_000,
      keep: opts[:keep] || false,
      spawn_concurrency: opts[:spawn_concurrency] || 1_000,
      wake_rate: opts[:wake_rate] || 0,
      warm_turn: opts[:warm_turn] || false,
      no_wake: opts[:no_wake] || false,
      warmup: opts[:warmup] || if(scenario == :chain, do: min(20, n), else: 0)
    }
  end

  defp print_header(cfg) do
    IO.puts("""
    Flezha workflow bench
      scenario:     #{cfg.scenario}
      factory:      #{cfg.factory}
      instances:    #{cfg.n}
      turns/inst:   #{if cfg.scenario == :turns, do: cfg.turns, else: "-"}
      concurrency:  #{if cfg.concurrency > 0, do: cfg.concurrency, else: "burst (≤#{cfg.spawn_concurrency} starts in flight)"}
      rate:         #{if cfg.rate == 0, do: "unpaced", else: "#{cfg.rate}/s"}
      runners:      #{cfg.runners}
      park idle:    #{cfg.park_idle_ms} ms#{if cfg.wake_after_ms, do: ", wake after #{cfg.wake_after_ms} ms", else: ""}
    """)
  end

  defp reset_namespace do
    for dir <- [
          Path.join(["app_data", "state", @namespace]),
          Path.join(["app_data", "process_hub", "#{@namespace}_hub"])
        ] do
      File.rm_rf!(dir)
    end
  end

  defp runner_env(cfg) do
    [{"FLEZHA_WORKFLOW_PARK_IDLE_MS", cfg.park_idle_ms}] ++
      if(cfg.wake_after_ms, do: [{"FLEZHA_BENCH_WAKE_AFTER_MS", cfg.wake_after_ms}], else: [])
  end

  # ------------------------------------------------------------- telemetry --

  # Completions, parks and rehydrations reach the bench process as messages
  # (each stamped where it happened); barrier commits are only counted.
  defp attach_telemetry(pid) do
    counters = :counters.new(3, [])

    :telemetry.attach_many(
      "bench-workflow",
      [
        [:flezha, :workflow, :completed],
        [:flezha, :workflow, :suspended],
        [:flezha, :workflow, :rehydrating],
        [:flezha, :workflow, :failed],
        [:flezha, :state, :proc_commit],
        [:flezha, :state, :iter_commit]
      ],
      &__MODULE__.handle_event/4,
      {pid, counters}
    )

    counters
  end

  @doc false
  def handle_event([:flezha, :workflow, :completed], _m, %{operative_id: id}, {pid, _c}) do
    msg = {:wf_completed, id, now_ns()}

    case :ets.lookup(@waiters, id) do
      [{_, _, _, waiter}] when is_pid(waiter) -> send(waiter, msg)
      _ -> :ok
    end

    send(pid, msg)
  end

  def handle_event([:flezha, :workflow, :suspended], _m, %{operative_id: id}, {pid, _c}),
    do: send(pid, {:wf_suspended, id, now_ns()})

  def handle_event([:flezha, :workflow, :rehydrating], _m, %{operative_id: id}, {pid, _c}),
    do: send(pid, {:wf_rehydrating, id, now_ns()})

  def handle_event([:flezha, :workflow, :failed], _m, _md, {_pid, c}),
    do: :counters.add(c, 3, 1)

  def handle_event([:flezha, :state, :proc_commit], _m, _md, {_pid, c}),
    do: :counters.add(c, 1, 1)

  def handle_event([:flezha, :state, :iter_commit], _m, _md, {_pid, c}),
    do: :counters.add(c, 2, 1)

  defp commits(counters),
    do: %{proc: :counters.get(counters, 1), iter: :counters.get(counters, 2), failed: :counters.get(counters, 3)}

  # ------------------------------------------------------------ scenarios --

  defp run_chain(cfg, counters) do
    Fleet.setup(@namespace, cfg.runners, runner_env(cfg))

    try do
      sink = sink(self())
      total = cfg.warmup + cfg.n
      st = new_state(cfg, total, cfg.warmup)

      st =
        if cfg.warmup > 0 do
          IO.puts("Warmup: #{cfg.warmup} workflows (burst, excluded)...")
          spawn_burst(cfg, sink, 0..(cfg.warmup - 1), self())
          st = collect(st, &(&1.completed >= cfg.warmup and &1.out.seen >= cfg.warmup))
          if st.stalled, do: raise("warmup stalled")
          st
        else
          st
        end

      seqs = cfg.warmup..(total - 1)
      c0 = commits(counters)
      mode = spawn_mode(cfg)
      IO.puts("Spawning #{cfg.n} × #{cfg.factory} (#{mode})...")
      t0 = now_ns()

      case mode do
        "burst" -> spawn_burst(cfg, sink, seqs, self())
        "closed loop" -> spawn_closed_loop(cfg, sink, seqs, self())
        "open loop" -> spawn_paced(cfg, sink, seqs, self())
      end

      st = collect(st, &(&1.completed >= total and &1.out.seen >= total))
      t1 = now_ns()
      c1 = commits(counters)

      spawn_window = (st.last_spawn_at || t1) - t0
      # First spawn → last completion/output; a stall's idle wait is not work.
      whole_window = max(max(st.last_done_at || t1, st.last_out_at || t1) - t0, 1)
      missing = total - st.out.seen
      Process.sleep(2_000)
      {tick_us, _} = :timer.tc(fn -> Flezha.Convergence.Loop.tick_now() end)

      IO.puts("""

      === chain summary: #{cfg.factory}, n=#{cfg.n}, #{mode} ===
      spawned:              #{st.spawned} ok, #{st.spawn_errors} errors, in #{fmt_ms(spawn_window)} ms → #{rate(cfg.n, spawn_window)} spawns/s
      start_operative call: #{Tally.latency_line(st.spawn)}
      seed → sink output:   #{Tally.latency_line(st.out)}
      seed → completed:     #{Tally.latency_line(st.done)}
      completed:            #{st.completed - cfg.warmup} in #{fmt_ms(whole_window)} ms → #{rate(cfg.n, whole_window)} workflows/s
      sink outputs:         #{st.out.received} measured, #{st.out.duplicates} dup, #{missing} missing, #{st.out.foreign} foreign
      proc commits:         #{c1.proc - c0.proc} (#{Float.round((c1.proc - c0.proc) / max(cfg.n, 1), 2)}/workflow) → #{rate(c1.proc - c0.proc, whole_window)} commits/s; failed=#{c1.failed - c0.failed}
      state dir:            #{Tally.state_dir_summary(@namespace)}; fsync p99 #{Tally.fsync_p99_ms()} ms
      beam:                 #{mib(:erlang.memory(:total))} MiB, #{length(Process.list())} processes
      convergence tick:     #{fmt_ms(tick_us * 1_000)} ms with #{st.completed} completed instances retained on disk
      process kinds:        #{process_breakdown(process_sample())}
      #{verdict(st, missing)}
      """)

      diagnose_missing(st)
    after
      Fleet.cleanup(@namespace)
    end
  end

  defp run_turns(cfg, counters) do
    Fleet.setup(@namespace, cfg.runners, runner_env(cfg))

    try do
      sink = sink(self())
      n = cfg.n
      total = n + n * cfg.turns
      st = new_state(cfg, total, n)

      {st, ids} = spawn_population(cfg, sink, st)

      # Every loop starts empty and parks once idle; begin from that known
      # state so the first turn of each is a wake, and say so.
      IO.puts("  waiting for the #{n} loops to park (idle #{cfg.park_idle_ms} ms)...")
      st = collect(st, &(&1.suspended >= n), cfg.park_idle_ms + 30_000)
      IO.puts("  #{st.suspended} parked")

      IO.puts("Warmup: one turn per loop (excluded)...")
      # Re-parks are counted from the wake, not from the last reply: a fast
      # loop parks before the slow ones have answered.
      st = %{st | suspended: 0}
      Enum.each(0..(n - 1), &send_turn(ids, &1))
      st = collect(st, &(&1.out.seen >= n))
      if st.stalled, do: raise("warmup stalled")
      st = collect(st, &(&1.suspended >= n), cfg.park_idle_ms + 30_000)

      IO.puts(
        "Sending #{n * cfg.turns} turns (#{cfg.turns}/loop, " <>
          "#{if cfg.rate == 0, do: "unpaced", else: "#{cfg.rate}/s"})..."
      )

      c0 = commits(counters)
      st = %{st | suspended: 0, rehydrating: 0}
      if cfg.kill_runner_at, do: Process.send_after(self(), :kill_runner, cfg.kill_runner_at * 1_000)
      t0 = now_ns()
      received = :counters.new(1, [])
      start_turn_pacer(cfg, ids, n..(total - 1), received)
      st = collect(%{st | received_counter: received}, &(&1.out.seen >= total))
      t1 = now_ns()
      c1 = commits(counters)

      if st.stalled, do: diagnose_stall(st, ids)

      window = max(t1 - t0, 1)
      measured = n * cfg.turns
      missing = total - st.out.seen

      IO.puts("""

      === turns summary: n=#{n} loops × #{cfg.turns} turns, #{if cfg.rate == 0, do: "unpaced", else: "#{cfg.rate}/s"} ===
      turn → reply:         #{Tally.latency_line(st.out)}
      throughput:           #{st.out.received} replies in #{fmt_ms(window)} ms → #{rate(st.out.received, window)} turns/s
      integrity:            #{st.out.duplicates} dup, #{missing} missing, #{st.out.foreign} foreign
      parks / wakes:        #{st.suspended} parked, #{st.rehydrating} rehydrated during the turns
      commits:              #{c1.proc - c0.proc} proc (#{Float.round((c1.proc - c0.proc) / max(measured, 1), 2)}/turn), #{c1.iter - c0.iter} iter (#{Float.round((c1.iter - c0.iter) / max(measured, 1), 2)}/turn) → #{rate(c1.proc - c0.proc, window)} proc commits/s; failed=#{c1.failed - c0.failed}
      #{kill_line(st)}state dir:            #{Tally.state_dir_summary(@namespace)}; fsync p99 #{Tally.fsync_p99_ms()} ms
      beam:                 #{mib(:erlang.memory(:total))} MiB, #{length(Process.list())} processes
      #{verdict(st, missing)}
      """)
    after
      Fleet.cleanup(@namespace)
    end
  end

  defp run_sleep(cfg, counters) do
    Fleet.setup(@namespace, cfg.runners, runner_env(cfg))

    try do
      sink = sink(self())
      n = cfg.n
      st = new_state(cfg, 2 * n, 0)
      mem0 = memory_snapshot()

      {st, ids} = spawn_population(cfg, sink, st)

      # `--warm-turn` gives every loop one turn of state first — and, with the
      # loops parking behind the spawn, is itself an n-wide wake storm.
      st =
        if cfg.warm_turn do
          IO.puts("One turn per loop...")
          Enum.each(0..(n - 1), &send_turn(ids, &1))
          st = collect(st, &(&1.out.seen >= n))

          if st.stalled do
            diagnose_missing(st, id_of(ids))
            raise("warm turn stalled")
          end

          IO.puts("  turn → reply: #{Tally.latency_line(st.out)}")
          st
        else
          st
        end

      IO.puts("Waiting for every loop to park...")
      c_turn = commits(counters)

      # Parks counted since the spawn: a loop parks half a second after its own
      # start, long before the last one is spawned.
      st = collect(st, &(&1.suspended >= n), cfg.park_idle_ms + 180_000)

      if st.stalled do
        diagnose_stall(st, ids)
        raise("parking stalled at #{st.suspended}/#{n}")
      end
      sleep_report(cfg, mem0, n, c_turn)
      wake_storms(cfg, st, ids, counters)
    after
      if cfg.keep do
        IO.puts("--keep: namespace #{@namespace} left on disk with its #{cfg.n} sleepers; run `--scenario sleep-boot -n #{cfg.n}` next")
      else
        Fleet.cleanup(@namespace)
      end
    end
  end

  defp run_sleep_boot(cfg, counters, boot_t0) do
    n = cfg.n
    mem0 = memory_snapshot()
    st = new_state(cfg, 2 * n, 0)

    # The namespace hub — and with it the recovery replay of its declared
    # children — starts when the first runner connects, so the restore is
    # measured from that connect, not from the VM start.
    Fleet.setup(@namespace, cfg.runners, runner_env(cfg))
    connected_at = now_ns()
    IO.puts("Booted over #{@namespace}; runners connected #{fmt_ms(connected_at - boot_t0)} ms after app start; waiting for #{n} sleepers to be restored as suspended...")
    st = collect(st, &(&1.suspended >= n), 300_000)
    restored_at = now_ns()

    if st.stalled do
      IO.puts("  only #{st.suspended}/#{n} restored — see the boot gate / reconcile grace")
    else
      IO.puts("  #{n} restored #{fmt_ms(restored_at - connected_at)} ms after the runners connected")
    end

    :ets.delete_all_objects(@waiters)
    ids = st.park_at |> Map.keys() |> Enum.sort() |> List.to_tuple()
    for {id, seq} <- Enum.with_index(Tuple.to_list(ids)), do: :ets.insert(@waiters, {id, seq, nil, nil})

    sleep_report(cfg, mem0, tuple_size(ids), commits(counters))

    try do
      wake_storms(cfg, st, ids, counters)
    after
      Fleet.cleanup(@namespace)
    end
  end

  defp sleep_report(cfg, mem0, n, c) do
    Process.sleep(3_000)
    mem1 = memory_snapshot()
    ticks = for _ <- 1..3, do: :timer.tc(fn -> Flezha.Convergence.Loop.tick_now() end) |> elem(0)
    {bytes, wals, snaps} = Tally.state_dir_stats(@namespace)
    sample = process_sample()
    n = max(n, 1)

    IO.puts("""

    === resting cost: #{n} suspended #{cfg.factory} ===
    beam total:           #{mib(mem0.total)} → #{mib(mem1.total)} MiB (+#{Float.round((mem1.total - mem0.total) / n / 1024, 1)} KiB/sleeper)
    beam processes heap:  #{mib(mem0.processes)} → #{mib(mem1.processes)} MiB (+#{Float.round((mem1.processes - mem0.processes) / n / 1024, 1)} KiB/sleeper)
    beam ets:             #{mib(mem0.ets)} → #{mib(mem1.ets)} MiB
    processes:            #{mem0.count} → #{mem1.count} (+#{Float.round((mem1.count - mem0.count) / n, 2)}/sleeper); #{process_breakdown(sample)}
    largest processes:    #{largest_processes(sample)}
    beam binary:          #{mib(:erlang.memory(:binary))} MiB; an Overwatch heap: #{overwatch_heap_kib()} KiB; its journal writer: #{writer_heap_kib()} KiB
    state dir:            #{div(bytes, 1024)} KiB (#{Float.round(bytes / n / 1024, 1)} KiB/sleeper), #{wals} wal, #{snaps} snap
    convergence tick:     #{Enum.map_join(ticks, " / ", &"#{fmt_ms(&1 * 1_000)} ms")} (3 samples)
    commits so far:       #{c.proc} proc, #{c.iter} iter
    """)
  end

  # `--no-wake` ends a sleep cell at the resting report — a `--keep` that
  # leaves every sleeper suspended with nothing owed, for a clean boot.
  defp wake_storms(%{no_wake: true}, _st, _ids, _counters), do: :ok

  defp wake_storms(cfg, st, ids, counters) do
    n = tuple_size(ids)

    # Message wake: one turn to every sleeper — as fast as the casts go out,
    # or paced at `--wake-rate` wakes/s.
    IO.puts("Message wake storm: #{n} turns #{if cfg.wake_rate > 0, do: "at #{cfg.wake_rate}/s", else: "at once"}...")
    st = %{st | out: Tally.new(2 * n), rehydrating: 0, suspended: 0, park_at: %{}}
    c0 = commits(counters)
    t0 = now_ns()

    Enum.each(n..(2 * n - 1), fn seq ->
      if cfg.wake_rate > 0 do
        due_at = t0 + div((seq - n) * 1_000_000_000, cfg.wake_rate)
        wait = div(due_at - now_ns(), 1_000_000)
        if wait > 0, do: Process.sleep(wait)
      end

      send_turn(ids, seq)
    end)

    sent_dt = now_ns() - t0
    st = collect(st, &(&1.out.seen >= n))
    t1 = now_ns()
    c1 = commits(counters)

    IO.puts("""
      casts out in #{fmt_ms(sent_dt)} ms; last reply #{fmt_ms(t1 - t0)} ms after the first cast → #{rate(n, t1 - t0)} wakes/s
      wake → reply:         #{Tally.latency_line(st.out)}
      rehydrations:         #{st.rehydrating} (expect #{n}); replies #{st.out.received}, dup #{st.out.duplicates}, missing #{n - st.out.seen}
      commits:              #{c1.proc - c0.proc} proc, #{c1.iter - c0.iter} iter
      #{if st.stalled, do: "RESULT: STALLED", else: ""}
    """)

    unless st.stalled do
      IO.puts("Waiting for all #{n} to park again...")
      st = collect(st, &(&1.suspended >= n), cfg.park_idle_ms + 60_000)
      IO.puts("  #{st.suspended} parked; #{st.out.seen - n} extra replies meanwhile")

      if cfg.wake_after_ms && not st.stalled do
        IO.puts("Timer wake storm: every sleeper's #{cfg.wake_after_ms} ms deadline...")
        st = %{st | timer_wakes: [], rehydrating: 0}
        t0 = now_ns()
        st = collect(st, &(length(&1.timer_wakes) >= n), cfg.wake_after_ms + 120_000)
        t1 = now_ns()

        late = Tally.new(n)

        late =
          Enum.reduce(st.timer_wakes, late, fn {id, at}, acc ->
            case {:ets.lookup(@waiters, id), Map.get(st.park_at, id)} do
              {[{_, seq, _, _}], parked_at} when is_integer(parked_at) ->
                deadline = parked_at + cfg.wake_after_ms * 1_000_000
                Tally.record({seq, max(at - deadline, 0)}, acc)

              _ ->
                acc
            end
          end)

        IO.puts("""
          timer wakes:          #{length(st.timer_wakes)} of #{n} in #{fmt_ms(t1 - t0)} ms; rehydrations #{st.rehydrating}
          deadline → reply:     #{Tally.latency_line(late)}
          #{if st.stalled, do: "RESULT: STALLED", else: ""}
        """)
      end
    end
  end

  # ---------------------------------------------------------------- spawns --

  defp spawn_mode(%{concurrency: c}) when c > 0, do: "closed loop"
  defp spawn_mode(%{rate: r}) when r > 0, do: "open loop"
  defp spawn_mode(_), do: "burst"

  defp sink(recv_pid) do
    %Flezha.Plugin{
      id: @sink_id,
      namespace: nil,
      operative_id: nil,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: recv_pid, include_metadata: true},
      subscriptions: [{:on_output, []}]
    }
  end

  # A burst is bounded at `--spawn-concurrency` concurrent starts (default
  # 1000): wide enough to saturate the controller's start path, narrow enough
  # that a 10k spawn does not queue thousands of starts into the hub's
  # per-start timeout.
  defp spawn_burst(cfg, sink, seqs, main) do
    Task.start_link(fn ->
      seqs
      |> Task.async_stream(&spawn_one(cfg, sink, &1, main, nil),
        max_concurrency: cfg.spawn_concurrency,
        ordered: false,
        timeout: :infinity
      )
      |> Stream.run()
    end)
  end

  defp spawn_closed_loop(cfg, sink, first..last//_, main) do
    next = :atomics.new(1, [])
    :atomics.put(next, 1, first)

    for _ <- 1..cfg.concurrency do
      Task.start_link(fn -> closed_loop_worker(cfg, sink, next, last, main) end)
    end
  end

  defp closed_loop_worker(cfg, sink, next, last, main) do
    seq = :atomics.add_get(next, 1, 1) - 1

    if seq <= last do
      case spawn_one(cfg, sink, seq, main, self()) do
        {:ok, id} ->
          receive do
            {:wf_completed, ^id, _} -> :ok
          after
            120_000 -> :ok
          end

        :error ->
          :ok
      end

      closed_loop_worker(cfg, sink, next, last, main)
    end
  end

  defp spawn_paced(cfg, sink, seqs, main) do
    Task.start_link(fn -> paced_loop(cfg, sink, Enum.to_list(seqs), now_ns(), 0, main) end)
  end

  defp paced_loop(_cfg, _sink, [], _t0, _issued, _main), do: :ok

  defp paced_loop(cfg, sink, pending, t0, issued, main) do
    due = trunc((now_ns() - t0) / 1.0e9 * cfg.rate) - issued
    {now, rest} = Enum.split(pending, max(due, 0))
    Enum.each(now, fn seq -> Task.start(fn -> spawn_one(cfg, sink, seq, main, nil) end) end)
    Process.sleep(5)
    paced_loop(cfg, sink, rest, t0, issued + length(now), main)
  end

  defp spawn_one(cfg, sink, seq, main, waiter) do
    id = "wf_#{seq}"
    seed_ts = now_ns()
    :ets.insert(@waiters, {id, seq, seed_ts, waiter})

    # A run-once chain reads its entry from the spawn args (JSON of
    # `init_values`); a loop reads its inbox, so it starts empty.
    init_values = if cfg.scenario == :chain, do: %{"s" => "#{seq},#{seed_ts}"}, else: %{}

    builder = %Builder{
      id: id,
      factory_id: cfg.factory,
      kind: :workflow,
      init_values: init_values,
      opts: [plugins: [sink]]
    }
    t0 = now_ns()

    case OperativeManager.start_operative(@namespace, builder) do
      {:ok, _info} ->
        send(main, {:spawned, seq, now_ns() - t0, now_ns()})
        {:ok, id}

      error ->
        send(main, {:spawn_error, seq, error})
        :error
    end
  end

  # Spawn the loop population (burst) and name it: `{state, ids}` with the
  # start-call latencies tallied. A failed spawn ends the cell.
  defp spawn_population(cfg, sink, st) do
    n = cfg.n
    IO.puts("Spawning #{n} × #{cfg.factory}...")
    t0 = now_ns()
    spawn_burst(cfg, sink, 0..(n - 1), self())
    st = collect(st, &(&1.spawned + &1.spawn_errors >= n))
    if st.spawn_errors > 0, do: raise("#{st.spawn_errors} spawns failed")
    IO.puts("  spawned in #{fmt_ms(now_ns() - t0)} ms; start_operative #{Tally.latency_line(st.spawn)}")
    {st, List.to_tuple(for seq <- 0..(n - 1), do: "wf_#{seq}")}
  end

  # ----------------------------------------------------------------- turns --

  defp send_turn(ids, seq) do
    id = elem(ids, rem(seq, tuple_size(ids)))
    OperativeManager.send_io(id, @namespace, "#{seq},#{now_ns()}", %{"channel" => "in"})
  end

  # Unpaced turns ride an in-flight cap (a few per loop) so the Overwatch
  # mailboxes stay bounded and the measurement is the engine, not the queue.
  defp start_turn_pacer(cfg, ids, first..last//_, received) do
    cap = max(4 * tuple_size(ids), 64)
    Task.start_link(fn -> turn_loop(cfg, ids, first, last, now_ns(), first, received, cap) end)
  end

  defp turn_loop(_cfg, _ids, _first, last, _t0, next, _received, _cap) when next > last, do: :ok

  defp turn_loop(cfg, ids, first, last, t0, next, received, cap) do
    sent = next - first

    allowed =
      if cfg.rate > 0 do
        trunc((now_ns() - t0) / 1.0e9 * cfg.rate) - sent
      else
        cap - (sent - :counters.get(received, 1))
      end

    upto = min(next + max(allowed, 0) - 1, last)
    if upto >= next, do: Enum.each(next..upto, &send_turn(ids, &1))
    if upto < last, do: Process.sleep(if(cfg.rate > 0, do: 5, else: 1))
    turn_loop(cfg, ids, first, last, t0, upto + 1, received, cap)
  end

  defp kill_one_runner do
    case Process.get(:bench_ports, []) do
      [port | _] ->
        case Port.info(port, :os_pid) do
          {:os_pid, os_pid} ->
            System.cmd("kill", ["-9", Integer.to_string(os_pid)])
            os_pid

          _ ->
            nil
        end

      [] ->
        nil
    end
  end

  defp kill_line(%{killed_at: nil}), do: ""

  defp kill_line(st) do
    recovered =
      case st.first_out_after_kill do
        nil -> "no reply after the kill"
        at -> "first reply #{fmt_ms(at - st.killed_at)} ms after the kill"
      end

    "runner kill:          os pid #{st.killed_pid} at +#{st.cfg.kill_runner_at} s; #{recovered}; #{st.rehydrating} rehydrations after it\n"
  end

  # --------------------------------------------------------------- collect --

  defp new_state(cfg, expected, prime) do
    %{
      cfg: cfg,
      out: Tally.new(expected, prime),
      spawn: Tally.new(expected, 0),
      done: Tally.new(expected, prime),
      spawned: 0,
      spawn_errors: 0,
      completed: 0,
      suspended: 0,
      rehydrating: 0,
      park_at: %{},
      timer_wakes: [],
      last_spawn_at: nil,
      last_done_at: nil,
      last_out_at: nil,
      received_counter: nil,
      killed_at: nil,
      killed_pid: nil,
      first_out_after_kill: nil,
      stalled: false,
      last_progress: now_ms(),
      status_at: now_ms(),
      status_seen: 0
    }
  end

  # Drain until `done?` holds; a `timeout` (ms, default the stall timeout) with
  # no progress marks the state stalled and returns it. Each phase starts
  # fresh: a stall is a verdict on one phase, never carried into the next.
  defp collect(st, done?, timeout \\ nil) do
    timeout = timeout || st.cfg.stall_timeout
    do_collect(%{st | stalled: false, last_progress: now_ms()}, done?, timeout)
  end

  defp do_collect(st, done?, timeout) do
    cond do
      done?.(st) ->
        st

      st.stalled ->
        st

      true ->
        receive do
          msg -> do_collect(handle(msg, st), done?, timeout)
        after
          500 ->
            if now_ms() - st.last_progress > timeout do
              IO.puts(
                "\nSTALLED: no progress for #{timeout} ms (outputs #{st.out.seen}/#{st.out.expected}, " <>
                  "spawned #{st.spawned}, completed #{st.completed}, suspended #{st.suspended})"
              )

              %{st | stalled: true}
            else
              do_collect(maybe_status(st), done?, timeout)
            end
        end
    end
  end

  defp handle({:spawned, seq, dur_ns, at}, st) do
    st = %{st | spawn: Tally.record({seq, dur_ns}, st.spawn), spawned: st.spawned + 1, last_spawn_at: at}
    progress(st)
  end

  defp handle({:spawn_error, seq, error}, st) do
    if st.spawn_errors < 3, do: IO.puts("  spawn wf_#{seq} failed: #{inspect(error)}")
    progress(%{st | spawn_errors: st.spawn_errors + 1})
  end

  defp handle({:message_forward, msg, md}, st) do
    now = now_ns()
    if st.received_counter, do: :counters.add(st.received_counter, 1, 1)

    st =
      case parse_payload(msg) do
        {:turn, seq, ts} ->
          # A reply to a turn sent by a previous VM (`sleep-boot` over a kept
          # namespace) carries that VM's monotonic clock: count it, time it 0.
          st = %{st | out: Tally.record({seq, max(now - ts, 0)}, st.out), last_out_at: now}
          if st.killed_at && is_nil(st.first_out_after_kill), do: %{st | first_out_after_kill: now}, else: st

        :timer_wake ->
          id = md |> Map.get("flz.entity", "") |> String.split("/") |> hd()
          %{st | timer_wakes: [{id, now} | st.timer_wakes]}

        :unknown ->
          %{st | out: %{st.out | foreign: st.out.foreign + 1}}
      end

    progress(st)
  end

  defp handle({:wf_completed, id, at}, st) do
    st =
      case :ets.lookup(@waiters, id) do
        [{_, seq, seed_ts, _}] when is_integer(seed_ts) ->
          %{st | done: Tally.record({seq, at - seed_ts}, st.done)}

        _ ->
          st
      end

    progress(%{st | completed: st.completed + 1, last_done_at: at})
  end

  defp handle({:wf_suspended, id, at}, st),
    do: progress(%{st | suspended: st.suspended + 1, park_at: Map.put(st.park_at, id, at)})

  defp handle({:wf_rehydrating, _id, _at}, st), do: %{st | rehydrating: st.rehydrating + 1}

  defp handle(:kill_runner, st) do
    pid = kill_one_runner()
    IO.puts("\n  SIGKILL runner os pid #{inspect(pid)} at +#{st.cfg.kill_runner_at} s")
    %{st | killed_at: now_ns(), killed_pid: pid}
  end

  defp handle({:EXIT, pid, reason}, _st) when is_pid(pid) and reason not in [:normal, :killed, :shutdown],
    do: raise("bench worker #{inspect(pid)} crashed: #{inspect(reason)}")

  defp handle(_other, st), do: st

  defp parse_payload(msg) do
    body =
      case to_string(msg) do
        "reply:" <> rest -> rest
        "{" <> _ = json -> json |> Jason.decode!() |> Map.get("s", "?")
        other -> other
      end

    case body do
      "" ->
        :timer_wake

      _ ->
        with [seq, ts] <- String.split(body, ",", parts: 2),
             {seq, ""} <- Integer.parse(seq),
             {ts, ""} <- Integer.parse(ts) do
          {:turn, seq, ts}
        else
          _ -> :unknown
        end
    end
  end

  defp progress(st), do: %{st | last_progress: now_ms()}

  defp maybe_status(%{cfg: %{status_every: every}} = st) when every > 0 do
    now = now_ms()

    if now - st.status_at >= every * 1_000 do
      rate = (st.out.seen - st.status_seen) * 1_000 / max(now - st.status_at, 1)

      IO.puts(
        "  [status] #{st.out.seen}/#{st.out.expected} replies, #{round(rate)}/s, dup=#{st.out.duplicates} " <>
          "parked=#{st.suspended} rehydrated=#{st.rehydrating} | state #{Tally.state_dir_summary(@namespace)} | " <>
          "fsync p99 #{Tally.fsync_p99_ms()} ms | beam #{mib(:erlang.memory(:total))} MiB, #{length(Process.list())} procs"
      )

      %{st | status_at: now, status_seen: st.out.seen}
    else
      st
    end
  end

  defp maybe_status(st), do: st

  # The full stall picture: which loops never answered and where their
  # control channels sit. `ids` is the loop tuple; a chain uses the default id.
  defp diagnose_stall(st, ids) do
    diagnose_missing(st, id_of(ids))
    diagnose_channels()
  end

  # The instance id for a turn seq, round-robin over the loop tuple.
  defp id_of(ids), do: &elem(ids, rem(&1, tuple_size(ids)))

  # Name what never arrived — the seqs without an output (and, for a chain,
  # without a completion) — and what the controller knows about each: the
  # journal's last entries, the inbox it still owes, the Overwatch's belief.
  defp diagnose_missing(st, id_of \\ &"wf_#{&1}") do
    no_output = Tally.missing(st.out)
    no_done = Tally.missing(st.done)

    if no_output != [] or no_done != [] do
      IO.puts("missing outputs (seq): #{inspect(no_output)}; missing completions (seq): #{inspect(no_done)}")

      for seq <- Enum.uniq(no_output ++ no_done) |> Enum.take(5) do
        id = id_of.(seq)
        {:ok, entries} = Flezha.State.ProcJournal.read(id)
        journal = entries |> Enum.map(&{&1.type, &1.iter}) |> Enum.take(-4)
        undelivered = id |> Flezha.State.ProcInbox.undelivered() |> Enum.map(& &1.offset)

        overwatch =
          case OperativeManager.get_operative_pid(@namespace, id) do
            {:ok, pid} ->
              pid
              |> GenServer.call(:get_overwatch_state, 5_000)
              |> Map.take([:suspended, :runner_id, :migrate_task, :parked_in_flight, :init_error])

            other ->
              other
          end

        IO.puts(
          "  #{id}: journal=#{inspect(journal)} undelivered=#{inspect(undelivered)} " <>
            "completion=#{inspect(Flezha.State.Completion.read(id))} overwatch=#{inspect(overwatch)}"
        )
      end
    end
  end

  # The per-runner control processes' mailboxes: a wedged channel shows here.
  defp diagnose_channels do
    for runner_id <- Flezha.Runner.FleetManager.active_runners(@namespace),
        {kind, name} <- [
          {:control_channel, Flezha.Service.Identificator.control_channel(runner_id)},
          {:control_unit, Flezha.Service.Identificator.control_unit(runner_id)}
        ] do
      info =
        case GenServer.whereis(name) do
          nil -> :down
          pid -> Process.info(pid, [:message_queue_len, :current_function, :reductions])
        end

      IO.puts("  #{runner_id} #{kind}: #{inspect(info)}")
    end
  end

  # One verdict for a measured phase: stalled, clean, or an integrity failure
  # (a missing or duplicated seq).
  defp verdict(%{stalled: true}, _missing), do: "RESULT: STALLED"
  defp verdict(%{out: %{duplicates: 0}}, 0), do: "RESULT: clean"
  defp verdict(_st, _missing), do: "RESULT: INTEGRITY FAILURE"

  # ----------------------------------------------------------------- utils --

  defp writer_heap_kib do
    case Flezha.State.Supervisor.lookup(Flezha.State.Registry.journal_id("wf_0")) do
      {:ok, pid} -> pid |> Process.info(:memory) |> elem(1) |> div(1024)
      _ -> "?"
    end
  end

  # Heap before and after a forced collection: a sleeper that never hibernates
  # carries its bind-time garbage until the next major GC.
  defp overwatch_heap_kib do
    case OperativeManager.get_operative_pid(@namespace, "wf_0") do
      {:ok, pid} ->
        before = pid |> Process.info(:memory) |> elem(1) |> div(1024)
        :erlang.garbage_collect(pid)
        after_gc = pid |> Process.info(:memory) |> elem(1) |> div(1024)
        "#{before} → #{after_gc} after GC"

      _ ->
        "?"
    end
  end

  # One walk over every process — who it is (registered name, else initial
  # call) and its memory — feeding both resting-population summaries.
  defp process_sample do
    Enum.flat_map(Process.list(), fn pid ->
      case Process.info(pid, [:memory, :dictionary, :registered_name]) do
        [memory: m, dictionary: dict, registered_name: name] ->
          who =
            case {name, Keyword.get(dict, :"$initial_call")} do
              {n, _} when is_atom(n) and n != [] -> inspect(n)
              {_, {mod, _, _}} -> inspect(mod)
              _ -> "other"
            end

          [{who, m}]

        _ ->
          []
      end
    end)
  end

  # The five heaviest processes: what a resting population actually holds.
  defp largest_processes(sample) do
    sample
    |> Enum.sort_by(&elem(&1, 1), :desc)
    |> Enum.take(5)
    |> Enum.map_join(", ", fn {who, m} -> "#{who}=#{div(m, 1024)}KiB" end)
  end

  # The five most numerous process kinds — what a thousand completed or
  # sleeping instances leave behind on the controller.
  defp process_breakdown(sample) do
    sample
    |> Enum.map(&elem(&1, 0))
    |> Enum.frequencies()
    |> Enum.sort_by(&elem(&1, 1), :desc)
    |> Enum.take(5)
    |> Enum.map_join(", ", fn {m, c} -> "#{m}=#{c}" end)
  end

  defp memory_snapshot do
    m = :erlang.memory()
    %{total: m[:total], processes: m[:processes], ets: m[:ets], count: length(Process.list())}
  end

  defp now_ns, do: System.monotonic_time(:nanosecond)
  defp now_ms, do: System.monotonic_time(:millisecond)
  defp fmt_ms(ns), do: Float.round(ns / 1_000_000, 1)
  defp mib(bytes), do: div(bytes, 1_048_576)
  defp rate(count, window_ns), do: Float.round(count * 1.0e9 / max(window_ns, 1), 1)
end
