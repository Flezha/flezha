defmodule Mix.Tasks.Bench.Workflow.Scenarios do
  @moduledoc """
  The four scenarios and the spawners they drive: `chain` (spawn to completion),
  `turns` (iteration commits on a live loop), `sleep` (park, then wake storms)
  and `sleep-boot` (rehydrate a parked population at boot).

  Each sets its fleet up, drives `Mix.Tasks.Bench.Workflow.Run`'s state machine,
  and reports. Takes `cfg` as a plain map and names no other part of the task.
  """

  alias Flezha.Convergence.Loop
  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager
  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Report
  alias Mix.Tasks.Bench.Tally
  alias Mix.Tasks.Bench.Workflow.Run

  @sink_id "bench_sink"

  defp runner_env(cfg) do
    [{"FLEZHA_WORKFLOW_PARK_IDLE_MS", cfg.park_idle_ms}] ++
      if(cfg.wake_after_ms, do: [{"FLEZHA_BENCH_WAKE_AFTER_MS", cfg.wake_after_ms}], else: [])
  end

  # Completions, parks and rehydrations reach the bench process as messages
  # (each stamped where it happened); barrier commits are only counted.
  def run_chain(cfg, counters) do
    Fleet.setup(Run.namespace(), cfg.runners, runner_env(cfg))
    Report.emit()

    try do
      sink = sink(self())
      total = cfg.warmup + cfg.n
      st = warm_up(cfg, sink, Run.new_state(cfg, total, cfg.warmup))

      seqs = cfg.warmup..(total - 1)
      c0 = Run.commits(counters)
      mode = spawn_mode(cfg)
      IO.puts("Spawning #{cfg.n} × #{cfg.factory} (#{mode})...")
      t0 = Run.now_ns()

      case mode do
        "burst" -> spawn_burst(cfg, sink, seqs, self())
        "closed loop" -> spawn_closed_loop(cfg, sink, seqs, self())
        "open loop" -> spawn_paced(cfg, sink, seqs, self())
      end

      st = Run.collect(st, &(&1.completed >= total and &1.out.seen >= total))
      t1 = Run.now_ns()
      c1 = Run.commits(counters)

      report_chain(cfg, st, %{
        mode: mode,
        commits: {c0, c1},
        spawn_window: (st.last_spawn_at || t1) - t0,
        # First spawn → last completion/output; a stall's idle wait is not work.
        whole_window: max(max(st.last_done_at || t1, st.last_out_at || t1) - t0, 1),
        missing: total - st.out.seen
      })

      Run.diagnose_missing(st)
    after
      Fleet.cleanup(Run.namespace())
    end
  end

  # Excluded from every number: it only exists so the measured burst meets a
  # warm fleet.
  defp warm_up(%{warmup: warmup}, _sink, st) when warmup <= 0, do: st

  defp warm_up(cfg, sink, st) do
    IO.puts("Warmup: #{cfg.warmup} workflows (burst, excluded)...")
    spawn_burst(cfg, sink, 0..(cfg.warmup - 1), self())
    st = Run.collect(st, &(&1.completed >= cfg.warmup and &1.out.seen >= cfg.warmup))
    if st.stalled, do: raise("warmup stalled")
    st
  end

  defp report_chain(cfg, st, m) do
    {c0, c1} = m.commits
    Process.sleep(2_000)
    {tick_us, _} = :timer.tc(fn -> Loop.tick_now() end)

    Report.section(
      "chain summary: #{cfg.factory}, n=#{cfg.n}, #{m.mode}",
      Report.kv([
        {"spawned",
         "#{st.spawned} ok, #{st.spawn_errors} errors, in #{Report.ms_from_ns(m.spawn_window)} ms → #{Report.decimal(Run.rate(cfg.n, m.spawn_window))} spawns/s"},
        {"start_operative call", Tally.latency_line(st.spawn)},
        {"seed → sink output", Tally.latency_line(st.out)},
        {"seed → completed", Tally.latency_line(st.done)},
        {"completed",
         "#{st.completed - cfg.warmup} in #{Report.ms_from_ns(m.whole_window)} ms → #{Report.decimal(Run.rate(cfg.n, m.whole_window))} workflows/s"},
        {"sink outputs",
         "#{st.out.received} measured, #{st.out.duplicates} dup, #{m.missing} missing, #{st.out.foreign} foreign"},
        {"task commits",
         "#{c1.task - c0.task} (#{Report.decimal((c1.task - c0.task) / max(cfg.n, 1), 2)}/workflow) → #{Report.decimal(Run.rate(c1.task - c0.task, m.whole_window))} commits/s; failed=#{c1.failed - c0.failed}"},
        {"state dir",
         "#{Tally.state_dir_summary(Run.namespace())}; fsync p99 #{Tally.fsync_p99_ms()} ms"},
        {"beam", "#{Run.mib(:erlang.memory(:total))} MiB, #{length(Process.list())} processes"},
        {"convergence tick",
         "#{Report.ms_from_ns(tick_us * 1_000)} ms with #{st.completed} completed instances retained on disk"},
        {"process kinds", Run.process_breakdown(Run.process_sample())},
        {"result", Run.verdict(st, m.missing)}
      ])
    )

    Run.emit_result(
      scenario: :chain,
      factory: cfg.factory,
      throughput: Run.rate(cfg.n, m.whole_window),
      unit: "workflows/s",
      p50_ms: Run.p50_ms(st.done),
      commits_s: Run.rate(c1.task - c0.task, m.whole_window),
      integrity: Run.integrity(st, m.missing)
    )
  end

  def run_turns(cfg, counters) do
    Fleet.setup(Run.namespace(), cfg.runners, runner_env(cfg))
    Report.emit()

    try do
      sink = sink(self())
      n = cfg.n
      total = n + n * cfg.turns
      st = Run.new_state(cfg, total, n)

      {st, ids} = spawn_population(cfg, sink, st)
      st = park_then_warm_turn(cfg, st, ids, n)

      IO.puts(
        "Sending #{n * cfg.turns} turns (#{cfg.turns}/loop, " <>
          "#{if cfg.rate == 0, do: "unpaced", else: "#{cfg.rate}/s"})..."
      )

      c0 = Run.commits(counters)
      st = %{st | suspended: 0, rehydrating: 0}

      if cfg.kill_runner_at,
        do: Process.send_after(self(), :kill_runner, cfg.kill_runner_at * 1_000)

      t0 = Run.now_ns()
      received = :counters.new(1, [])
      start_turn_pacer(cfg, ids, n..(total - 1), received)
      st = Run.collect(%{st | received_counter: received}, &(&1.out.seen >= total))
      t1 = Run.now_ns()
      c1 = Run.commits(counters)

      if st.stalled, do: Run.diagnose_stall(st, ids)

      report_turns(cfg, st, %{
        commits: {c0, c1},
        n: n,
        window: max(t1 - t0, 1),
        measured: n * cfg.turns,
        missing: total - st.out.seen
      })
    after
      Fleet.cleanup(Run.namespace())
    end
  end

  # Every loop starts empty and parks once idle; begin from that known state so
  # the first turn of each is a wake, and say so. Re-parks are counted from the
  # wake, not from the last reply: a fast loop parks before the slow ones have
  # answered.
  defp park_then_warm_turn(cfg, st, ids, n) do
    IO.puts("  waiting for the #{n} loops to park (idle #{cfg.park_idle_ms} ms)...")
    st = Run.collect(st, &(&1.suspended >= n), cfg.park_idle_ms + 30_000)
    IO.puts("  #{st.suspended} parked")

    IO.puts("Warmup: one turn per loop (excluded)...")
    st = %{st | suspended: 0}
    Enum.each(0..(n - 1), &send_turn(ids, &1))
    st = Run.collect(st, &(&1.out.seen >= n))
    if st.stalled, do: raise("warmup stalled")
    Run.collect(st, &(&1.suspended >= n), cfg.park_idle_ms + 30_000)
  end

  defp report_turns(cfg, st, m) do
    {c0, c1} = m.commits
    %{n: n, window: window, measured: measured, missing: missing} = m

    Report.section(
      "turns summary: n=#{n} loops × #{cfg.turns} turns, #{if cfg.rate == 0, do: "unpaced", else: "#{cfg.rate}/s"}",
      Report.kv(
        [
          {"turn → reply", Tally.latency_line(st.out)},
          {"throughput",
           "#{st.out.received} replies in #{Report.ms_from_ns(window)} ms → #{Report.decimal(Run.rate(st.out.received, window))} turns/s"},
          {"integrity",
           "#{st.out.duplicates} dup, #{missing} missing, #{st.out.foreign} foreign"},
          {"parks / wakes",
           "#{st.suspended} parked, #{st.rehydrating} rehydrated during the turns"},
          {"commits",
           "#{c1.task - c0.task} task (#{Report.decimal((c1.task - c0.task) / max(measured, 1), 2)}/turn), #{c1.iter - c0.iter} iter (#{Report.decimal((c1.iter - c0.iter) / max(measured, 1), 2)}/turn) → #{Report.decimal(Run.rate(c1.task - c0.task, window))} task commits/s; failed=#{c1.failed - c0.failed}"}
        ] ++
          kill_rows(st) ++
          [
            {"state dir",
             "#{Tally.state_dir_summary(Run.namespace())}; fsync p99 #{Tally.fsync_p99_ms()} ms"},
            {"beam",
             "#{Run.mib(:erlang.memory(:total))} MiB, #{length(Process.list())} processes"},
            {"result", Run.verdict(st, missing)}
          ]
      )
    )

    Run.emit_result(
      scenario: :turns,
      factory: cfg.factory,
      throughput: Run.rate(st.out.received, window),
      unit: "turns/s",
      p50_ms: Run.p50_ms(st.out),
      commits_s: Run.rate(c1.task - c0.task, window),
      integrity: Run.integrity(st, missing)
    )
  end

  def run_sleep(cfg, counters) do
    Fleet.setup(Run.namespace(), cfg.runners, runner_env(cfg))
    Report.emit()

    try do
      sink = sink(self())
      n = cfg.n
      st = Run.new_state(cfg, 2 * n, 0)
      mem0 = Run.memory_snapshot()

      {st, ids} = spawn_population(cfg, sink, st)

      st = warm_turn(cfg, st, ids, n)

      IO.puts("Waiting for every loop to park...")
      c_turn = Run.commits(counters)

      # Parks counted since the spawn: a loop parks half a second after its own
      # start, long before the last one is spawned.
      st = Run.collect(st, &(&1.suspended >= n), cfg.park_idle_ms + 180_000)

      if st.stalled do
        Run.diagnose_stall(st, ids)
        raise("parking stalled at #{st.suspended}/#{n}")
      end

      sleep_report(cfg, mem0, n, c_turn)
      wake_storms(cfg, st, ids, counters)
    after
      if cfg.keep do
        IO.puts(
          "--keep: namespace #{Run.namespace()} left on disk with its #{cfg.n} sleepers; run `--scenario sleep-boot -n #{cfg.n}` next"
        )
      else
        Fleet.cleanup(Run.namespace())
      end
    end
  end

  # `--warm-turn` gives every loop one turn of state first — and, with the loops
  # parking behind the spawn, is itself an n-wide wake storm.
  defp warm_turn(%{warm_turn: false}, st, _ids, _n), do: st

  defp warm_turn(_cfg, st, ids, n) do
    IO.puts("One turn per loop...")
    Enum.each(0..(n - 1), &send_turn(ids, &1))
    st = Run.collect(st, &(&1.out.seen >= n))

    if st.stalled do
      Run.diagnose_missing(st, Run.id_of(ids))
      raise("warm turn stalled")
    end

    IO.puts("  turn → reply: #{Tally.latency_line(st.out)}")
    st
  end

  def run_sleep_boot(cfg, counters, boot_t0) do
    n = cfg.n
    mem0 = Run.memory_snapshot()
    st = Run.new_state(cfg, 2 * n, 0)

    # The namespace hub — and with it the recovery replay of its declared
    # children — starts when the first runner connects, so the restore is
    # measured from that connect, not from the VM start.
    Fleet.setup(Run.namespace(), cfg.runners, runner_env(cfg))
    Report.emit()
    connected_at = Run.now_ns()

    IO.puts(
      "Booted over #{Run.namespace()}; runners connected #{Report.ms_from_ns(connected_at - boot_t0)} ms after app start; waiting for #{n} sleepers to be restored as suspended..."
    )

    st = Run.collect(st, &(&1.suspended >= n), 300_000)
    restored_at = Run.now_ns()

    if st.stalled do
      IO.puts("  only #{st.suspended}/#{n} restored — see the boot gate / reconcile grace")
    else
      IO.puts(
        "  #{n} restored #{Report.ms_from_ns(restored_at - connected_at)} ms after the runners connected"
      )
    end

    :ets.delete_all_objects(Run.waiters())
    ids = st.park_at |> Map.keys() |> Enum.sort() |> List.to_tuple()

    for {id, seq} <- Enum.with_index(Tuple.to_list(ids)),
        do: :ets.insert(Run.waiters(), {id, seq, nil, nil})

    sleep_report(cfg, mem0, tuple_size(ids), Run.commits(counters))

    try do
      wake_storms(cfg, st, ids, counters)
    after
      Fleet.cleanup(Run.namespace())
    end
  end

  defp sleep_report(cfg, mem0, n, c) do
    Process.sleep(3_000)
    mem1 = Run.memory_snapshot()
    ticks = for _ <- 1..3, do: :timer.tc(fn -> Loop.tick_now() end) |> elem(0)
    {bytes, wals, snaps} = Tally.state_dir_stats(Run.namespace())
    sample = Run.process_sample()
    n = max(n, 1)

    Report.section(
      "resting cost: #{n} suspended #{cfg.factory}",
      Report.kv([
        {"beam total",
         "#{Run.mib(mem0.total)} → #{Run.mib(mem1.total)} MiB (+#{Report.decimal((mem1.total - mem0.total) / n / 1024, 1)} KiB/sleeper)"},
        {"beam processes heap",
         "#{Run.mib(mem0.processes)} → #{Run.mib(mem1.processes)} MiB (+#{Report.decimal((mem1.processes - mem0.processes) / n / 1024, 1)} KiB/sleeper)"},
        {"beam ets", "#{Run.mib(mem0.ets)} → #{Run.mib(mem1.ets)} MiB"},
        {"processes",
         "#{mem0.count} → #{mem1.count} (+#{Report.decimal((mem1.count - mem0.count) / n, 2)}/sleeper); #{Run.process_breakdown(sample)}"},
        {"largest processes", Run.largest_processes(sample)},
        {"beam binary",
         "#{Run.mib(:erlang.memory(:binary))} MiB; an Overwatch heap: #{Run.overwatch_heap_kib()} KiB; its journal writer: #{Run.writer_heap_kib()} KiB"},
        {"state dir",
         "#{div(bytes, 1024)} KiB (#{Report.decimal(bytes / n / 1024, 1)} KiB/sleeper), #{wals} wal, #{snaps} snap"},
        {"convergence tick",
         "#{Enum.map_join(ticks, " / ", &"#{Report.ms_from_ns(&1 * 1_000)} ms")} (3 samples)"},
        {"commits so far", "#{c.task} task, #{c.iter} iter"}
      ])
    )
  end

  # `--no-wake` ends a sleep run at the resting report — a `--keep` that
  # leaves every sleeper suspended with nothing owed, for a clean boot.
  defp wake_storms(%{no_wake: true}, _st, _ids, _counters), do: :ok

  defp wake_storms(cfg, st, ids, counters) do
    n = tuple_size(ids)

    # Message wake: one turn to every sleeper — as fast as the casts go out,
    # or paced at `--wake-rate` wakes/s.
    IO.puts(
      "Message wake storm: #{n} turns #{if cfg.wake_rate > 0, do: "at #{cfg.wake_rate}/s", else: "at once"}..."
    )

    st = %{st | out: Tally.new(2 * n), rehydrating: 0, suspended: 0, park_at: %{}}
    c0 = Run.commits(counters)
    t0 = Run.now_ns()

    Enum.each(n..(2 * n - 1), fn seq ->
      if cfg.wake_rate > 0 do
        due_at = t0 + div((seq - n) * 1_000_000_000, cfg.wake_rate)
        wait = div(due_at - Run.now_ns(), 1_000_000)
        if wait > 0, do: Process.sleep(wait)
      end

      send_turn(ids, seq)
    end)

    sent_dt = Run.now_ns() - t0
    st = Run.collect(st, &(&1.out.seen >= n))
    t1 = Run.now_ns()
    c1 = Run.commits(counters)

    report_wake_storm(cfg, st, n, %{commits: {c0, c1}, sent_dt: sent_dt, window: t1 - t0})

    unless st.stalled do
      IO.puts("Waiting for all #{n} to park again...")
      st = Run.collect(st, &(&1.suspended >= n), cfg.park_idle_ms + 60_000)
      IO.puts("  #{st.suspended} parked; #{st.out.seen - n} extra replies meanwhile")
      if cfg.wake_after_ms && not st.stalled, do: timer_wake_storm(cfg, st, n)
    end
  end

  defp report_wake_storm(cfg, st, n, m) do
    {c0, c1} = m.commits
    window = m.window

    Report.section(
      "wake storm",
      Report.kv(
        [
          {"casts out",
           "#{Report.ms_from_ns(m.sent_dt)} ms; last reply #{Report.ms_from_ns(window)} ms after the first cast → #{Report.decimal(Run.rate(n, window))} wakes/s"},
          {"wake → reply", Tally.latency_line(st.out)},
          {"rehydrations",
           "#{st.rehydrating} (expect #{n}); replies #{st.out.received}, dup #{st.out.duplicates}, missing #{n - st.out.seen}"},
          {"commits", "#{c1.task - c0.task} task, #{c1.iter - c0.iter} iter"}
        ] ++ if(st.stalled, do: [{"result", "STALLED"}], else: [])
      )
    )

    Run.emit_result(
      scenario: :sleep,
      factory: cfg.factory,
      throughput: Run.rate(n, window),
      unit: "wakes/s",
      p50_ms: Run.p50_ms(st.out),
      commits_s: Run.rate(c1.task - c0.task, max(window, 1)),
      integrity:
        Run.integrity(%{st | out: %{st.out | duplicates: st.out.duplicates}}, n - st.out.seen)
    )
  end

  # The second storm: nothing casts, every sleeper's own deadline fires. Late
  # is measured from the deadline the park set, not from the wake.
  defp timer_wake_storm(cfg, st, n) do
    IO.puts("Timer wake storm: every sleeper's #{cfg.wake_after_ms} ms deadline...")
    st = %{st | timer_wakes: [], rehydrating: 0}
    t0 = Run.now_ns()
    st = Run.collect(st, &(length(&1.timer_wakes) >= n), cfg.wake_after_ms + 120_000)
    t1 = Run.now_ns()

    late =
      Enum.reduce(st.timer_wakes, Tally.new(n), fn {id, at}, acc ->
        case {:ets.lookup(Run.waiters(), id), Map.get(st.park_at, id)} do
          {[{_, seq, _, _}], parked_at} when is_integer(parked_at) ->
            deadline = parked_at + cfg.wake_after_ms * 1_000_000
            Tally.record({seq, max(at - deadline, 0)}, acc)

          _ ->
            acc
        end
      end)

    Report.section(
      "timer wakes",
      Report.kv(
        [
          {"timer wakes",
           "#{length(st.timer_wakes)} of #{n} in #{Report.ms_from_ns(t1 - t0)} ms; rehydrations #{st.rehydrating}"},
          {"deadline → reply", Tally.latency_line(late)}
        ] ++ if(st.stalled, do: [{"result", "STALLED"}], else: [])
      )
    )
  end

  # --------------------------------------------------------------- spawns --

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
      hooks: [{:on_output, []}]
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
    Task.start_link(fn -> paced_loop(cfg, sink, Enum.to_list(seqs), Run.now_ns(), 0, main) end)
  end

  defp paced_loop(_cfg, _sink, [], _t0, _issued, _main), do: :ok

  defp paced_loop(cfg, sink, pending, t0, issued, main) do
    due = trunc((Run.now_ns() - t0) / 1.0e9 * cfg.rate) - issued
    {now, rest} = Enum.split(pending, max(due, 0))
    Enum.each(now, fn seq -> Task.start(fn -> spawn_one(cfg, sink, seq, main, nil) end) end)
    Process.sleep(5)
    paced_loop(cfg, sink, rest, t0, issued + length(now), main)
  end

  defp spawn_one(cfg, sink, seq, main, waiter) do
    id = "wf_#{seq}"
    seed_ts = Run.now_ns()
    :ets.insert(Run.waiters(), {id, seq, seed_ts, waiter})

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

    t0 = Run.now_ns()

    case OperativeManager.start_operative(Run.namespace(), builder) do
      {:ok, _info} ->
        send(main, {:spawned, seq, Run.now_ns() - t0, Run.now_ns()})
        {:ok, id}

      error ->
        send(main, {:spawn_error, seq, error})
        :error
    end
  end

  # Spawn the loop population (burst) and name it: `{state, ids}` with the
  # start-call latencies tallied. A failed spawn ends the run.
  defp spawn_population(cfg, sink, st) do
    n = cfg.n
    IO.puts("Spawning #{n} × #{cfg.factory}...")
    t0 = Run.now_ns()
    spawn_burst(cfg, sink, 0..(n - 1), self())
    st = Run.collect(st, &(&1.spawned + &1.spawn_errors >= n))
    if st.spawn_errors > 0, do: raise("#{st.spawn_errors} spawns failed")

    IO.puts(
      "  spawned in #{Report.ms_from_ns(Run.now_ns() - t0)} ms; start_operative #{Tally.latency_line(st.spawn)}"
    )

    {st, List.to_tuple(for seq <- 0..(n - 1), do: "wf_#{seq}")}
  end

  # ---------------------------------------------------------------- turns --

  defp send_turn(ids, seq) do
    id = elem(ids, rem(seq, tuple_size(ids)))
    OperativeManager.send_io(id, Run.namespace(), "#{seq},#{Run.now_ns()}", %{"channel" => "in"})
  end

  # Unpaced turns ride an in-flight cap (a few per loop) so the Overwatch
  # mailboxes stay bounded and the measurement is the engine, not the queue.
  defp start_turn_pacer(cfg, ids, first..last//_, received) do
    cap = max(4 * tuple_size(ids), 64)

    Task.start_link(fn ->
      turn_loop(cfg, ids, first, last, Run.now_ns(), first, received, cap)
    end)
  end

  defp turn_loop(_cfg, _ids, _first, last, _t0, next, _received, _cap) when next > last, do: :ok

  defp turn_loop(cfg, ids, first, last, t0, next, received, cap) do
    sent = next - first

    allowed =
      if cfg.rate > 0 do
        trunc((Run.now_ns() - t0) / 1.0e9 * cfg.rate) - sent
      else
        cap - (sent - :counters.get(received, 1))
      end

    upto = min(next + max(allowed, 0) - 1, last)
    if upto >= next, do: Enum.each(next..upto, &send_turn(ids, &1))
    if upto < last, do: Process.sleep(if(cfg.rate > 0, do: 5, else: 1))
    turn_loop(cfg, ids, first, last, t0, upto + 1, received, cap)
  end

  defp kill_rows(%{killed_at: nil}), do: []

  defp kill_rows(st) do
    recovered =
      case st.first_out_after_kill do
        nil -> "no reply after the kill"
        at -> "first reply #{Report.ms_from_ns(at - st.killed_at)} ms after the kill"
      end

    [
      {"runner kill",
       "os pid #{st.killed_pid} at +#{st.cfg.kill_runner_at} s; #{recovered}; #{st.rehydrating} rehydrations after it"}
    ]
  end
end
