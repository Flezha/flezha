defmodule Mix.Tasks.Bench.Workflow.Run do
  @moduledoc """
  One run's own state: the telemetry handler and the memory sampler that write
  into it, the collector that folds their messages, and the summary it answers
  with. Every scenario drives the same state machine.

  Owns the two constants a run is scoped by — its namespace and its waiter
  table — because it is the module nothing else here is named by, and a module
  attribute cannot cross a module boundary.
  """

  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.Paths
  alias Flezha.State.ProcInbox
  alias Flezha.State.ProcJournal
  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Report
  alias Mix.Tasks.Bench.Tally

  @namespace "bench_wf"
  @waiters :bench_wf_waiters

  @doc "The scratch namespace this benchmark owns."
  def namespace, do: @namespace

  @doc "The ETS table mapping an instance id to its seq, seed time and waiter."
  def waiters, do: @waiters

  def start_mem_sampler do
    ref = :atomics.new(1, [])
    Process.put(:mem_peak_ref, ref)
    Process.put(:mem_peak_pid, spawn_link(fn -> mem_sample_loop(ref) end))
  end

  defp mem_sample_loop(ref) do
    cur = :erlang.memory(:total)
    if cur > :atomics.get(ref, 1), do: :atomics.put(ref, 1, cur)
    Process.sleep(250)
    mem_sample_loop(ref)
  end

  def stop_mem_sampler do
    case Process.get(:mem_peak_pid) do
      pid when is_pid(pid) ->
        Process.unlink(pid)
        Process.exit(pid, :kill)

      _ ->
        :ok
    end
  end

  defp mem_peak_mb do
    case Process.get(:mem_peak_ref) do
      ref when not is_nil(ref) ->
        div(max(:atomics.get(ref, 1), :erlang.memory(:total)), 1_048_576)

      _ ->
        mib(:erlang.memory(:total))
    end
  end

  def print_header(cfg) do
    runtime = Fleet.runtime()

    Report.open("bench.workflow", [
      {"runtime", "#{runtime} (#{Report.dispatch_note(runtime)})"},
      {"toolchain", Report.toolchain(runtime)},
      {"runners", to_string(cfg.runners)},
      {"scenario", to_string(cfg.scenario)},
      {"factory", cfg.factory},
      {"instances", to_string(cfg.n)},
      {"turns/instance", if(cfg.scenario == :turns, do: to_string(cfg.turns), else: "-")},
      {"concurrency",
       if(cfg.concurrency > 0,
         do: to_string(cfg.concurrency),
         else: "burst (≤#{cfg.spawn_concurrency} starts in flight)"
       )},
      {"rate", if(cfg.rate == 0, do: "unpaced", else: "#{cfg.rate}/s")},
      {"park idle",
       "#{cfg.park_idle_ms} ms#{if cfg.wake_after_ms, do: ", wake after #{cfg.wake_after_ms} ms", else: ""}"},
      {"mix env", to_string(Mix.env())}
    ])
  end

  def attach_telemetry(pid) do
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

  def commits(counters),
    do: %{
      proc: :counters.get(counters, 1),
      iter: :counters.get(counters, 2),
      failed: :counters.get(counters, 3)
    }

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

  def new_state(cfg, expected, prime) do
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
  def collect(st, done?, timeout \\ nil) do
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
    st = %{
      st
      | spawn: Tally.record({seq, dur_ns}, st.spawn),
        spawned: st.spawned + 1,
        last_spawn_at: at
    }

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

          if st.killed_at && is_nil(st.first_out_after_kill),
            do: %{st | first_out_after_kill: now},
            else: st

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

  defp handle({:EXIT, pid, reason}, _st)
       when is_pid(pid) and reason not in [:normal, :killed, :shutdown],
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
  def diagnose_stall(st, ids) do
    diagnose_missing(st, id_of(ids))
    diagnose_channels()
  end

  # The instance id for a turn seq, round-robin over the loop tuple.
  def id_of(ids), do: &elem(ids, rem(&1, tuple_size(ids)))

  # Name what never arrived — the seqs without an output (and, for a chain,
  # without a completion) — and what the controller knows about each: the
  # journal's last entries, the inbox it still owes, the Overwatch's belief.
  def diagnose_missing(st, id_of \\ &"wf_#{&1}") do
    no_output = Tally.missing(st.out)
    no_done = Tally.missing(st.done)

    if no_output != [] or no_done != [] do
      IO.puts(
        "missing outputs (seq): #{inspect(no_output)}; missing completions (seq): #{inspect(no_done)}"
      )

      for seq <- Enum.uniq(no_output ++ no_done) |> Enum.take(5) do
        id = id_of.(seq)
        {:ok, entries} = ProcJournal.read(id)
        journal = entries |> Enum.map(&{&1.type, &1.iter}) |> Enum.take(-4)
        undelivered = id |> ProcInbox.undelivered() |> Enum.map(& &1.offset)

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
            "completion=#{inspect(Completion.read(id))} overwatch=#{inspect(overwatch)}"
        )
      end
    end
  end

  # The per-runner control processes' mailboxes: a wedged channel shows here.
  defp diagnose_channels do
    for runner_id <- FleetManager.active_runners(@namespace),
        {kind, name} <- [
          {:control_channel, Identificator.control_channel(runner_id)},
          {:control_unit, Identificator.control_unit(runner_id)}
        ] do
      info =
        case GenServer.whereis(name) do
          nil -> :down
          pid -> Process.info(pid, [:message_queue_len, :current_function, :reductions])
        end

      IO.puts("  #{runner_id} #{kind}: #{inspect(info)}")
    end
  end

  # A single machine-readable summary line the acceptance runner greps:
  # `BENCH_RESULT k=v ...`. Additive to the human summary above it.
  def emit_result(kv) do
    # Peak memory (capacity signal) and fsync p99 (is the disk the ceiling?)
    # ride every result — both read from in-VM state already computed, so no
    # sampler, no /proc, no observer effect.
    kv = kv ++ [peak_mb: mem_peak_mb(), fsync_p99_ms: Tally.fsync_p99_ms()]
    fields = Enum.map_join(kv, " ", fn {k, v} -> "#{k}=#{fmt_field(v)}" end)
    # `bench/acceptance/run.sh` parses this line; the JSON report below is the
    # newer contract, and both name the same numbers.
    IO.puts("BENCH_RESULT #{fields}")

    scenario = Keyword.get(kv, :scenario, :workflow)
    runtime = Fleet.runtime()

    Report.finish(
      "workflow-#{runtime}-#{scenario}",
      %{metrics: Map.new(kv, fn {k, v} -> {k, fmt_field(v)} end)}
    )
  end

  defp fmt_field(v) when is_float(v), do: Float.round(v, 2)
  defp fmt_field(v), do: v

  # `clean` | `loss` (missing or duplicated seqs) | `stall`.
  def integrity(%{stalled: true}, _missing), do: :stall
  def integrity(%{out: %{duplicates: 0}}, 0), do: :clean
  def integrity(_st, _missing), do: :loss

  def p50_ms(tally), do: Float.round(Tally.percentiles(tally, [0.50])[0.50] / 1_000_000, 2)

  # One verdict for a measured phase: stalled, clean, or an integrity failure
  # (a missing or duplicated seq).
  def verdict(%{stalled: true}, _missing), do: "RESULT: STALLED"
  def verdict(%{out: %{duplicates: 0}}, 0), do: "RESULT: clean"
  def verdict(_st, _missing), do: "RESULT: INTEGRITY FAILURE"

  # ---------------------------------------------------------------- utils --

  def writer_heap_kib do
    case Flezha.State.Supervisor.lookup(Paths.journal_id("wf_0")) do
      {:ok, pid} -> pid |> Process.info(:memory) |> elem(1) |> div(1024)
      _ -> "?"
    end
  end

  # Heap before and after a forced collection: a sleeper that never hibernates
  # carries its bind-time garbage until the next major GC.
  def overwatch_heap_kib do
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
  def process_sample do
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
  def largest_processes(sample) do
    sample
    |> Enum.sort_by(&elem(&1, 1), :desc)
    |> Enum.take(5)
    |> Enum.map_join(", ", fn {who, m} -> "#{who}=#{div(m, 1024)}KiB" end)
  end

  # The five most numerous process kinds — what a thousand completed or
  # sleeping instances leave behind on the controller.
  def process_breakdown(sample) do
    sample
    |> Enum.map(&elem(&1, 0))
    |> Enum.frequencies()
    |> Enum.sort_by(&elem(&1, 1), :desc)
    |> Enum.take(5)
    |> Enum.map_join(", ", fn {m, c} -> "#{m}=#{c}" end)
  end

  def memory_snapshot do
    m = :erlang.memory()
    %{total: m[:total], processes: m[:processes], ets: m[:ets], count: length(Process.list())}
  end

  def now_ns, do: System.monotonic_time(:nanosecond)
  defp now_ms, do: System.monotonic_time(:millisecond)
  def fmt_ms(ns), do: Float.round(ns / 1_000_000, 1)
  def mib(bytes), do: div(bytes, 1_048_576)
  def rate(count, window_ns), do: Float.round(count * 1.0e9 / max(window_ns, 1), 1)
end
