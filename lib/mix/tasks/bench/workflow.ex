defmodule Mix.Tasks.Bench.Workflow do
  use Mix.Task

  alias Mix.Tasks.Bench.Fleet
  alias Mix.Tasks.Bench.Report
  alias Mix.Tasks.Bench.Workflow.Run
  alias Mix.Tasks.Bench.Workflow.Scenarios

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
    no_wake: :boolean,
    runtime: :string
  ]

  def run(args) do
    {opts, _, invalid} =
      OptionParser.parse(args,
        switches: @switches,
        aliases: [n: :num, m: :turns, k: :procs, c: :concurrency]
      )

    if invalid != [], do: raise("unknown options: #{inspect(invalid)}")

    cfg = config(opts)
    runtime = Fleet.parse_runtime(opts[:runtime], :workflow)

    Report.apply_log_level()

    if cfg.scenario != :sleep_boot, do: reset_namespace()

    # The handler is attached before the controller boots so a sleeper restored
    # during boot (`sleep-boot`) is counted like one that parked at run time.
    Mix.Task.run("loadpaths")
    {:ok, _} = Application.ensure_all_started(:telemetry)
    :ets.new(Run.waiters(), [:named_table, :public, read_concurrency: true])
    counters = Run.attach_telemetry(self())
    Process.flag(:trap_exit, true)

    boot_t0 = Run.now_ns()
    Mix.Task.run("app.start")
    Report.apply_log_level()
    Fleet.preload_code()
    Process.put(:bench_runtime, runtime)

    Run.print_header(cfg)
    Run.start_mem_sampler()

    try do
      case cfg.scenario do
        :chain -> Scenarios.run_chain(cfg, counters)
        :turns -> Scenarios.run_turns(cfg, counters)
        :sleep -> Scenarios.run_sleep(cfg, counters)
        :sleep_boot -> Scenarios.run_sleep_boot(cfg, counters, boot_t0)
      end
    after
      Run.stop_mem_sampler()
    end
  end

  # Peak BEAM memory across the run, tracked by a tiny sampler process (a
  # 250 ms tick, ~4 reads/s — off the collector's hot path, so it captures a
  # mid-run spike a busy receive loop would miss and adds no measurable load).
  # The max lives in an :atomics cell; the run's summary reads it at the end.
  defp config(opts) do
    scenario = parse_scenario(opts[:scenario] || "chain")
    procs = opts[:procs] || 4
    tier = opts[:tier] || "durable"
    factory = fixture(scenario, tier, procs)

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

  defp parse_scenario("chain"), do: :chain
  defp parse_scenario("turns"), do: :turns
  defp parse_scenario("sleep"), do: :sleep
  defp parse_scenario("sleep-boot"), do: :sleep_boot
  defp parse_scenario(other), do: raise("unknown --scenario #{inspect(other)}")

  # The test-app fixture each scenario drives. Only the chain has variants, and
  # only the ones its factories exist for.
  defp fixture(:chain, "durable", procs) when procs in [1, 4, 16], do: "bench_chain_#{procs}"
  defp fixture(:chain, "optout", 4), do: "bench_chain_optout_4"

  defp fixture(:chain, _tier, _procs),
    do: raise("chain fixtures: --procs 1|4|16 --tier durable, or --procs 4 --tier optout")

  defp fixture(_scenario, _tier, _procs), do: "bench_loop_workflow"

  defp reset_namespace do
    for dir <- [
          Path.join(["app_data", "state", Run.namespace()]),
          Path.join(["app_data", "process_hub", "#{Run.namespace()}_hub"])
        ] do
      File.rm_rf!(dir)
    end
  end
end
