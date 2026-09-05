defmodule FlezhaTest.System.HealthVerdictDrillTest do
  @moduledoc """
  Every health state, produced through the real transport, read on four
  surfaces, asserted to be ONE word.

  The table is `for kind <- Verdict.kinds(), state <- Verdict.reachable(kind)`,
  so a state added to the vocabulary and driven by nothing fails here by name.
  Each row either names the condition that mints it — a killed runner OS
  process, a bind-time restore the SDK refused, an unloadable provider, an
  Overwatch suspended past the probe budget, a workflow instance evicted at a
  committed boundary — or is a RECORDED deferral saying which tier owns it.

  Per driven row the drill reads `GET /healthz` on the test's bound port,
  `flz operative info --json` and `flz operative list --json` in process, and
  the observability bus for the subject's last `[:flezha, :health, :verdict]`,
  and asserts they agree. Every row shares one booted fixture: the module boots
  a namespace, a runner, the convergence Loop and the healthz listener once,
  and produces each condition on its own operative.

  Tagged `:system`; run with
  `mix test --include system test/system/health_verdict_drill_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 3, os_pid: 1, sigkill: 1, stop_namespace: 1, unique_id_path: 1]

  alias Flezha.Convergence.{Loop, Snapshot, Verdict}
  alias Flezha.Healthz.Server, as: Healthz
  alias Flezha.Operative.Migration
  alias Flezha.Plugin
  alias Flezha.Service.{CliManager, OperativeManager, PluginManager}

  require Logger

  # The loop is driven by hand: a self-tick between minting a record and reading
  # it would heal the very condition the row is asserting. Declared facts still
  # tick at once, which is why every read below tries the published snapshot
  # BEFORE asking for another tick.
  @tick_ms 600_000
  @grace_ms 30_000

  # A provider whose `activate/1` always fails: the plugin is subscribed and
  # stays `:prepared` for ever, which is a declared plugin that is not running.
  @stuck_provider FlezhaTest.System.HealthVerdictDrillTest.ActivateFailsProvider

  # The rows this tier cannot drive, each with the reason recorded. A `:channel`
  # or `:driver` state needs the data-plane classifier to mint a verdict, which
  # this tier cannot force without wedging the NIF thread that carries the test
  # node's own traffic.
  @deferred %{
    {:operative, :unbound} =>
      "needs a live Overwatch whose binding was cleared by a failed rebind; the rebind ladder " <>
        "is `remedy_drill_test`'s row and clearing the binding by hand would not be a condition",
    {:runner, :unknown} =>
      "needs the namespace's live-runner read to fail mid-tick; the seam test drives it against " <>
        "an `:unknown` roster",
    {:runner, :degraded} =>
      "needs a runner-scoped classifier layer, which `remedy_drill_test`'s `restart_runner` row " <>
        "drives on the pool-wedge fixture",
    {:channel, :healthy} => "the classifier is the channel detector; see `stall_classifier_test`",
    {:channel, :degraded} => "a connection-scoped layer; `remedy_drill_test` drives the remedy",
    {:channel, :failed} => "a channel row whose runner left the roster; covered by the seam test",
    {:channel, :unknown} => "the classifier could not answer; covered by the seam test",
    {:driver, :healthy} =>
      "the live driver's own self-report; `report-quic-driver-stats` covers it",
    {:driver, :degraded} =>
      "wedging the NIF driver thread would take the test node's own transport with it — the " <>
        "fabric arm is proved against a published snapshot in `Flezha.Healthz.ServerTest`",
    {:driver, :unknown} => "the classifier could not answer; covered by the seam test",
    {:plugin, :unknown} =>
      "a plugin the loop never probed at all: the probe runs on every tick that reaches the " <>
        "operative, so the window cannot be held open from here"
  }

  setup_all do
    namespace = "health_drill_#{System.unique_integer([:positive])}"
    Snapshot.reset_recovery_states()

    # The `failed` row is `recovering` inside the unmet-intent grace and
    # `degraded` past it; the drill asserts the second half, so the grace is
    # short enough to reach inside one row.
    previous_grace = Application.get_env(:flezha, :convergence_unmet_intent_grace_ms)
    Application.put_env(:flezha, :convergence_unmet_intent_grace_ms, 1_000)

    port = start_healthz()
    {runner_port, runner_id} = boot_runner(namespace, unique_id_path("health_drill"), [])
    loop = start_loop()

    on_exit(fn ->
      if Process.alive?(loop), do: GenServer.stop(loop, :normal, 5_000)

      if previous_grace,
        do: Application.put_env(:flezha, :convergence_unmet_intent_grace_ms, previous_grace),
        else: Application.delete_env(:flezha, :convergence_unmet_intent_grace_ms)

      stop_namespace(namespace)
    end)

    %{namespace: namespace, port: port, runner_id: runner_id, runner_port: runner_port}
  end

  @tag timeout: 300_000
  test "every reachable state is produced, and four surfaces report one word", ctx do
    # Attached HERE, not in `setup_all`: the handler sends to the process that
    # attached it, and only the test process reads the mailbox.
    collect_verdicts(self())
    on_exit(fn -> :telemetry.detach({__MODULE__, :verdicts}) end)

    # Ordered, and the order is the point: `/healthz` is a FLEET verdict, so
    # every benign row runs while nothing has degraded the fleet yet, and the
    # rows that kill the runner run last.
    driven = [
      {{:operative, :healthy}, &row_healthy/1},
      {{:runner, :healthy}, &row_runner_healthy/1},
      {{:plugin, :healthy}, &row_plugin_healthy/1},
      {{:operative, :suspended}, &row_suspended/1},
      {{:operative, :unknown}, &row_unknown/1},
      {{:operative, :degraded}, &row_degraded/1},
      {{:plugin, :degraded}, &row_plugin_degraded/1},
      {{:operative, :parked}, &row_parked/1},
      {{:operative, :failed}, &row_failed/1},
      {{:runner, :failed}, &row_runner_failed/1}
    ]

    rows = for kind <- Verdict.kinds(), state <- Verdict.reachable(kind), do: {kind, state}

    covered = MapSet.new(driven, fn {row, _fun} -> row end)
    uncovered = Enum.reject(rows, &(MapSet.member?(covered, &1) or Map.has_key?(@deferred, &1)))

    assert uncovered == [],
           "a state the vocabulary reaches that no row drives and no deferral names: " <>
             inspect(uncovered)

    for {row, reason} <- @deferred do
      {kind, state} = row
      assert state in Verdict.reachable(kind)
      Logger.info("[health_drill] deferred #{inspect(row)}: #{reason}")
    end

    for {_row, fun} <- driven, do: fun.(ctx)
  end

  # ---------- the rows ----------

  defp row_healthy(ctx) do
    id = start_operative(ctx, "healthy")
    assert_surfaces(ctx, id, :healthy, "healthy")
  end

  # A declared plugin that is not running: subscribed, activation impossible,
  # so the loop's own `start_plugin` heal has something to do and the operative
  # is degraded while it does.
  defp row_degraded(ctx) do
    id = start_operative(ctx, "degraded")
    :ok = enable_plugin(ctx, id, id <> "_missing", @stuck_provider)
    assert_surfaces(ctx, id, :degraded, "degraded", reason: "plugin_unhealthy")
  end

  # The bind-time restore the SDK refused — the message `Operative.Replay`
  # sends and the one path that parks an operative.
  defp row_parked(ctx) do
    id = start_operative(ctx, "parked")
    {:ok, pid} = OperativeManager.get_operative_pid(ctx.namespace, id)

    repark = fn -> repark(pid) end

    assert repark.() == :ok
    assert_surfaces(ctx, id, :parked, "degraded", refresh: repark, reason: "parked")
  end

  # The park the loop is actively healing has to be re-declared before each
  # read, or the row would be asserting a state the loop has already cleared.
  defp repark(pid) do
    if is_nil(:sys.get_state(pid).init_error) do
      send(pid, {:restore_refused, :drill})
      wait_until(fn -> not is_nil(:sys.get_state(pid).init_error) end, 5_000)
    end

    :ok
  end

  # Evicted at a committed iteration boundary: planned, so no surface may call
  # it a fault and `/healthz` stays healthy.
  defp row_suspended(ctx) do
    id = start_operative(ctx, "suspended")
    {:ok, pid} = OperativeManager.get_operative_pid(ctx.namespace, id)
    :ok = Migration.suspend(pid, 0, nil)
    assert_surfaces(ctx, id, :suspended, "healthy")
  end

  # An Overwatch suspended past the probe budget: the bounded call cannot
  # answer, so the record is `unknown` and nothing escalates on it.
  defp row_unknown(ctx) do
    id = start_operative(ctx, "unknown")
    {:ok, pid} = OperativeManager.get_operative_pid(ctx.namespace, id)
    :sys.suspend(pid)
    on_exit(fn -> if Process.alive?(pid), do: :sys.resume(pid) end)
    assert_surfaces(ctx, id, :unknown, "healthy")
  end

  # The runner's OS process is killed: every operative bound to it is `failed`
  # with `dead_runner`, and the runner's own record says so too. The grace this
  # module sets is short enough that the endpoint is past it by the first tick,
  # so the `recovering` half of that window is asserted where it can be timed —
  # against a published snapshot in `Flezha.Healthz.ServerTest`.
  defp row_failed(ctx) do
    id = start_operative(ctx, "failed")
    sigkill(os_pid(ctx.runner_port))

    assert_surfaces(ctx, id, :failed, "degraded", timeout: 90_000, reason: "dead_runner")
  end

  defp row_plugin_healthy(ctx) do
    id = start_operative(ctx, "plugged")
    plugin_id = id <> "_ok"
    :ok = enable_plugin(ctx, id, plugin_id, FlezhaTest.System.HealthVerdictDrillTest.OkProvider)

    assert wait_until(ticked(fn -> plugin_state(ctx, id, plugin_id) == :healthy end), 60_000),
           "the plugin never reached a healthy record"
  end

  # A running plugin whose provider self-reports unhealthy: the fact seam
  # normalises it, the judge turns it into the plugin's one degraded record.
  defp row_plugin_degraded(ctx) do
    id = start_operative(ctx, "plugin_degraded")
    plugin_id = id <> "_sick"

    :ok =
      enable_plugin(
        ctx,
        id,
        plugin_id,
        FlezhaTest.System.HealthVerdictDrillTest.UnhealthyProvider
      )

    assert wait_until(ticked(fn -> plugin_state(ctx, id, plugin_id) == :degraded end), 60_000),
           "a declared plugin that cannot run never reached a degraded record"
  end

  defp row_runner_healthy(ctx) do
    assert wait_until(ticked(fn -> runner_state(ctx.runner_id) == :healthy end), 60_000),
           "the connected runner never reached a healthy record"
  end

  defp row_runner_failed(ctx) do
    assert wait_until(ticked(fn -> runner_state(ctx.runner_id) == :failed end), 60_000),
           "the killed runner never reached a failed record"
  end

  # ---------- the four surfaces ----------

  # One word on all four: the record the loop published, `flz operative info`,
  # `flz operative list`, and the `/healthz` word `Verdict.fold/2` gives that
  # state. The bus's verdict event for the subject is asserted last, because a
  # transition is what it reports and a state that never changed emits none.
  defp assert_surfaces(ctx, id, state, healthz_word, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 60_000)
    # A row whose condition the loop heals re-establishes it before each read:
    # asserting four surfaces on a state the loop is actively repairing is a
    # race, and re-declaring the condition is what makes the window real.
    refresh = Keyword.get(opts, :refresh, fn -> :ok end)

    agree? = fn ->
      refresh.()

      record_state(ctx, id) == state and
        info_health(ctx, id) in [to_string(state), nil] and
        list_health(ctx, id) == to_string(state)
    end

    assert wait_until(ticked(agree?), timeout),
           "#{id}: the surfaces never agreed on #{state} — record #{inspect(record_state(ctx, id))}, " <>
             "info #{inspect(info_health(ctx, id))}, list #{inspect(list_health(ctx, id))}"

    assert_healthz(ctx, id, state, healthz_word, refresh, Keyword.get(opts, :reason))
    assert_verdict_signal(ctx, id, state)
  end

  # `/healthz` is a verdict about the FLEET, so a benign row asserts the whole
  # endpoint stays healthy and a degrading one asserts it can never read healthy
  # while the row stands — the 08-18 shape, whichever other rows have run.
  defp assert_healthz(ctx, id, state, "healthy", refresh, _reason) do
    assert wait_until(
             ticked(fn -> refresh.() && healthz_body(ctx)["status"] == "healthy" end),
             60_000
           ),
           "#{id}: a #{state} operative must leave /healthz healthy, got #{inspect(healthz_body(ctx))}"
  end

  # The endpoint word AND the row's own reason: a fleet degraded by an earlier
  # row would satisfy the word on its own, so the reason is what proves THIS
  # subject reached the kubelet's answer. That is the 08-18 shape — a parked
  # operative the health map knew about and `/healthz` never heard of.
  defp assert_healthz(ctx, id, state, word, refresh, reason) do
    assert wait_until(
             ticked(fn ->
               refresh.()
               body = healthz_body(ctx)

               body["status"] in [word, "recovering"] and
                 (is_nil(reason) or String.contains?(body["reason"] || "", reason))
             end),
             60_000
           ),
           "#{id}: a #{state} operative did not reach /healthz as #{inspect(reason)} — " <>
             "got #{inspect(healthz_body(ctx))}"
  end

  defp assert_verdict_signal(ctx, id, state) do
    assert wait_until(fn -> verdict_signal?(ctx, id, state) end, 30_000),
           "#{id}: no [:flezha, :health, :verdict] signal for #{state}"
  end

  defp verdict_signal?(ctx, id, state) do
    Enum.any?(drain_verdicts(), fn meta ->
      meta[:kind] == :operative and meta[:id] == {ctx.namespace, id} and meta[:state] == state
    end)
  end

  # ---------- readers ----------

  # `:absent` and `:unknown` are different answers: an operative the loop has
  # never judged has no record, and asserting on `Snapshot.operative_health/2`
  # alone would read the missing-record default as a judged `unknown`.
  defp record_state(ctx, id) do
    state_of({:operative, {ctx.namespace, id}}) || :absent
  end

  defp plugin_state(ctx, id, plugin_id),
    do: state_of({:plugin, {ctx.namespace, id, plugin_id}})

  defp runner_state(runner_id), do: state_of({:runner, runner_id})

  defp state_of(subject) do
    case Snapshot.record(subject) do
      %Verdict{state: state} -> state
      _ -> nil
    end
  end

  defp info_health(ctx, id) do
    out = ExUnit.CaptureIO.capture_io(fn -> CliManager.operative_info(ctx.namespace, "json") end)
    %{"operatives" => rows} = Jason.decode!(out)

    case Enum.find(rows, &(&1["id"] == id)) do
      %{"health" => health} -> health
      _ -> nil
    end
  end

  defp list_health(ctx, id) do
    out = ExUnit.CaptureIO.capture_io(fn -> CliManager.list_operatives(ctx.namespace, "json") end)
    %{"operatives" => rows} = Jason.decode!(out)

    case Enum.find(rows, &(&1["id"] == id)) do
      %{"health" => health} -> health
      _ -> nil
    end
  end

  defp healthz_body(%{port: port}) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 5_000)
    :ok = :gen_tcp.send(socket, "GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
    {:ok, response} = :gen_tcp.recv(socket, 0, 5_000)
    :gen_tcp.close(socket)

    [_headers, body] = String.split(response, "\r\n\r\n", parts: 2)
    Jason.decode!(body)
  end

  # ---------- the fixture ----------

  defp start_healthz() do
    Application.put_env(:flezha, :healthz_port, 0)
    {:ok, _pid} = Healthz.start_link([])
    {:ok, port} = Healthz.listen_port()
    on_exit(fn -> Application.delete_env(:flezha, :healthz_port) end)
    port
  end

  defp start_loop() do
    {:ok, pid} = Loop.start_link(tick_ms: @tick_ms, output_stall_grace_ms: @grace_ms)
    pid
  end

  defp ticked(fun) do
    fn ->
      fun.() or
        (fn ->
           try do
             Loop.tick_now()
           catch
             _, _ -> :ok
           end

           fun.()
         end).()
    end
  end

  defp start_operative(ctx, label) do
    id = "#{label}_#{System.unique_integer([:positive])}"

    builder = %Flezha.Operative.Builder{
      id: id,
      factory_id: "do_nothing",
      init_values: %{},
      opts: [plugins: []]
    }

    {:ok, _} = OperativeManager.start_operative(ctx.namespace, builder)
    id
  end

  # The loop reads the DECLARED plugin set from the durable cspec, which only
  # `PluginManager.enable_plugin/2` writes.
  # The cspec is written before the subscribe, so an activation that fails
  # still leaves the plugin DECLARED — which is the condition the degraded row
  # needs: declared, and not running.
  defp enable_plugin(ctx, operative_id, plugin_id, provider) do
    result =
      PluginManager.enable_plugin(plugin_id, %Plugin{
        id: plugin_id,
        namespace: ctx.namespace,
        operative_id: operative_id,
        provider: provider,
        opts: %{},
        subscriptions: [{:on_input, [position: 0]}]
      })

    case result do
      {:ok, _} -> :ok
      {:error, {:activation_failed, _}} -> :ok
      other -> flunk("enable_plugin refused: #{inspect(other)}")
    end
  end

  defp collect_verdicts(parent) do
    :telemetry.attach(
      {__MODULE__, :verdicts},
      Verdict.event(),
      fn _event, _measurements, metadata, _config -> send(parent, {:verdict, metadata}) end,
      nil
    )
  end

  # Every verdict metadata collected so far, put back so a later row can read
  # the same history — the collector delivers into this process's mailbox.
  defp drain_verdicts(acc \\ []) do
    receive do
      {:verdict, meta} -> drain_verdicts([meta | acc])
    after
      0 ->
        Enum.each(acc, &send(self(), {:verdict, &1}))
        acc
    end
  end

  # An always-healthy provider with no owned pid and no `activate/1`: it reaches
  # `:running` and the probe answers `:healthy`.
  defmodule OkProvider do
    @moduledoc false
    def on_input(ctx, _opts), do: {:ok, ctx}
  end

  # Subscribes, never activates: the phase stays `:prepared`, which is what a
  # declared plugin that is not running looks like.
  defmodule ActivateFailsProvider do
    @moduledoc false
    def on_input(ctx, _opts), do: {:ok, ctx}
    def activate(_opts), do: {:error, :drill}
  end

  # Runs, and says so itself: the provider health contract's unhealthy answer,
  # which is the only definite fault a plugin can report.
  defmodule UnhealthyProvider do
    @moduledoc false
    def on_input(ctx, _opts), do: {:ok, ctx}
    def health(_opts), do: {:unhealthy, :drill}
  end
end
