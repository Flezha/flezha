defmodule FlezhaTest.System.EffectivelyOncePostgresE2ETest do
  @moduledoc """
  Crash-matrix proof for `effectively-once-output-postgres-sink` against a
  live Rust runner and a real Postgres: a source drives a tier operative
  (`tier_store`, declared `effectively_once()`) whose keyed outputs land on
  the transactional Postgres sink. The runner is SIGKILLed mid-stream
  while inputs keep flowing, and the controller's state layer is crashed
  with a staled flush cursor (forcing a full outbox re-drive). The final
  `flezha_records` row set and `flezha_sink_progress` watermark must equal
  the no-crash run's final state — re-driven duplicates and stale epochs
  are absorbed by the `(epoch, seq)` monotonicity guard inside the sink's
  single flush transaction.

  Tagged `:system` + `:postgres_integration` (boots real subprocesses AND
  needs the docker Postgres); run with:

      POSTGRES_INTEGRATION=1 mix test --include system \\
        test/system/effectively_once_postgres_e2e_test.exs

  Pre-req: `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink`.
  """

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.PostgresSink
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Runner.FleetManager
  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Outbox

  @moduletag :system
  @moduletag :postgres_integration
  @moduletag timeout: 300_000

  @factory "tier_store"
  @procedure "tier_store_procedure"

  setup_all do
    if System.get_env("POSTGRES_INTEGRATION") != "1" do
      raise "Refusing to run without POSTGRES_INTEGRATION=1 and the postgres-sink container up"
    end

    :ok
  end

  setup do
    db = connect!()
    drop_tables!(db)
    # Operator-owned schema (the prod default): pre-created, auto_create off.
    create_tables!(db)
    %{db: db}
  end

  test "kills at each phase boundary converge the Postgres sink to the no-crash state", %{db: db} do
    namespace = unique("eff_once_pg")
    operative_id = unique("tier_store")
    id_path = unique_id_path()
    entity_id = {operative_id, @procedure, ""}
    entity_label = "#{operative_id}/#{@procedure}/"

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    # --- Phase 0: boot, bind, sink ------------------------------------------
    {port_a, exec_a} = boot_runner(namespace, id_path)

    # The sink rides the builder opts so every Overwatch restart (the
    # controller-crash phases) re-prepares it from the cspec; the stable
    # plugin_id makes each re-prepare reuse the live worker + pool.
    sink = %Flezha.Plugin{
      id: "tier_pg_sink",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.Postgres,
      opts:
        conn_opts()
        |> Map.new()
        |> Map.merge(%{
          auto_create: false,
          linger_ms: 10,
          plugin_id: "tier_pg_sink_#{operative_id}"
        }),
      subscriptions: [{:on_output, []}]
    }

    builder = %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{},
      opts: [plugins: [sink]]
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)
    assert info.runner_id == exec_a

    # The SDK-declared tier must have been resolved at bind.
    assert wait_until(
             fn -> Flezha.Operative.ClassRegistry.effectively_once?(operative_id) end,
             30_000
           ),
           "the tier declaration never reached the controller"

    # --- Phase 1: clean stream ------------------------------------------------
    send_inputs(operative_id, namespace, ["k1:a", "k2:a", "k1:b"])
    assert await_rows(db, %{"k1" => "k1=b", "k2" => "k2=a"}, 30_000)

    # --- Phase 2: runner SIGKILL mid-stream ---------------------------------
    sigkill(os_pid(port_a))

    # Inputs while no runner is alive: journaled, undeliverable — the
    # rebind's journal replay must recover them.
    send_inputs(operative_id, namespace, ["k2:b", "k3:a"])

    {_port_b, exec_b} = boot_runner(namespace, id_path)
    assert exec_b != exec_a

    assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end, 30_000)
    force_fresh_rebind(namespace, operative_id)

    assert await_rows(db, %{"k1" => "k1=b", "k2" => "k2=b", "k3" => "k3=a"}, 60_000),
           "journaled inputs were not replayed after the runner kill"

    # --- Phase 3: controller state-layer crash with a staled cursor ----------
    stale_cursor_and_crash_writers(operative_id, entity_id)
    force_fresh_rebind(namespace, operative_id)

    send_inputs(operative_id, namespace, ["k1:c"])

    expected_final = %{"k1" => "k1=c", "k2" => "k2=b", "k3" => "k3=a"}
    assert await_rows(db, expected_final, 60_000)

    # Let the full outbox re-drive land, then audit the final table state.
    Process.sleep(2_000)

    assert db_changelog(db) == expected_final,
           "the row set diverged from the no-crash final state"

    # Every row carries the durable identity under the tier's live epoch;
    # the re-drive (staled cursor ⇒ full outbox replay) regressed nothing.
    live_epoch = Outbox.read(entity_id).epoch
    rows = row_identities(db)
    assert rows != []

    for [key, entity, seq, epoch] <- rows do
      assert entity == entity_label, "row #{key} missing its entity stamp"
      assert seq =~ ~r/^\d+\.\d+$/, "row #{key} missing its outbox seq"
      assert epoch <= live_epoch, "row #{key} carries an epoch from the future"
    end

    # The progress watermark committed transactionally with the rows: same
    # entity, at/above every row's (epoch, seq), never past the live epoch.
    assert [[^entity_label, last_seq, progress_epoch]] = progress(db)
    assert progress_epoch <= live_epoch

    max_row = rows |> Enum.map(fn [_, _, seq, epoch] -> {epoch, seq_key(seq)} end) |> Enum.max()

    assert {progress_epoch, seq_key(last_seq)} >= max_row,
           "the watermark lags a committed row — data and progress diverged"
  end

  # --- helpers ----------------------------------------------------------------

  defp await_rows(db, expected, timeout) do
    wait_until(
      fn -> Map.take(db_changelog(db), Map.keys(expected)) == expected end,
      timeout
    )
  end

  # The changelog as the sink TABLE holds it (the harness's `changelog/1` builds
  # one from what the plugin forwarded).
  defp db_changelog(db) do
    Map.new(rows(db), fn [key, _seg, _seq, _epoch, payload, _at] -> {key, payload} end)
  end

  defp row_identities(db) do
    Enum.map(rows(db), fn [key, entity, seq, epoch, _payload, _at] ->
      [key, entity, seq, epoch]
    end)
  end

  defp seq_key(seq), do: seq |> String.split(".") |> Enum.map(&String.to_integer/1)
end
