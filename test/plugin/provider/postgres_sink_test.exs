defmodule FlezhaTest.Plugin.Provider.PostgresSinkTest do
  @moduledoc """
  Integration tests for the transactional Postgres sink against a real
  Postgres in `docker/dev/docker-compose.yml`. Tagged
  `:postgres_integration` so the default `mix test` skips them — run with:

      POSTGRES_INTEGRATION=1 mix test --include postgres_integration \\
        test/plugin/provider/postgres_sink_test.exs

  Pre-req: `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink`
  (reachable at localhost:5433, user/db/password `flezha`/`flezha_test`).
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.PostgresSink

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Postgres
  alias Flezha.Plugin.Provider.Postgres.{SinkSup, SinkWorker}

  @moduletag :postgres_integration

  setup_all do
    if System.get_env("POSTGRES_INTEGRATION") != "1" do
      raise "Refusing to run postgres_integration tests without POSTGRES_INTEGRATION=1"
    end

    :ok
  end

  setup do
    db = connect!()
    drop_tables!(db)
    %{db: db}
  end

  defp base_opts(extra \\ %{}) do
    conn_opts()
    |> Map.new()
    |> Map.merge(%{
      auto_create: true,
      linger_ms: 10,
      plugin_id: "pg-test-#{System.unique_integer([:positive])}"
    })
    |> Map.merge(extra)
  end

  defp start_provider(extra \\ %{}) do
    opts = base_opts(extra)
    {:ok, opts, pid} = Postgres.init_provider(opts)
    on_exit(fn -> SinkSup.stop_worker(opts.plugin_id) end)
    {opts, pid}
  end

  defp emit(opts, payload, metadata) do
    ctx = %Ctx.IO{operative_id: "op", namespace: "ns", message: payload, metadata: metadata}
    {:ok, _} = Postgres.on_output(ctx, opts)
  end

  defp stamps(segment, seq, epoch, key) do
    %{"flz.segment" => segment, "flz.seq" => seq, "flz.epoch" => epoch, "flz.key" => key}
  end

  test "batch of 3 records commits rows and progress atomically", %{db: db} do
    {opts, _pid} = start_provider()
    segment = "op/tier_op/"

    emit(opts, "v1", stamps(segment, "1.0", "1", "k1"))
    emit(opts, "v2", stamps(segment, "2.0", "1", "k2"))
    emit(opts, "v3", stamps(segment, "2.1", "1", "k1"))

    assert wait_until(fn -> length(rows(db)) == 2 end, 10_000)

    assert [
             ["k1", ^segment, "2.1", 1, "v3", %DateTime{}],
             ["k2", ^segment, "2.0", 1, "v2", %DateTime{}]
           ] = rows(db)

    # Progress committed in the same transaction as the rows.
    assert [[^segment, "2.1", 1]] = progress(db)
    assert Postgres.health(opts) == :healthy
  end

  test "re-driven batch is a byte-identical no-op", %{db: db} do
    {opts, pid} = start_provider()
    segment = "op/tier_op/"
    batch = [{"a", "1.0", "k1"}, {"b", "2.0", "k2"}, {"c", "3.0", "k1"}]

    deliver = fn ->
      for {payload, seq, key} <- batch, do: emit(opts, payload, stamps(segment, seq, "1", key))
    end

    deliver.()
    assert wait_until(fn -> progress(db) == [[segment, "3.0", 1]] end, 10_000)
    first = {rows(db), progress(db)}

    # Crash-between-flush-and-cursor-advance: the identical batch again.
    deliver.()
    assert wait_until(fn -> SinkWorker.stats(pid).buffer_len == 0 end, 10_000)

    assert {rows(db), progress(db)} == first,
           "a re-driven batch must leave rows and progress byte-identical"
  end

  test "stale epoch cannot regress a row or the watermark", %{db: db} do
    {opts, pid} = start_provider()
    segment = "op/tier_op/"

    emit(opts, "new", stamps(segment, "1.0", "2", "k1"))
    assert wait_until(fn -> progress(db) == [[segment, "1.0", 2]] end, 10_000)
    first = {rows(db), progress(db)}

    emit(opts, "stale", stamps(segment, "9.0", "1", "k1"))
    assert wait_until(fn -> SinkWorker.stats(pid).buffer_len == 0 end, 10_000)

    assert {rows(db), progress(db)} == first,
           "an epoch-1 record must not regress an epoch-2 row"
  end

  test "records without tier stamps become plain inserts and never touch progress", %{db: db} do
    {opts, _pid} = start_provider()

    emit(opts, "plain", %{})
    emit(opts, "plain", %{})

    assert wait_until(fn -> length(rows(db)) == 2 end, 10_000)
    assert progress(db) == []
  end

  test "auto_create off + missing tables fails health, then retries without loss", %{db: db} do
    {opts, pid} = start_provider(%{auto_create: false})

    assert wait_until(
             fn ->
               match?({:unhealthy, {:missing_tables, _}}, Postgres.health(opts))
             end,
             10_000
           ),
           "health must report the missing tables while auto_create is off"

    refute table_exists?(db, "flezha_records"), "auto_create off must never run DDL"

    # Records delivered while the tables are missing are retried, not dropped.
    segment = "op/tier_op/"
    emit(opts, "v1", stamps(segment, "1.0", "1", "k1"))
    emit(opts, "v2", stamps(segment, "2.0", "1", "k2"))

    Process.sleep(500)
    assert SinkWorker.stats(pid).buffer_len == 2
    assert SinkWorker.stats(pid).consecutive_failures >= 1

    # Operator creates the schema out of band → the next retry commits all.
    create_tables!(db)

    assert wait_until(fn -> length(rows(db)) == 2 end, 15_000),
           "buffered records must commit once the tables appear"

    assert [[^segment, "2.0", 1]] = progress(db)
    assert wait_until(fn -> Postgres.health(opts) == :healthy end, 10_000)
  end

  test "unreachable database delays with backoff and never loses records" do
    # Point at a closed port: every flush fails, records stay buffered and
    # health degrades to a commit stall with the sanitized failure reason.
    {opts, pid} =
      start_provider(%{port: 59_999, unhealthy_after_ms: 1, retry_max_backoff_ms: 200})

    segment = "op/tier_op/"
    emit(opts, "v1", stamps(segment, "1.0", "1", "k1"))

    assert wait_until(
             fn ->
               stats = SinkWorker.stats(pid)
               stats.buffer_len == 1 and stats.consecutive_failures >= 2
             end,
             10_000
           ),
           "the flush must keep retrying while the database is unreachable"

    assert {:unhealthy, {:commit_stalled, reason}} = Postgres.health(opts)
    assert is_binary(reason)
  end

  test "connection failures never log password material" do
    # Test env caps the primary logger level at :error; the worker logs
    # flush failures at :warning, so open the module up for the capture.
    :ok = :logger.set_module_level(SinkWorker, :all)
    on_exit(fn -> :logger.unset_module_level(SinkWorker) end)

    log =
      capture_log(fn ->
        {opts, pid} =
          start_provider(%{
            port: 59_999,
            password: "sup3r-s3cret-pw",
            unhealthy_after_ms: 1,
            retry_max_backoff_ms: 200
          })

        emit(opts, "v1", stamps("op/tier_op/", "1.0", "1", "k1"))
        wait_until(fn -> SinkWorker.stats(pid).consecutive_failures >= 1 end, 10_000)

        # The redacted opts surface (health, CLI listing) must not carry it either.
        assert opts.password == "**redacted**"
        {:unhealthy, {:commit_stalled, reason}} = Postgres.health(opts)
        refute reason =~ "sup3r-s3cret-pw"
      end)

    co = Map.new(conn_opts())
    assert log =~ "[postgres-sink]"
    assert log =~ "#{co.hostname}:59999/#{co.database}", "failure logs must carry host/database"
    refute log =~ "sup3r-s3cret-pw", "failure logs must never carry password material"
  end
end
