defmodule Flezha.Plugin.Provider.PostgresPollIntegrationTest do
  @moduledoc """
  The Postgres poll source against the `postgres-sink` service of
  `docker/dev/docker-compose.yml`. Tagged `:postgres_integration`, so the
  default `mix test` skips it:

      POSTGRES_INTEGRATION=1 mix test --include postgres_integration \\
        test/flezha/plugin/provider/postgres_poll_integration_test.exs

  The test process stands in for the host operator: every injection is an
  `{:ingest, row, metadata}` call it answers, and every cursor commit a
  `{:plugin_opt_persist, …}` cast it receives. The reader connects as a role of
  its own with a literal password, so each case also proves the password the
  prepared options redact is the one the connection authenticates with. The
  first read is the reader's own; later ones are driven by sending `:poll`, with
  `interval_ms` parked at a minute.
  """

  # async: false: the primary Logger.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Plugin.Provider.PostgresPoll
  alias Flezha.Plugin.Provider.PostgresPoll.Reader
  alias FlezhaTest.Helper.PostgresSink
  alias FlezhaTest.Helper.ProviderPrepare
  alias FlezhaTest.Helper.Telemetry

  @moduletag :postgres_integration

  @role "flezha_poll_reader"
  @password "poll-#{System.unique_integer([:positive])}-secret"

  setup_all do
    if System.get_env("POSTGRES_INTEGRATION") != "1" do
      raise "Refusing to run postgres_integration tests without POSTGRES_INTEGRATION=1"
    end

    db = PostgresSink.connect!()

    Postgrex.query!(
      db,
      "DO $$ BEGIN CREATE ROLE #{@role} LOGIN; " <>
        "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
      []
    )

    Postgrex.query!(db, "ALTER ROLE #{@role} PASSWORD '#{@password}'", [])
    GenServer.stop(db)
    :ok
  end

  setup do
    # The suite's primary level drops a warning; open the reader so its lines
    # reach the capture.
    :ok = :logger.set_module_level(Reader, :all)
    on_exit(fn -> :logger.unset_module_level(Reader) end)

    db = PostgresSink.connect!()
    table = "poll_#{System.unique_integer([:positive])}"
    on_exit(fn -> drop(table) end)
    %{db: db, table: table}
  end

  describe "reading in order" do
    test "rows in id order, then the rows inserted later, each once", %{db: db, table: t} do
      create!(db, t, "id bigint PRIMARY KEY, item text")
      insert!(db, t, "(1, 'a'), (2, 'b'), (3, 'c')")

      Telemetry.capture([
        [:flezha, :plugin, :postgres_poll, :shipped],
        [:flezha, :plugin, :postgres_poll, :caught_up]
      ])

      {opts, reader} = start(%{table: t})

      assert [%{"id" => 1, "item" => "a"}, %{"id" => 2}, %{"id" => 3}] = take(3, opts, t)
      assert_receive {:telemetry, [_, _, _, :shipped], %{count: 3}, %{table: ^t}}
      assert_receive {:telemetry, [_, _, _, :caught_up], %{count: 1}, %{table: ^t}}

      insert!(db, t, "(4, 'd'), (5, 'e')")
      send(reader, :poll)
      assert [%{"id" => 4}, %{"id" => 5}] = take(2, opts, t)

      poll_once(reader)
      ProviderPrepare.refute_ingest()
      refute_received {:telemetry, [_, _, _, :caught_up], _, _}
    end

    test "each type becomes its JSON value", %{db: db, table: t} do
      create!(
        db,
        t,
        "id int PRIMARY KEY, n numeric(12,4), at timestamptz, u uuid, j jsonb, b bytea, t text"
      )

      insert!(
        db,
        t,
        "(1, 12.34, '2026-10-02 12:30:00.5+02', '6f1c3a52-6f0a-4f43-9a41-0d8f8c2e7b11', " <>
          ~s|'{"a": [1, 2]}', '\\x01ff', NULL)|
      )

      {opts, _reader} = start(%{table: t})

      assert [
               %{
                 "id" => 1,
                 "n" => "12.3400",
                 "at" => "2026-10-02T10:30:00.500000Z",
                 "u" => "6f1c3a52-6f0a-4f43-9a41-0d8f8c2e7b11",
                 "j" => %{"a" => [1, 2]},
                 "b" => "Af8=",
                 "t" => nil
               }
             ] = take(1, opts, t)
    end

    test "every read runs in a read-only transaction", %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY")
      insert!(db, t, "(1)")
      view = "#{t}_ro"

      create_view!(
        db,
        view,
        "SELECT id, current_setting('transaction_read_only') AS ro FROM #{t}"
      )

      {opts, _reader} = start(%{table: view})
      assert [%{"id" => 1, "ro" => "on"}] = take(1, opts, view)
    end
  end

  describe "the timestamp mode" do
    test "an updated row comes again with its new values", %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY, item text, updated_at timestamptz")
      insert!(db, t, "(7, 'old', now() - interval '1 minute')")

      {opts, reader} = start(%{table: t, timestamp_column: "updated_at"})
      {[%{"item" => "old"}], [first_id]} = take_with_ids(1, opts)

      Postgrex.query!(db, "UPDATE #{t} SET item = 'new', updated_at = now() WHERE id = 7", [])
      send(reader, :poll)

      {[%{"id" => 7, "item" => "new"}], [second_id]} = take_with_ids(1, opts)
      assert first_id =~ ~r|^#{opts.plugin_id}/public\.#{t}/.+\+00/7$|
      assert second_id != first_id
    end

    test "a row younger than the delay is left for a later read", %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY, updated_at timestamptz")
      insert!(db, t, "(1, now() - interval '2 hours'), (2, now())")

      {opts, reader} =
        start(%{table: t, timestamp_column: "updated_at", timestamp_delay_ms: 3_600_000})

      assert [%{"id" => 1}] = take(1, opts, t)
      poll_once(reader)
      ProviderPrepare.refute_ingest()

      Postgrex.query!(
        db,
        "UPDATE #{t} SET updated_at = now() - interval '90 minutes' WHERE id = 2",
        []
      )

      send(reader, :poll)
      assert [%{"id" => 2}] = take(1, opts, t)
    end
  end

  describe "the cursor" do
    test "start_at end commits the rows present as read, and reads only what comes after",
         %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY")
      insert!(db, t, "(1), (2), (3)")

      {opts, reader} = start(%{table: t, start_at: "end"})
      assert committed() == ["3"]

      insert!(db, t, "(4)")
      send(reader, :poll)
      assert [%{"id" => 4}] = take(1, opts, t)
    end

    test "a refused row is read again", %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY")
      insert!(db, t, "(1), (2)")

      log =
        capture_log([level: :warning], fn ->
          {_opts, reader} = start(%{table: t, batch_size: 1})
          assert {~s|{"id":1}|, _} = ProviderPrepare.take_ingest({:error, :parked})
          poll_once_after_refusal(reader)
        end)

      assert log =~ "read failed table=#{t}"
      assert log =~ "ingest refused: :parked"
    end

    test "with durable input the cursor is committed after every confirmed batch",
         %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY")
      insert!(db, t, "(1), (2), (3)")

      {opts, _reader} = start(%{table: t, batch_size: 1, durable_input: true})

      for id <- ["1", "2", "3"] do
        assert [_row] = take(1, opts, t)
        assert committed() == [id]
      end
    end

    test "without durable input, full batches are not committed until the reader catches up",
         %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY")
      insert!(db, t, "(1), (2), (3)")

      {opts, reader} = start(%{table: t, batch_size: 1, commit_interval_ms: 60_000})

      assert [_, _, _] = for(_ <- 1..3, do: hd(take(1, opts, t)))
      assert committed() == ["3"]
      refute_received {:"$gen_cast", {:plugin_opt_persist, _, :cursor, _}}
      assert :sys.get_state(reader).committed.at == ["3"]
    end

    test "a reader brought up with the committed cursor continues after it", %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY")
      insert!(db, t, "(1), (2)")

      {opts, reader} = start(%{table: t})
      assert [_, _] = take(2, opts, t)
      cursor = :sys.get_state(reader).committed
      assert cursor.at == ["2"]
      :ok = ProviderPrepare.stop(PostgresPoll, reader)

      insert!(db, t, "(3)")
      {opts, _reader} = start(%{table: t, plugin_id: opts.plugin_id, cursor: cursor})
      assert [%{"id" => 3}] = take(1, opts, t)
    end
  end

  describe "health" do
    test "a missing table and a missing column are named, and the password is never shown",
         %{db: db, table: t} do
      log =
        capture_log([level: :warning], fn ->
          {opts, reader} = start(%{table: t})
          assert await_health(opts, reader, {:unhealthy, {:table_missing, t}})

          create!(db, t, "id int PRIMARY KEY")
          poll_once(reader)
          assert health(opts, reader) == :healthy

          for printed <- [inspect(:sys.get_state(reader)), inspect(opts)],
              do: refute(printed =~ @password)

          assert opts.password == "**redacted**"

          :ok = ProviderPrepare.stop(PostgresPoll, reader)

          {opts, reader} = start(%{table: t, columns: "id,nope"})
          assert await_health(opts, reader, {:unhealthy, {:column_missing, "nope"}})
        end)

      assert log =~ ~s|table=#{t} localhost:5433/flezha_test {:table_missing, "#{t}"}|
      assert log =~ ~s|{:column_missing, "nope"}|
      refute log =~ @password
    end

    test "three failed reads report unhealthy, and the next successful one healthy",
         %{db: db, table: t} do
      create!(db, t, "id int PRIMARY KEY")
      insert!(db, t, "(1)")
      view = "#{t}_slow"
      create_view!(db, view, "SELECT id, pg_sleep(0.2)::text AS s FROM #{t}")

      log =
        capture_log([level: :warning], fn ->
          {opts, reader} = start(%{table: view, query_timeout_ms: 20, failure_threshold: 3})
          assert wait_until(fn -> :sys.get_state(reader).consecutive_failures == 1 end, 5_000)
          poll_once(reader)
          assert health(opts, reader) == :healthy
          poll_once(reader)

          assert health(opts, reader) ==
                   {:unhealthy,
                    {:read_failed, "query_canceled: canceling statement due to statement timeout"}}

          create_view!(db, view, "SELECT id, ''::text AS s FROM #{t}")
          send(reader, :poll)
          assert [%{"id" => 1}] = take(1, opts, view)
          assert health(opts, reader) == :healthy
        end)

      assert log =~ "consecutive=3 reason=query_canceled"
    end
  end

  # ---------- helpers ----------

  defp start(extra) do
    opts =
      Map.merge(
        %{
          hostname: "localhost",
          port: Keyword.fetch!(PostgresSink.conn_opts(), :port),
          database: Keyword.fetch!(PostgresSink.conn_opts(), :database),
          username: @role,
          password: @password,
          channel: "rows",
          id_column: "id",
          interval_ms: 60_000,
          plugin_id: "poll-#{System.unique_integer([:positive])}"
        },
        extra
      )

    db = PostgresSink.connect!()

    for name <- [opts.table], do: grant(db, name)
    GenServer.stop(db)

    {:ok, prepared, reader} = ProviderPrepare.prepare_and_activate(PostgresPoll, opts)
    ProviderPrepare.await_stop_on_exit(reader)
    {prepared, reader}
  end

  # The reader's role reads what it is pointed at; a table that does not exist
  # yet is granted when it is created.
  defp grant(db, name) do
    if PostgresSink.table_exists?(db, name),
      do: Postgrex.query!(db, "GRANT SELECT ON #{name} TO #{@role}", [])
  end

  defp create!(db, table, columns) do
    Postgrex.query!(db, "CREATE TABLE #{table} (#{columns})", [])
    grant(db, table)
  end

  defp create_view!(db, view, select) do
    Postgrex.query!(db, "CREATE OR REPLACE VIEW #{view} AS #{select}", [])
    grant(db, view)
  end

  defp insert!(db, table, rows),
    do: Postgrex.query!(db, "INSERT INTO #{table} VALUES #{rows}", [])

  defp drop(table) do
    db = PostgresSink.connect!()

    for view <- ["#{table}_ro", "#{table}_slow"],
        do: Postgrex.query!(db, "DROP VIEW IF EXISTS #{view}", [])

    Postgrex.query!(db, "DROP TABLE IF EXISTS #{table}", [])
    GenServer.stop(db)
  end

  # The next `n` rows injected, each taken and checked for its channel and its
  # id-only message id.
  defp take(n, opts, relation) do
    {rows, ids} = take_with_ids(n, opts)

    for {row, id} <- Enum.zip(rows, ids),
        not Map.has_key?(opts, :timestamp_column) or is_nil(opts.timestamp_column),
        do: assert(id == "#{opts.plugin_id}/public.#{relation}/#{row["id"]}")

    rows
  end

  defp take_with_ids(n, opts) do
    1..n
    |> Enum.map(fn _ ->
      {body, metadata} = ProviderPrepare.take_ingest()
      assert metadata["channel"] == opts.entry_channel
      {Jason.decode!(body), metadata["flz.msg_id"]}
    end)
    |> Enum.unzip()
  end

  defp committed do
    assert_receive {:"$gen_cast", {:plugin_opt_persist, _, :cursor, %{at: at}}}, 5_000
    at
  end

  # One read of the reader's own, waited out; it found nothing to inject.
  defp poll_once(reader) do
    send(reader, :poll)
    assert wait_until(fn -> :sys.get_state(reader).read == nil end, 5_000)
  end

  # After a refusal the reader waits `interval_ms`; the next read offers the
  # refused row first and then the one after it.
  defp poll_once_after_refusal(reader) do
    send(reader, :poll)
    assert {~s|{"id":1}|, _} = ProviderPrepare.take_ingest()
    assert {~s|{"id":2}|, _} = ProviderPrepare.take_ingest()
  end

  defp health(opts, reader), do: PostgresPoll.health(Map.put(opts, :owned_pid, reader))

  defp await_health(opts, reader, verdict),
    do: wait_until(fn -> health(opts, reader) == verdict end, 5_000)
end
