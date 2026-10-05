defmodule FlezhaTest.System.PostgresPollE2ETest do
  @moduledoc """
  The Postgres poll source feeding a live effectively-once relay
  (`tier_store`) through restarts: every row of a 10 000-row table reaches the
  relay although its Overwatch is killed mid-read, and rows read again while
  the relay's input log still holds them are absorbed, so each reaches the
  relay once.

  `tier_store` splits its input at the first `:`, so the row `{"k":"r17"}`
  comes out as `{"k"="r17"}`: the row's value is in every output, and the
  outputs say which rows the relay ran on.

  Tagged `:system` + `:postgres_integration`; run with:

      POSTGRES_INTEGRATION=1 mix test --include system \\
        test/system/postgres_poll_e2e_test.exs

  Pre-req: `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink`.
  """

  use FlezhaTest.SystemCase, capability: :effectively_once

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Overwatch
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.PostgresPoll
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.PostgresSink
  alias FlezhaTest.Helper.Telemetry

  @moduletag :postgres_integration
  @moduletag timeout: 300_000

  @moduletag skip:
               FlezhaTest.SystemCase.skip_unless(
                 System.get_env("POSTGRES_INTEGRATION") == "1",
                 "needs POSTGRES_INTEGRATION=1 and the postgres-sink container up"
               )

  @rows 10_000
  @again 50

  setup do
    db = PostgresSink.connect!()
    table = "poll_e2e_#{System.unique_integer([:positive])}"
    Postgrex.query!(db, "CREATE TABLE #{table} (id bigint PRIMARY KEY, k text NOT NULL)", [])

    on_exit(fn ->
      db = PostgresSink.connect!()
      Postgrex.query!(db, "DROP TABLE IF EXISTS #{table}", [])
    end)

    %{db: db, table: table}
  end

  test "every row arrives through an Overwatch kill, and a re-read is absorbed",
       %{db: db, table: table} do
    namespace = unique("pg_poll")
    relay_id = unique("tier_store")
    id_path = unique_id_path()
    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    insert(db, table, 1, @rows)
    {port, _runner} = boot_runner(namespace, id_path)

    source = %Flezha.Plugin{
      id: "rows_in",
      provider: PostgresPoll,
      opts:
        PostgresSink.conn_opts()
        |> Map.new()
        |> Map.merge(%{table: table, id_column: "id", columns: "k", channel: "input"}),
      hooks: [{:on_terminate, []}]
    }

    sink = %Flezha.Plugin{
      id: "tier_sink",
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      hooks: [{:on_output, []}]
    }

    [source, _sink] = bindings = RelayFixture.bindings(namespace, relay_id, [source, sink])

    builder = %Builder{
      id: relay_id,
      factory_id: "tier_store",
      init_values: %{},
      opts: [bindings: bindings]
    }

    assert {:ok, _info} = RelayManager.start_relay(namespace, builder)
    assert wait_until(fn -> ClassRegistry.effectively_once?(relay_id) end, 30_000)

    # --- the Overwatch killed mid-read ---------------------------------------
    assert wait_until(fn -> map_size(seen()) >= div(@rows, 5) end, 60_000),
           "the relay never ran on the first rows"

    force_fresh_rebind(namespace, relay_id)

    assert wait_until(fn -> map_size(seen()) == @rows end, 120_000),
           "rows were lost across the kill: #{@rows - map_size(seen())} never arrived"

    # --- the runner stalled, the cursor moved back, the reader brought up again --
    _ = drain_outputs([])
    _ = await_reader(source.id, ["#{@rows}"])
    os_pid = os_pid(port)
    {_, 0} = System.cmd("kill", ["-STOP", "#{os_pid}"])
    on_exit(fn -> System.cmd("kill", ["-CONT", "#{os_pid}"]) end)

    insert(db, table, @rows + 1, @rows + @again)
    reader = await_reader(source.id, ["#{@rows + @again}"])

    Telemetry.capture(
      [
        [:flezha, :plugin, :postgres_poll, :shipped],
        [:flezha, :plugin, :postgres_poll, :caught_up]
      ],
      filter: &(&1.plugin_id == source.id)
    )

    {:ok, overwatch} = RelayManager.get_relay_pid(namespace, relay_id)
    cursor = %{:sys.get_state(reader).committed | at: ["#{@rows}"]}
    :ok = Host.persist_opt(overwatch, source.id, :cursor, cursor)
    Process.exit(reader, :kill)
    assert :ok = Overwatch.plugin_restart(overwatch, source.id, source)

    assert read_again() == @again, "the reader did not read the rows past the cursor again"

    {_, 0} = System.cmd("kill", ["-CONT", "#{os_pid}"])

    assert wait_until(fn -> map_size(seen()) == @again end, 60_000),
           "the rows read again did not all reach the relay"

    assert await_outbox_drained(relay_id), "the outbox never drained"
    counts = drain_outputs([]) |> Enum.map(&row/1) |> Enum.frequencies()

    assert counts == Map.new((@rows + 1)..(@rows + @again), &{&1, 1}),
           "a row read again reached the relay twice"
  end

  defp insert(db, table, from, to) do
    Postgrex.query!(
      db,
      "INSERT INTO #{table} SELECT g, 'r' || g FROM generate_series($1::bigint, $2::bigint) g",
      [from, to]
    )
  end

  # The rows the relay ran on so far, by number, from the outputs not taken.
  defp seen, do: Map.new(peek_outputs(), &{row(&1), true})

  defp row({output, _metadata}) do
    [_, n] = Regex.run(~r/"r(\d+)"/, output)
    String.to_integer(n)
  end

  # The live reader, once it has committed `at`.
  defp await_reader(plugin_id, at) do
    assert wait_until(
             fn ->
               case WorkerSup.worker_pid(PostgresPoll, plugin_id) do
                 pid when is_pid(pid) -> :sys.get_state(pid).committed.at == at
                 nil -> false
               end
             end,
             60_000
           ),
           "the reader never committed #{inspect(at)}"

    WorkerSup.worker_pid(PostgresPoll, plugin_id)
  end

  # Every row the brought-up reader handed over until it caught up.
  defp read_again(total \\ 0) do
    receive do
      {:telemetry, [_, _, _, :shipped], %{count: count}, _metadata} -> read_again(total + count)
      {:telemetry, [_, _, _, :caught_up], _measurements, _metadata} -> total
    after
      60_000 -> flunk("the brought-up reader never caught up")
    end
  end
end
