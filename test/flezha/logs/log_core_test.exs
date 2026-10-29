defmodule Flezha.Logs.LogCoreTest do
  @moduledoc """
  The shared log core under a kind of its own: the group-committing writer and
  its bounds, the reader newest first and oldest first, and a per-controller
  read that stops at the newest records it was asked for and orders a file
  written before records carried wall-clock times by their ISO time.
  """

  # async: false: traces `:file.sync/1` and the reader globally; restarts the
  # relay history kind under the application's logs supervisor.
  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false
  use Mimic

  alias Flezha.Cluster.Page
  alias Flezha.Logs.{Codec, Kinds, Local, Reader, RelayHistory, Seen, Writer}
  alias Flezha.Relay.Builder

  @moduletag :capture_log

  # A direct kind of the test's own, registered beside the real ones.
  defp test_kind(keys \\ %{}) do
    kind = Kinds.fetch!(:relay_history)
    overrides = Map.new(keys, fn {role, value} -> {role, {:value, value}} end)

    struct!(kind,
      id: :"log_core_#{System.unique_integer([:positive])}",
      prefix: "test_log",
      children: [],
      keys: Map.merge(kind.keys, overrides)
    )
  end

  defp start_writer(kind, dir) do
    pid = start_supervised!(%{id: kind.id, start: {Writer, :start_link, [kind, [dir: dir]]}})
    {kind, pid}
  end

  defp line(n), do: ~s({"ts_us":#{n},"event":"started","node":"n@h"}\n)

  defp queued(pid, n) do
    FlezhaTest.Helper.NodeTestHelper.wait_until(
      fn -> Process.info(pid, :message_queue_len) == {:message_queue_len, n} end,
      5_000
    )
  end

  describe "the writer" do
    test "appends queued at one moment are answered after one shared sync", %{tmp: dir} do
      {kind, pid} = start_writer(test_kind(), dir)
      :erlang.trace(pid, true, [:call, {:tracer, self()}])
      :erlang.trace_pattern({:file, :sync, 1}, true, [:local])

      on_exit(fn -> :erlang.trace_pattern({:file, :sync, 1}, false, [:local]) end)

      :ok = :sys.suspend(pid)
      tasks = for n <- 1..50, do: Task.async(fn -> Writer.append(kind, line(n)) end)
      assert queued(pid, 50)
      :ok = :sys.resume(pid)

      assert Enum.map(tasks, &Task.await/1) == List.duplicate(:ok, 50)
      syncs = count_syncs(pid, 0)
      assert syncs >= 1 and syncs < 50, "#{syncs} syncs served 50 appends"
      assert Writer.stats(kind).current_record_count == 50
    end

    test "a queue at its bound refuses at once and counts the refusal", %{tmp: dir} do
      {kind, pid} = start_writer(test_kind(max_pending: 2), dir)
      :ok = :sys.suspend(pid)
      waiting = for n <- 1..2, do: Task.async(fn -> Writer.append(kind, line(n)) end)
      assert queued(pid, 2)

      {elapsed_us, refused} = :timer.tc(fn -> Writer.append(kind, line(3)) end)
      assert refused == {:error, :overloaded}
      assert elapsed_us < 100_000
      assert Writer.counters(kind).dropped_count == 1

      :ok = :sys.resume(pid)
      assert Enum.map(waiting, &Task.await/1) == [:ok, :ok]
    end

    test "an append that outlives its bound is counted, and its record still lands", %{tmp: dir} do
      {kind, pid} = start_writer(test_kind(append_ms: 100), dir)
      :ok = :sys.suspend(pid)

      assert Writer.append(kind, line(1)) == {:error, :timeout}
      assert Writer.counters(kind).timeout_count == 1

      :ok = :sys.resume(pid)
      assert Writer.stats(kind).current_record_count == 1

      assert [%{"ts_us" => 1}] =
               dir |> Path.join("test_log-current.jsonl") |> Reader.stream() |> Enum.to_list()
    end
  end

  describe "the reader" do
    test "newest first, a record split across a block boundary reads whole", %{tmp: dir} do
      path = Path.join(dir, "big.jsonl")
      padding = String.duplicate("x", 40_000)
      lines = for n <- 1..5, do: ~s({"ts_us":#{n},"pad":"#{padding}"}\n)
      File.write!(path, lines)

      assert path |> Reader.reverse_stream() |> Enum.map(fn {_off, r} -> r["ts_us"] end) == [
               5,
               4,
               3,
               2,
               1
             ]

      offsets = path |> Reader.reverse_stream() |> Enum.map(&elem(&1, 0)) |> Enum.reverse()
      assert offsets == path |> Reader.stream(offsets: true) |> Enum.map(&elem(&1, 0))
    end

    test "an empty file, a truncated last line and a gzipped file", %{tmp: dir} do
      empty = Path.join(dir, "empty.jsonl")
      File.write!(empty, "")
      assert Enum.to_list(Reader.reverse_stream(empty)) == []

      truncated = Path.join(dir, "truncated.jsonl")
      File.write!(truncated, ~s({"ts_us":1}\n{"ts_us":2}\n{"ts_us":3,"cut))

      assert truncated |> Reader.reverse_stream() |> Enum.map(fn {_o, r} -> r["ts_us"] end) == [
               2,
               1
             ]

      assert truncated |> Reader.stream() |> Enum.map(& &1["ts_us"]) == [1, 2]

      gz = Path.join(dir, "rotated.jsonl.gz")
      File.write!(gz, :zlib.gzip(Enum.map(1..4, &~s({"ts_us":#{&1}}\n))))
      newest = Reader.reverse_stream(gz, limit: 2) |> Enum.map(fn {_o, r} -> r["ts_us"] end)
      assert newest == [4, 3]
    end
  end

  describe "a read on one controller" do
    setup %{tmp: dir} do
      history_in(dir)
      %{kind: Kinds.fetch!(:relay_history)}
    end

    test "the newest ten open no file older than the tenth match", %{tmp: dir, kind: kind} do
      for file <- 1..3 do
        for n <- 1..10, do: :ok = append(kind, file * 100 + n)
        {:ok, _} = Writer.rotate_now(kind)
      end

      for n <- 1..5, do: :ok = append(kind, 400 + n)

      reader =
        Task.async(fn -> receive(do: (:go -> Local.read(:relay_history, query(), page(10)))) end)

      :erlang.trace(reader.pid, true, [:call, {:tracer, self()}])
      :erlang.trace_pattern({Reader, :reverse_stream, 2}, true, [:local])
      on_exit(fn -> :erlang.trace_pattern({Reader, :reverse_stream, 2}, false, [:local]) end)
      send(reader.pid, :go)
      %{rows: rows} = Task.await(reader)

      assert Enum.map(rows, fn {_key, r} -> r["first_started_at"] end) ==
               Enum.to_list(405..401//-1) ++ Enum.to_list(310..306//-1)

      opened = opened([])
      assert length(opened) == 2, "opened #{inspect(opened)} in #{dir}"
    end

    test "a file written before wall-clock times orders by its ISO time", %{tmp: dir, kind: kind} do
      old_iso = DateTime.utc_now() |> DateTime.add(-60) |> DateTime.to_iso8601()

      File.write!(
        Path.join(dir, "relay_history-20200101T000000Z-1.jsonl"),
        ~s({"ts_us":12345,"ts_iso":"#{old_iso}","event":"started","relay":"old"}\n)
      )

      # The catalog the first start wrote does not name the file yet.
      File.rm!(Path.join(dir, "INDEX"))
      restart_history()
      :ok = append(kind, 1)

      %{rows: rows} = Local.read(:relay_history, query(), page(10))
      assert Enum.map(rows, fn {_key, r} -> r["relay"] end) == ["r1", "old"]
      assert Codec.order_us(elem(List.last(rows), 1)) < Codec.order_us(elem(hd(rows), 1))
    end
  end

  describe "the spawn lookup" do
    setup %{tmp: dir} do
      history_in(dir)
    end

    test "finds a run by id and by spawn identity, again after a restart" do
      origin = %{kind: "spawn", parent_relay: "ma_fan", spawn_identity: "ident-1"}

      relay = %Builder{
        id: "child-1",
        factory_id: "fan_trade",
        run: :steps,
        opts: [origin: origin]
      }

      :ok = RelayHistory.started("ns", relay)
      assert {:started, "child-1", _ts} = Seen.lookup("ns", {:relay, "child-1"})
      assert {:started, "child-1", _ts} = Seen.lookup("ns", {:spawn_identity, "ident-1"})
      assert Seen.lookup("other_ns", {:relay, "child-1"}) == nil

      restart_history()
      assert {:started, "child-1", _ts} = RelayHistory.seen("ns", {:spawn_identity, "ident-1"})
      assert RelayHistory.seen("ns", {:relay, "never-ran"}) == :none
    end

    test "answers unknown, never none, while a controller does not answer" do
      ghost = :"history_ghost@127.0.0.1"
      stub(Flezha.Cluster.Directory, :controller_nodes, fn -> [node(), ghost] end)
      assert RelayHistory.seen("ns", {:relay, "never-ran"}) == {:unknown, [ghost]}
    end

    test "an ended run is not taken back by a late started record" do
      Seen.note(%{"event" => "ended", "namespace" => "ns", "relay" => "r", "ts_us" => 2})
      Seen.note(%{"event" => "started", "namespace" => "ns", "relay" => "r", "ts_us" => 3})
      assert Seen.lookup("ns", {:relay, "r"}) == {:ended, "r", 2}
    end

    test "past its cap the oldest keys go and are counted" do
      Application.put_env(
        :flezha,
        :relay_history,
        Keyword.put(Application.get_env(:flezha, :relay_history), :dedup_max_entries, 2)
      )

      before = Seen.evicted()

      for n <- 1..3,
          do:
            Seen.note(%{
              "event" => "started",
              "namespace" => "ns",
              "relay" => "r#{n}",
              "ts_us" => n
            })

      assert Seen.evicted() == before + 1
      assert Seen.lookup("ns", {:relay, "r1"}) == nil
      assert Seen.lookup("ns", {:relay, "r3"}) == {:started, "r3", 3}
    end

    test "answers disabled when the kind is off" do
      Application.put_env(
        :flezha,
        :relay_history,
        Keyword.put(Application.get_env(:flezha, :relay_history), :enabled, false)
      )

      assert RelayHistory.seen("ns", {:relay, "r"}) == :disabled
    end
  end

  defp append(kind, n) do
    Flezha.Logs.append(kind.id, %{
      "ts_us" => System.os_time(:microsecond),
      "event" => "started",
      "namespace" => "ns",
      "relay" => "r#{n}",
      "first_started_at" => n
    })
  end

  defp query(), do: %{filters: %{}, since_us: nil, until_us: nil, namespace: :all}

  defp page(limit) do
    {:ok, page} =
      Page.parse(%{limit: limit}, :logs_read,
        sorts: [newest: :desc],
        keys: [:limit, :before],
        cursor: :before,
        limit: {100, 1_000}
      )

    page
  end

  # The relay history kind over `dir`, put back over its own directory after.
  defp history_in(dir) do
    env = Application.get_env(:flezha, :relay_history, [])
    Application.put_env(:flezha, :relay_history, Keyword.put(env, :dir, dir))
    restart_history()

    on_exit(fn ->
      Application.put_env(:flezha, :relay_history, env)
      restart_history()
    end)
  end

  defp restart_history() do
    _ = Supervisor.terminate_child(Flezha.Logs.Supervisor, :relay_history)
    {:ok, _} = Supervisor.restart_child(Flezha.Logs.Supervisor, :relay_history)
  end

  defp opened(acc) do
    receive do
      {:trace, _pid, :call, {Reader, :reverse_stream, [path | _]}} ->
        opened([Path.basename(path) | acc])
    after
      0 -> acc
    end
  end

  defp count_syncs(pid, n) do
    receive do
      {:trace, ^pid, :call, {:file, :sync, _}} -> count_syncs(pid, n + 1)
    after
      200 -> n
    end
  end
end
