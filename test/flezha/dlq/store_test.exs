defmodule Flezha.Dlq.StoreTest do
  # Rebinds the global :dlq config to a tmp dir.
  # async: false: writes Application env; a shared on-disk root; registered singletons.
  use FlezhaTest.Case, fixtures: [:isolated_dlq_dir], async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 3]

  alias Flezha.Dlq.Store

  setup %{dlq_dir: dir} do
    name = :"dlq_store_#{System.unique_integer([:positive])}"
    pid = start_supervised!(Supervisor.child_spec({Store, name: name}, id: name))
    %{store: pid, dir: dir, name: name}
  end

  defp entry(overrides \\ %{}) do
    Map.merge(
      %{
        relay_class: "MarketRelay",
        tasklet_id: "ohlc",
        cell_key: "BTC",
        channel: "input",
        origin_offset: nil,
        error: "boom",
        attempts: 3,
        payload: "poison-bytes",
        park_id: "seg#0"
      },
      overrides
    )
  end

  test "append persists the entry and payload blob", %{store: store} do
    assert {:ok, ref} = Store.append(store, entry())
    assert [listed] = Store.list(store, nil)
    assert listed["channel"] == "input"
    assert listed["error"] == "boom"
    assert listed["attempts"] == 3
    assert listed["generation"] == 1
    assert listed["status"] == "parked"

    assert {:ok, got, payload} = Store.get(store, ref)
    assert got["id"] == listed["id"]
    assert payload == "poison-bytes"
  end

  test "entries survive a store restart", %{store: store, name: name} do
    assert {:ok, ref} = Store.append(store, entry())
    stop_supervised!(name)

    fresh_name = :"dlq_store_#{System.unique_integer([:positive])}"
    fresh = start_supervised!(Supervisor.child_spec({Store, name: fresh_name}, id: fresh_name))

    assert [listed] = Store.list(fresh, nil)
    assert "MarketRelay/ohlc/" <> _ = ref
    assert {:ok, _entry, "poison-bytes"} = Store.get(fresh, ref)
    assert listed["error"] == "boom"
  end

  test "re-park of the same origin identity is idempotent", %{store: store} do
    assert {:ok, ref} = Store.append(store, entry())
    assert {:existing, ^ref} = Store.append(store, entry(%{attempts: 5}))

    assert [listed] = Store.list(store, nil)
    assert listed["attempts"] == 3, "idempotent re-park must not mutate the durable entry"
    assert listed["generation"] == 1
  end

  # `Flezha.Dlq.Store.path_component/1` is `safe/1`, the guard every identity
  # component off the wire passes through before it becomes a directory. The
  # character class permits `.` and `-`, so before this change `safe("..")`
  # came back `".."` and `Path.join/1` read it as a move out of the class
  # directory.
  test "a path component that would climb out of the root is a name, not a move" do
    assert Store.path_component("..") == "__"
    assert Store.path_component(".") == "_"
    assert Store.path_component("") == "_"
    assert Store.path_component("...") == "...", "only `.` and `..` move"
    assert Store.path_component("../../etc") == ".._.._etc"
    assert Store.path_component("ohlc") == "ohlc", "an ordinary id is untouched"
  end

  # The park a frame naming no declared entry channel writes: the identity slot
  # is `_unresolved`, chosen by the SDK, and the requested name is in the row's
  # channel field where nothing joins it to a path.
  test "an unresolved park lands in a listed tasklet directory", %{store: store} do
    assert {:ok, ref} =
             Store.append(
               store,
               entry(%{tasklet_id: "_unresolved", channel: "../../../../etc"})
             )

    assert [listed] = Store.list(store, nil)
    assert listed["channel"] == "../../../../etc"
    assert {"MarketRelay", "_unresolved"} in Store.tasklets("MarketRelay")
    assert {:ok, _entry, "poison-bytes"} = Store.get(store, ref)
  end

  # A runner mints a fresh `park_id` on every park, so an input-log replay of a
  # frame that parked once arrives with the same bytes and a different id. The
  # entry id is the ORIGIN identity, not the ack handle, which is what keeps a
  # replay from writing a second row per restart, without bound.
  test "a re-park under a fresh park_id is still one entry", %{store: store} do
    assert {:ok, ref} = Store.append(store, entry(%{park_id: "unresolved/not_prices/_entry#0"}))

    assert {:existing, ^ref} =
             Store.append(store, entry(%{park_id: "unresolved/not_prices/_entry#7"}))

    assert length(Store.list(store, nil)) == 1
  end

  test "tier-on parks dedup on cell@offset identity", %{store: store} do
    e = entry(%{origin_offset: 42, park_id: nil})
    assert {:ok, ref} = Store.append(store, e)
    assert {:existing, ^ref} = Store.append(store, e)
    assert length(Store.list(store, nil)) == 1

    assert {:ok, other} = Store.append(store, entry(%{origin_offset: 43, park_id: nil}))
    refute other == ref
    assert length(Store.list(store, nil)) == 2
  end

  test "re-park while replaying increments generation", %{store: store} do
    assert {:ok, ref} = Store.append(store, entry())
    assert :ok = Store.mark_replaying(store, ref)
    assert {:existing, ^ref} = Store.append(store, entry(%{attempts: 4, error: "boom again"}))

    assert [listed] = Store.list(store, nil)
    assert listed["generation"] == 2
    assert listed["status"] == "parked"
    assert listed["attempts"] == 4
    assert listed["error"] == "boom again"
  end

  test "drop removes entry and blob", %{store: store, dir: dir} do
    assert {:ok, ref} = Store.append(store, entry())
    assert :ok = Store.drop(store, ref)
    assert Store.list(store, nil) == []
    assert {:error, :not_found} = Store.get(store, ref)

    ["MarketRelay", "ohlc", id] = String.split(ref, "/")
    refute File.exists?(Path.join([dir, "MarketRelay", "ohlc", id <> ".bin"]))
  end

  test "a tasklet's directory goes with its last entry, and its class's with the last tasklet",
       %{store: store, dir: dir} do
    assert {:ok, first} = Store.append(store, entry(%{park_id: "a", payload: "aaaa"}))
    assert {:ok, second} = Store.append(store, entry(%{park_id: "b", payload: "bbbb"}))
    assert {:ok, other} = Store.append(store, entry(%{tasklet_id: "fills", park_id: "c"}))

    assert :ok = Store.drop(store, first)
    assert File.dir?(Path.join([dir, "MarketRelay", "ohlc"]))

    assert :ok = Store.drop(store, second)
    refute File.exists?(Path.join([dir, "MarketRelay", "ohlc"]))
    assert Store.tasklets() == [{"MarketRelay", "fills"}]

    assert :ok = Store.drop(store, other)
    refute File.exists?(Path.join(dir, "MarketRelay"))
    assert Store.stats(store) == []
  end

  test "the retention sweep removes an emptied set left on disk", %{store: store, dir: dir} do
    task_dir = Path.join([dir, "MarketRelay", "ohlc"])
    File.mkdir_p!(task_dir)
    File.write!(Path.join(task_dir, "entries.jsonl"), "")
    assert [%{entries: 0}] = Store.stats(store)

    assert :ok = Store.sweep(store)
    refute File.exists?(Path.join(dir, "MarketRelay"))
  end

  test "append_async runs the durable callback with the append's answer", %{store: store} do
    parent = self()
    :ok = Store.append_async(store, entry(), &send(parent, {:durable, &1}))
    assert_receive {:durable, {:ok, "MarketRelay/ohlc/" <> _}}, 2_000
    assert [_] = Store.list(store, nil)
  end

  test "byteless park still produces an inspectable entry", %{store: store} do
    assert {:ok, ref} = Store.append(store, entry(%{payload: nil}))
    assert {:ok, listed, nil} = Store.get(store, ref)
    assert listed["payload_bytes"] == nil
  end

  test "retention drops by age with loud telemetry", %{store: store} do
    handler = :"dlq_test_#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:flezha, :dlq, :retention_dropped],
      fn _event, measurements, metadata, _cfg ->
        send(parent, {:dropped, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    cfg = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.put(cfg, :retention_seconds, 0))

    assert {:ok, _ref} = Store.append(store, entry())
    # A zero retention expires an entry strictly older than the sweep's clock
    # reading, so the entry's own millisecond has to pass first.
    [%{"ts_ms" => ts_ms}] = Store.list(store, nil)
    assert wait_until(fn -> System.system_time(:millisecond) > ts_ms end, 1_000, 1)
    assert :ok = Store.sweep(store)

    assert_receive {:dropped, %{count: 1}, %{tasklet_id: "ohlc", reason: :age}}, 2_000
    assert Store.list(store, nil) == []
  end

  test "retention shrinks to the total-size cap, oldest first", %{store: store} do
    cfg = Application.get_env(:flezha, :dlq, [])
    # Each entry ≈ payload + 256 accounting bytes; cap admits only one.
    Application.put_env(:flezha, :dlq, Keyword.put(cfg, :max_total_bytes, 300))

    assert {:ok, first} = Store.append(store, entry(%{park_id: "a", payload: "aaaa"}))
    assert {:ok, _second} = Store.append(store, entry(%{park_id: "b", payload: "bbbb"}))

    assert :ok = Store.sweep(store)
    assert [survivor] = Store.list(store, nil)
    refute "MarketRelay/ohlc/#{survivor["id"]}" == first, "oldest entry must be dropped"
  end
end
