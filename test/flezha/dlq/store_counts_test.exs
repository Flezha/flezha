defmodule Flezha.Dlq.StoreCountsTest do
  @moduledoc """
  The dead-letter Store's in-memory counts and its paged list: stats equal a
  full decode after every kind of change and read no index, a Store killed
  mid-sweep restarts counting what is on disk, and a page decodes only its own
  entries without repeating or skipping one across a drop.
  """
  # async: false: writes Application env; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:isolated_dlq_dir], async: false

  import ExUnit.CaptureLog

  alias Flezha.Cluster.Page
  alias Flezha.Dlq.Store

  setup do
    name = :"dlq_counts_#{System.unique_integer([:positive])}"
    pid = start_supervised!(Supervisor.child_spec({Store, name: name}, id: name))
    %{store: pid, name: name}
  end

  defp park(store, tasklet, n, payload \\ nil) do
    entry = %{
      relay_class: "CountRelay",
      tasklet_id: tasklet,
      cell_key: "k",
      channel: "input",
      origin_offset: n,
      error: ~s(boom "ts_ms":1 "id":"x),
      attempts: 1,
      payload: payload || "p#{n}"
    }

    {:ok, ref} = Store.append(store, entry)
    ref
  end

  defp cap_total_bytes(cap) do
    cfg = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.put(cfg, :max_total_bytes, cap))
    on_exit(fn -> Application.put_env(:flezha, :dlq, cfg) end)
  end

  # What a full decode of the index finds, as stats names it.
  defp decoded(store) do
    store
    |> Store.list(nil)
    |> Enum.frequencies_by(&{&1["relay_class"], &1["tasklet_id"]})
  end

  defp counted(store),
    do: Map.new(Store.stats(store), &{{&1.relay_class, &1.tasklet_id}, &1.entries})

  test "stats equal a full decode after parks, drops, replays and a sweep", %{store: store} do
    refs = for n <- 1..6, do: park(store, if(n <= 3, do: "a", else: "b"), n)
    assert counted(store) == decoded(store)

    :ok = Store.drop(store, Enum.at(refs, 0))
    :ok = Store.mark_replaying(store, Enum.at(refs, 1))

    {:existing, _} =
      Store.append(store, %{
        relay_class: "CountRelay",
        tasklet_id: "a",
        cell_key: "k",
        channel: "input",
        origin_offset: 2,
        payload: "p2"
      })

    assert counted(store) == decoded(store) and counted(store)[{"CountRelay", "a"}] == 2

    cap_total_bytes(0)
    :ok = Store.sweep(store)
    assert counted(store) == decoded(store) and counted(store) == %{}
  end

  test "stats open no index file", %{store: store} do
    for n <- 1..3, do: park(store, "a", n)
    index = Path.join(Store.task_dir("CountRelay", "a"), "entries.jsonl")
    File.chmod!(index, 0o000)
    on_exit(fn -> File.chmod(index, 0o644) end)

    assert [%{entries: 3, bytes: bytes}] = Store.stats(store)
    assert bytes > 0
  end

  test "a Store killed during a sweep restarts with the counts on disk", %{
    store: store,
    name: name
  } do
    for n <- 1..20, do: park(store, "s#{rem(n, 4)}", n, :binary.copy("x", 1000))
    cap_total_bytes(8_000)

    # Wherever the kill lands in the sweep, the restart counts what is on disk.
    capture_log(fn ->
      Task.start(fn -> catch_exit(Store.sweep(store)) end)
      Process.exit(store, :kill)
      stop_supervised(name)
    end)

    fresh = :"dlq_counts_#{System.unique_integer([:positive])}"
    restarted = start_supervised!(Supervisor.child_spec({Store, name: fresh}, id: fresh))
    assert counted(restarted) == decoded(restarted)
  end

  test "pages decode only their entries and survive a drop between two pages", %{store: store} do
    refs = for n <- 1..7, do: park(store, if(rem(n, 2) == 0, do: "a", else: "b"), n)
    {:ok, page} = Page.parse([limit: 3], :dlq_list, sorts: [newest: :desc])

    first = Store.list_page(store, nil, page)
    assert first.total == 7 and length(first.rows) == 3
    assert Enum.all?(first.rows, &(&1.node == node()))

    shown = MapSet.new(first.rows, & &1["id"])

    hidden =
      Enum.find(refs, fn ref ->
        not MapSet.member?(shown, ref |> String.split("/") |> List.last())
      end)

    :ok =
      Store.drop(
        store,
        hd(Enum.filter(refs, &MapSet.member?(shown, List.last(String.split(&1, "/")))))
      )

    {:ok, next} = Page.parse([limit: 10, after: first.next], :dlq_list, sorts: [newest: :desc])
    second = Store.list_page(store, nil, next)
    ids = Enum.map(first.rows ++ second.rows, & &1["id"])

    assert length(ids) == 7 and Enum.uniq(ids) == ids
    assert List.last(String.split(hidden, "/")) in ids
    keys = Enum.map(first.rows ++ second.rows, &Store.page_key/1)
    assert keys == Enum.sort(keys, :desc)
  end
end
