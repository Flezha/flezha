defmodule Flezha.Service.RelayListPageTest do
  @moduledoc """
  `Relay.list/3` with `opts`: one page of a namespace's relay rows, its total,
  a cursor to the next page and the namespace summary, through the real read
  path with the registry's rows stubbed and the published verdicts real.
  """
  # async: false: Mimic global mode; the published convergence snapshot.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Convergence.Snapshot
  alias Flezha.Partition
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias Flezha.Service.Relay
  alias FlezhaTest.Helper.ConvergenceFixtures

  @ns "default"
  @caller %{source: :cli}

  setup :set_mimic_global_until_exit

  setup do
    prev = Snapshot.current()
    on_exit(fn -> Snapshot.publish(prev) end)
  end

  defp relay(n, opts \\ []) do
    %{
      id: "r" <> String.pad_leading(Integer.to_string(n), 4, "0"),
      health: Keyword.get(opts, :health, :healthy),
      runner: Keyword.get(opts, :runner, "runner-1"),
      factory: Keyword.get(opts, :factory, "orders"),
      partition: Keyword.get(opts, :partition),
      node: node(),
      run: :flow,
      plugins: 0
    }
  end

  defp fleet(rows), do: ConvergenceFixtures.stub_registry_rows(@ns, rows, 500_000)

  test "pages through 120 relays 50 at a time, each id once, with the total" do
    fleet(for n <- 1..120, do: relay(n))

    pages =
      Stream.unfold(nil, fn
        :done ->
          nil

        cursor ->
          opts = if cursor, do: [limit: 50, after: cursor], else: [limit: 50]
          {:ok, page} = Relay.list(@ns, opts, @caller)
          {page, page.next || :done}
      end)
      |> Enum.to_list()

    assert Enum.map(pages, &length(&1.rows)) == [50, 50, 20]
    assert Enum.all?(pages, &(&1.total == 120))
    assert List.last(pages).next == nil
    ids = Enum.flat_map(pages, fn page -> Enum.map(page.rows, & &1.id) end)
    assert ids == Enum.map(1..120, &relay(&1).id)
  end

  defp member(group, index, count, health) do
    %{
      relay(0, health: health, factory: "candles", runner: "runner-#{index}")
      | id: Partition.member_id(group, index),
        partition: %{
          group: group,
          index: index,
          count: count,
          range: Enum.at(Partition.ranges(count), index)
        }
    }
  end

  # 6 healthy, 3 unbound past the grace (waiting on "orders"), 1 failed of
  # another factory, and a 2-wide group whose second member failed.
  defp mixed_fleet() do
    rows =
      for(n <- 1..6, do: relay(n)) ++
        for(n <- 7..9, do: relay(n, health: :unbound, runner: nil)) ++
        [relay(10, health: :failed, factory: "prices", runner: "runner-2")] ++
        [member("ohlc", 0, 2, :healthy), member("ohlc", 1, 2, :failed)]

    fleet(rows)
    rows
  end

  defp page(opts) do
    {:ok, page} = Relay.list(@ns, opts, @caller)
    page
  end

  defp ids(page), do: Enum.map(page.rows, &to_string(&1.id))

  test "each filter keeps exactly the rows it names" do
    mixed_fleet()

    assert ids(page(filter: [health: ["failed"]])) == ["ohlc", "r0010"]

    assert ids(page(filter: [health: "unhealthy"])) == [
             "ohlc",
             "r0007",
             "r0008",
             "r0009",
             "r0010"
           ]

    assert ids(page(filter: [factory: "prices"])) == ["r0010"]

    assert ids(page(filter: [runner: "runner-1"])) ==
             ["ohlc", "r0001", "r0002"] ++ ~w(r0003 r0004 r0005 r0006)

    assert ids(page(filter: [node: to_string(node())])) |> length() == 11
    assert ids(page(filter: [group: "ohlc"])) == ["ohlc"]
    assert ids(page(filter: [text: "R001"])) == ["r0010"]
    assert ids(page(filter: %{"text" => "CANDLE"})) == ["ohlc"]
  end

  test "the severity sort puts the worst first, then by id" do
    mixed_fleet()
    assert ids(page(sort: "severity", limit: 3)) == ["ohlc", "r0010", "r0007"]
  end

  test "limit 0 answers only the summary, counted as the unpaged list counts" do
    rows = mixed_fleet()
    {:ok, %{rows: unpaged}} = Relay.list(@ns, @caller)
    summary = page(limit: 0)

    assert summary.rows == [] and summary.next == nil and summary.total == 11
    assert summary.summary.health == Enum.frequencies_by(unpaged, & &1.health)
    assert summary.summary.waiting == [%{factory: "orders", relays: 3}]

    members = Enum.flat_map(unpaged, &(&1[:members] || []))
    {:ok, groups} = Relay.partition_groups(members, @caller)

    faulted =
      for g <- groups, Enum.any?(g.members, &(&1.health == :failed)) do
        %{group: g.group, health: :failed, complete: g.coverage == :ok}
      end

    assert summary.summary.faulted_groups == faulted
    assert faulted == [%{group: "ohlc", health: :failed, complete: true}]
    assert length(rows) == 12
  end

  test "an ids filter reads those entries alone and answers the unpaged row" do
    mixed_fleet()
    {:ok, %{rows: unpaged}} = Relay.list(@ns, @caller)
    Mimic.reject(HubManager, :relay_rows, 2)

    one = page(filter: [ids: ["r0010"]])
    assert one.summary == nil
    assert [row] = one.rows
    assert row == Enum.find(unpaged, &(&1.id == "r0010"))

    assert [group] = page(filter: [ids: ["ohlc"]]).rows
    assert length(group.members) == 2
  end

  test "the unpaged answer is the one it was" do
    mixed_fleet()
    {:ok, answer} = Relay.list(@ns, @caller)
    assert answer |> Map.keys() |> Enum.sort() == [:rows, :unreachable]
    assert length(answer.rows) == 11
  end

  test "a filter or limit the read does not take is refused before any work" do
    mixed_fleet()
    refused = &(Relay.list(@ns, &1, @caller) |> elem(1))

    assert %Flezha.Error{code: "control.unknown_parameter"} = refused.(filter: [colour: "red"])
    assert %Flezha.Error{code: "usage.invalid_number"} = refused.(limit: 501)
    assert %Flezha.Error{code: "usage.unknown_word"} = refused.(sort: "age")

    assert %Flezha.Error{code: "usage.invalid_number"} =
             refused.(filter: [ids: Enum.map(1..501, &"r#{&1}")])
  end

  test "flz relay list --limit prints the page, the total and the next cursor" do
    mixed_fleet()
    args = Jason.encode!(["default", %{"limit" => 2, "filter" => %{"health" => "unhealthy"}}])
    text = capture_io(fn -> Cli.call("Relay.list", args, []) end)

    assert text =~ "ohlc" and text =~ "r0007"
    refute text =~ "r0008"
    assert text =~ "Showing 2 of 5."
    assert [_, cursor] = Regex.run(~r/Next page: --after (\S+)/, text)

    args =
      Jason.encode!([
        "default",
        %{"limit" => 9, "after" => cursor, "filter" => %{"health" => "unhealthy"}}
      ])

    rest = capture_io(fn -> Cli.call("Relay.list", args, []) end)
    assert rest =~ "Showing 3 of 5." and not (rest =~ "Next page")

    whole = capture_io(fn -> Cli.call("Relay.list", Jason.encode!(["default"]), []) end)
    refute whole =~ "Showing"
  end
end
