defmodule FlezhaTest.Service.LivenessFilterTest do
  @moduledoc """
  `Controller.liveness_counters/2` with a relay or namespace filter: only the
  relays in scope and their runners' channel counters, read by key, or with
  `limit: 0` only their summed counts.
  """
  # async: false: Mimic global mode; the node-wide liveness table.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.Controller
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Transport.LivenessCounters

  @caller %{source: :cli}

  # Relays of two namespaces, `orders` in both, with the messages each received.
  @scoped [{"lv-a", "orders", 3}, {"lv-a", "trades", 4}, {"lv-b", "orders", 5}]

  setup :set_mimic_global_until_exit

  setup do
    LivenessCounters.ensure_table()
    test = self()

    owners =
      for n <- 1..30 do
        spawn_link(fn ->
          LivenessCounters.register_relay("default", relay(n))
          if n <= 2, do: LivenessCounters.register_channel(runner(n), :control)
          send(test, :registered)
          Process.sleep(:infinity)
        end)
      end

    scoped =
      for {namespace, id, received} <- @scoped do
        spawn_link(fn ->
          ref = LivenessCounters.register_relay(namespace, id)
          LivenessCounters.add_relay(ref, :io_in_received, received)
          send(test, :registered)
          Process.sleep(:infinity)
        end)
      end

    for _ <- owners ++ scoped, do: assert_receive(:registered)
    Mimic.stub(RelayManager, :on_hub_member, fn _ns, _key, {m, f, a} -> apply(m, f, a) end)

    Mimic.stub(HubManager, :relay_rows_of, fn ns, ids ->
      for id <- ids, do: %{id: id, runner: runner_of(ns, id)}
    end)

    Mimic.stub(HubManager, :relay_rows, fn ns ->
      for {^ns, id, _received} <- @scoped, do: %{id: id, runner: runner_of(ns, id)}
    end)

    on_exit(fn -> Enum.each(owners ++ scoped, &Process.exit(&1, :kill)) end)
  end

  defp relay(n), do: "lv-#{n}"
  defp runner(n), do: "lv-runner-#{n}"

  defp runner_of("default", "lv-" <> n), do: runner(rem(String.to_integer(n), 2) + 1)
  defp runner_of("lv-a", _id), do: runner(1)
  defp runner_of("lv-b", _id), do: runner(2)

  defp read(opts) do
    {:ok, answer} = Controller.liveness_counters(opts, @caller)
    answer
  end

  test "a filter naming 3 relays answers those 3 and their runners' channels" do
    named = [{"default", relay(1)}, {"default", relay(3)}, ["default", relay(5)]]
    {:ok, answer} = Controller.liveness_counters([filter: [relays: named]], @caller)

    assert answer.relays |> Enum.map(& &1.relay_id) |> Enum.sort() == ["lv-1", "lv-3", "lv-5"]
    assert answer.channels |> Enum.map(& &1.runner_id) |> Enum.uniq() == [runner(2)]
  end

  test "the unfiltered answer still holds every relay" do
    {:ok, answer} = Controller.liveness_counters(@caller)
    ids = MapSet.new(answer.relays, & &1.relay_id)
    assert Enum.all?(1..30, &MapSet.member?(ids, relay(&1)))
  end

  test "a named relay is matched on its namespace too" do
    answer = read(filter: [relays: [{"lv-a", "orders"}]])

    assert [%{namespace: "lv-a", relay_id: "orders", io_in_received: 3}] = answer.relays
    assert answer.channels |> Enum.map(& &1.runner_id) |> Enum.uniq() == [runner(1)]
  end

  test "a namespace filter answers that namespace's relays and their runners' channels" do
    answer = read(filter: [namespace: "lv-a"])

    assert answer.relays |> Enum.map(&{&1.namespace, &1.relay_id}) |> Enum.sort() ==
             [{"lv-a", "orders"}, {"lv-a", "trades"}]

    assert answer.channels |> Enum.map(& &1.runner_id) |> Enum.uniq() == [runner(1)]
  end

  test "limit 0 answers no rows, only the namespace's counted slots summed" do
    rows = read(filter: [namespace: "lv-a"]).relays
    answer = read(filter: [namespace: "lv-a"], limit: 0)

    assert {answer.relays, answer.channels, answer.unreachable} == {[], [], []}
    assert answer.totals.io_in_received == 7
    assert answer.totals.forwarded == 7

    for {slot, sum} <- answer.totals,
        do: assert(sum == rows |> Enum.map(&Map.fetch!(&1, slot)) |> Enum.sum(), "#{slot}")
  end

  test "limit 0 sums named relays, or every relay when no filter is given" do
    assert read(filter: [relays: [{"lv-b", "orders"}]], limit: 0).totals.io_in_received == 5
    assert read(limit: 0).totals.io_in_received >= 12
  end

  test "501 relays, both scopes at once, a limit above 0 or a paging key is refused" do
    many = for n <- 1..501, do: {"default", "x#{n}"}

    for opts <- [
          [filter: [relays: many]],
          [filter: [relays: [{"lv-a", "orders"}], namespace: "lv-a"]],
          [filter: [namespace: 7]],
          [limit: 5]
        ],
        do:
          assert(
            {:error, %Flezha.Error{code: "usage.invalid_number"}} =
              Controller.liveness_counters(opts, @caller)
          )

    assert {:error, %Flezha.Error{code: "control.unknown_parameter"}} =
             Controller.liveness_counters([after: "x"], @caller)
  end
end
