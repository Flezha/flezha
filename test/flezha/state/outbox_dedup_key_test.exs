defmodule Flezha.State.OutboxDedupKeyTest do
  use ExUnit.Case, async: true

  alias Flezha.State.Outbox

  test "an effectively-once output's durable identity is its cell, seq and epoch" do
    stamps =
      Outbox.output_stamps(%{
        cell: {"orders", "tally", "k1"},
        seq: "12.0",
        epoch: 3,
        channel: "out",
        key: "EUR-USD"
      })

    assert Outbox.dedup_key(stamps) == "orders/tally/k1/12.0/3"
  end

  test "an output without the effectively-once epoch has none" do
    assert Outbox.dedup_key(%{"flz.cell" => "orders/tally/k1", "flz.seq" => "12.0"}) == nil
    assert Outbox.dedup_key(%{"flz.key" => "EUR-USD"}) == nil
  end
end
