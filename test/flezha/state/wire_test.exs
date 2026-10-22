defmodule Flezha.State.WireTest do
  @moduledoc """
  The postcard bodies the controller decodes off a runner's frames, read from
  `test/fixtures/postcard_bodies.txt`: the bytes `flz_core` encodes from the
  real `EffectRecordBody`, `ParkBody` and `CorrectionRequestBody`. A field the
  core reorders or adds changes a row, and the decode below stops matching.
  The tests' effect and park encoders write the same rows back.
  """
  use ExUnit.Case, async: true

  alias Flezha.State.Wire
  alias FlezhaTest.Helper.WireEncoders

  @fixture Path.join([__DIR__, "..", "..", "fixtures", "postcard_bodies.txt"])
  @external_resource @fixture

  @dead_letter %{channel: "in", error: "boom", attempts: 3}

  @effects %{
    "outputs_edges_timer_register" => %{
      changes: [<<0xAA>>],
      outputs: [%{channel: "out", key: "k", metadata: [{"a", "b"}], payload: <<1, 2>>}],
      consumed_offset: 4711,
      dlq: nil,
      event_time: 1_700_000_000_123,
      timers: [%{kind: :processing_time, scheduled_at: 31_000, register: true}],
      edges: [%{channel: 2, consumer: 1, origin: 4711, payload: <<9>>}]
    },
    "dead_letter_timer_cancel" => %{
      changes: [],
      outputs: [],
      consumed_offset: 9,
      dlq: Map.put(@dead_letter, :payload, nil),
      event_time: 5,
      timers: [%{kind: :event_time, scheduled_at: 2_000, register: false}],
      edges: []
    }
  }

  @parks %{
    "parked" =>
      Map.merge(@dead_letter, %{
        park_id: "s#0",
        origin_offset: 7,
        payload: <<1, 2>>,
        reply_to: "w:t"
      })
  }

  @corrections %{"request" => 1_700_000_000_123}

  defp rows(type) do
    @fixture
    |> File.read!()
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.map(&String.split(&1, "|"))
    |> Enum.flat_map(fn
      [^type, case_name, hex] -> [{case_name, Base.decode16!(hex, case: :lower)}]
      _ -> []
    end)
  end

  # Every row of `type` decodes to its expected value, and every expected
  # value has its row: a case the core adds or drops fails here by name. With
  # an encoder, the value also encodes back to the row.
  defp assert_decodes(type, expected, decode, encode \\ nil) do
    rows = rows(type)
    assert Enum.sort(Enum.map(rows, &elem(&1, 0))) == Enum.sort(Map.keys(expected))

    for {case_name, body} <- rows do
      value = Map.fetch!(expected, case_name)
      assert decode.(body) == {:ok, value}, "#{type}|#{case_name}"
      if encode, do: assert(encode.(value) == body, "#{type}|#{case_name} encodes")
    end
  end

  test "every effect record row decodes to the record the core encoded, and back" do
    assert_decodes(
      "effect",
      @effects,
      &Wire.decode_effect_body/1,
      &WireEncoders.encode_effect_body/1
    )
  end

  test "every park row decodes to the park the core encoded, and back" do
    assert_decodes("park", @parks, &Wire.decode_park_body/1, &WireEncoders.encode_park_body/1)
  end

  test "every correction row decodes to the target event time the core encoded" do
    assert_decodes("correction", @corrections, &Wire.decode_correction_request/1)
  end

  # Fails when: a relay class or tasklet id that would name another directory
  # under the state root is decoded and joined into a path.
  test "a cell id whose relay class or tasklet id names another directory is refused" do
    for bad <- [".", "..", "../x", "a/b", "/abs", "a\0b"],
        {rc, sh} <- [{bad, "task"}, {"relay", bad}, {"", "task"}] do
      assert {:error, _} = Wire.decode_cell_id(Wire.encode_cell_id(rc, sh, "k")),
             "#{inspect({rc, sh})} decoded"
    end

    # A partition member, a schedule slot and a dotted name are directory names,
    # and a relay-level frame (a processed prefix) names no tasklet.
    for {rc, sh} <- [
          {"orders#p15", "Elixir.Shop.Tasks"},
          {"tick@2026-01-01T00:00:00Z", "..x"},
          {"orders", ""}
        ] do
      assert Wire.decode_cell_id(Wire.encode_cell_id(rc, sh, "../k")) == {:ok, {rc, sh, "../k"}}
    end
  end
end
