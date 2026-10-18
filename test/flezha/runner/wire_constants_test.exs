defmodule Flezha.Runner.WireConstantsTest do
  @moduledoc """
  The controller's copies of the wire constants, held to
  `test/fixtures/wire_constants.txt`, which `flz_core` holds to its own sources:
  the header's `sdk_lang` and `format_id` tables in `Flezha.State.Frame`, and
  the payload ids the stream and control channels dispatch on.
  """
  use ExUnit.Case, async: true

  alias Flezha.State.Frame

  @fixture Path.join([__DIR__, "..", "..", "fixtures", "wire_constants.txt"])
  @external_resource @fixture

  @channels ["lib/flezha/runner/stream_channel.ex", "lib/flezha/runner/control_channel.ex"]

  # Where a channel names a payload id: a dispatch clause, a guard, the write
  # path's list, and the id a control frame is written under.
  @dispatch [
    ~r/\{:flz_data, _stream_id, ("\w+")/,
    ~r/when payload_id (?:==|in) (.+) do/,
    ~r/@cell_write_payloads \[([^\]]+)\]/s,
    ~r/payload_id \\\\ ("\w+")/,
    ~r/write_control\(state, \w+, ("\w+")\)/
  ]

  defp rows(table) do
    for line <- File.stream!(@fixture),
        [^table, name, value] <- [line |> String.trim() |> String.split("|")],
        do: {name, value}
  end

  test "every sdk_lang row names the same language here" do
    for {name, value} <- rows("sdk_lang") do
      expected = if name in ["next_free", "reserved"], do: "unallocated", else: name
      assert Frame.sdk_lang_name(String.to_integer(value)) == expected, "sdk_lang|#{name}"
    end
  end

  test "every format_id row is the byte this table allocates" do
    for {name, value} <- rows("format_id") do
      assert Frame.format_id(String.to_atom(name)) == String.to_integer(value),
             "format_id|#{name}"
    end
  end

  test "the channels dispatch on exactly the fixture's payload ids" do
    dispatched =
      for path <- @channels,
          pattern <- @dispatch,
          [_, names] <- Regex.scan(pattern, File.read!(path)),
          [_, name] <- Regex.scan(~r/"(\w+)"/, names),
          into: MapSet.new(),
          do: name

    assert dispatched == MapSet.new(rows("payload_id"), &elem(&1, 1))
  end
end
