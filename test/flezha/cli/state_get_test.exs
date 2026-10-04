defmodule Flezha.Service.Cli.StateGetTest do
  @moduledoc """
  `flz state get <operator/tasklet[/cell_key]>` operations surface: the live
  read, and every refusal it can report. The SDK-side selector and its
  token-prefixed refusals are covered by `state_serialize.rs` and
  `OperatorStateSerializerTest`; here we pin how each verdict reaches
  the caller, and that the non-resident family points at the durable path.
  """
  # async: false: Mimic global mode; Mimic stubs.
  use ExUnit.Case, async: false

  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.Cli
  alias FlezhaTest.Helper.CliCapture

  setup :set_mimic_global_until_exit

  defp operator_id(), do: "sg_#{System.unique_integer([:positive])}"

  # `state_get` resolves the namespace holding the operator, then asks its
  # Overwatch (through OperatorManager) for the narrowed read.
  defp stub_read(operator, verdict) do
    stub(Flezha.Service.OperatorManager, :namespace_of, fn ^operator -> {:ok, "trading"} end)

    stub(Flezha.Service.OperatorManager, :get_cell_state, fn "trading", ^operator, _selector ->
      verdict
    end)
  end

  # A refusal exits `{:shutdown, 1}` (the CLI's non-zero exit) after printing on
  # stderr; a successful read prints on stdout and returns. Capture both and
  # swallow the exit, so every verdict stays assertable as one string.
  defp get(path, format \\ "text") do
    {_outcome, out, err} =
      CliCapture.run(fn -> Cli.call("Steps.state_get", Jason.encode!([path]), output: format) end)

    out <> err
  end

  test "a live Cell is printed with the seq it was read at" do
    operator = operator_id()
    stub_read(operator, {:ok, ~s({"candle":{"BTCUSD@1m":{"high":42}}}), 17})

    out = get("#{operator}/candle/BTCUSD@1m")

    assert out =~ "change_seq: 17"
    assert out =~ ~s({"candle":{"BTCUSD@1m":{"high":42}}})
  end

  test "the json format renders the state, the seq and the path together" do
    operator = operator_id()
    stub_read(operator, {:ok, ~s({"candle":{"BTCUSD@1m":{"high":42}}}), 17})

    out = get("#{operator}/candle/BTCUSD@1m", "json")
    decoded = Jason.decode!(out)

    assert decoded["change_seq"] == 17
    assert decoded["state"] == %{"candle" => %{"BTCUSD@1m" => %{"high" => 42}}}
    assert decoded["path"] == "#{operator}/candle/BTCUSD@1m"
  end

  test "a whole-Tasklet read needs no cell key and reports no single version" do
    operator = operator_id()
    stub_read(operator, {:ok, ~s({"candle":{"a":{},"b":{}}}), nil})

    out = get("#{operator}/candle")

    assert out =~ "change_seq: -"
  end

  test "an idle-evicted Cell is refused naming eviction and the durable path" do
    operator = operator_id()

    stub_read(
      operator,
      {:error, Flezha.Error.new("state.cell_evicted", tasklet: "candle", cell_key: "BTCUSD@1m")}
    )

    out = get("#{operator}/candle/BTCUSD@1m")

    assert out =~ "(state.cell_evicted)"
    assert out =~ "hint: flz state at <operator>/candle/BTCUSD@1m"
  end

  test "an unknown key is refused distinctly from an evicted one" do
    operator = operator_id()

    stub_read(
      operator,
      {:error, Flezha.Error.new("state.cell_not_found", tasklet: "candle", cell_key: "X")}
    )

    out = get("#{operator}/candle/X")

    assert out =~ "(state.cell_not_found)"
    assert out =~ "hint: flz state at <operator>/candle/X"
  end

  test "a suspended instance is refused naming suspension" do
    operator = operator_id()
    stub_read(operator, {:error, {:not_resident, :suspended, operator}})

    out = get("#{operator}/candle/BTCUSD@1m")

    assert out =~ "suspended"
    assert out =~ "flz state at"
  end

  test "an unbound Operator is refused rather than waited on" do
    operator = operator_id()
    stub_read(operator, {:error, {:not_resident, :unbound, operator}})

    out = get("#{operator}/candle/BTCUSD@1m")

    assert out =~ "is unbound"
    assert out =~ "(state.not_resident)"
    assert out =~ "hint: flz state at"
  end

  test "an operator running in no namespace is refused before any round-trip" do
    stub(Flezha.Service.OperatorManager, :namespace_of, fn _id -> :error end)

    out = get("ghost/candle/BTCUSD@1m")

    assert out =~ "no running operator 'ghost' in any namespace (state.no_operator)"
  end

  test "an oversized read is refused reporting the bytes and the narrowing selector" do
    operator = operator_id()
    limit = Application.get_env(:flezha, :wal_high_watermark_bytes, 16 * 1024 * 1024)
    fat = ~s({"candle":") <> String.duplicate("x", limit) <> ~s("})
    stub_read(operator, {:ok, fat, nil})

    out = get("#{operator}/candle")

    assert out =~ "(state.oversized_read)"
    assert out =~ "narrow the read"
    assert out =~ Integer.to_string(byte_size(fat))
  end

  test "a malformed path is refused naming the shape it expects" do
    out = get("just-an-operator")

    assert out =~ "write it as <operator>/<tasklet>[/<cell_key>] (state.bad_live_path)"
  end
end
