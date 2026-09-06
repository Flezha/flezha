defmodule Flezha.Service.CliManagerStateGetTest do
  @moduledoc """
  `flz state get <operative/procedure[/entity_key]>` operator surface: the live
  read, and every refusal it can report. The SDK-side selector and its
  token-prefixed refusals are covered by `state_serialize.rs` and
  `StreamOperativeStateSerializerTest`; here we pin how each verdict reaches
  the operator, and that the non-resident family points at the durable path.
  """
  # async: false: Mimic global mode; Mimic stubs.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Service.CliManager

  setup :set_mimic_global_until_exit

  defp operative_id(), do: "sg_#{System.unique_integer([:positive])}"

  # `state_get` resolves the namespace holding the operative, then asks its
  # Overwatch (through OperativeManager) for the narrowed read.
  defp stub_read(operative, verdict) do
    stub(Flezha.Service.OperativeManager, :namespace_of, fn ^operative -> {:ok, "trading"} end)

    stub(Flezha.Service.OperativeManager, :get_entity_state, fn "trading",
                                                                ^operative,
                                                                _selector ->
      verdict
    end)
  end

  # A refusal exits `{:shutdown, 1}` (the CLI's non-zero exit) after printing on
  # stderr; a successful read prints on stdout and returns. Capture both and
  # swallow the exit, so every verdict stays assertable as one string.
  defp get(path, format \\ "text") do
    stderr =
      capture_io(:stderr, fn ->
        printed =
          capture_io(fn ->
            try do
              CliManager.state_get(path, format)
            catch
              :exit, _ -> :ok
            end
          end)

        send(self(), {:stdout, printed})
      end)

    receive do
      {:stdout, printed} -> printed <> stderr
    end
  end

  test "a live Entity is printed with the seq it was read at" do
    operative = operative_id()
    stub_read(operative, {:ok, ~s({"candle":{"BTCUSD@1m":{"high":42}}}), 17})

    out = get("#{operative}/candle/BTCUSD@1m")

    assert out =~ "change_seq: 17"
    assert out =~ ~s({"candle":{"BTCUSD@1m":{"high":42}}})
  end

  test "the json format renders the state, the seq and the path together" do
    operative = operative_id()
    stub_read(operative, {:ok, ~s({"candle":{"BTCUSD@1m":{"high":42}}}), 17})

    out = get("#{operative}/candle/BTCUSD@1m", "json")
    decoded = Jason.decode!(out)

    assert decoded["change_seq"] == 17
    assert decoded["state"] == %{"candle" => %{"BTCUSD@1m" => %{"high" => 42}}}
    assert decoded["path"] == "#{operative}/candle/BTCUSD@1m"
  end

  test "a whole-Procedure read needs no entity key and reports no single version" do
    operative = operative_id()
    stub_read(operative, {:ok, ~s({"candle":{"a":{},"b":{}}}), nil})

    out = get("#{operative}/candle")

    assert out =~ "change_seq: -"
  end

  test "an idle-evicted Entity is refused naming eviction and the durable path" do
    operative = operative_id()

    stub_read(
      operative,
      {:error,
       "entity_evicted: Entity `BTCUSD@1m` of procedure `candle` was idle-evicted; " <>
         "eviction is destructive, so there is no durable value to serve"}
    )

    out = get("#{operative}/candle/BTCUSD@1m")

    assert out =~ "entity_evicted"
    assert out =~ "flz state at"
  end

  test "an unknown key is refused distinctly from an evicted one" do
    operative = operative_id()

    stub_read(
      operative,
      {:error, "entity_not_found: procedure `candle` holds no Entity keyed `X`"}
    )

    out = get("#{operative}/candle/X")

    assert out =~ "entity_not_found"
    refute out =~ "entity_evicted"
    assert out =~ "flz state at"
  end

  test "a suspended instance is refused naming suspension" do
    operative = operative_id()
    stub_read(operative, {:error, {:not_resident, :suspended}})

    out = get("#{operative}/candle/BTCUSD@1m")

    assert out =~ "suspended"
    assert out =~ "flz state at"
  end

  test "an unbound Operative is refused rather than waited on" do
    operative = operative_id()
    stub_read(operative, {:error, {:not_resident, :unbound}})

    out = get("#{operative}/candle/BTCUSD@1m")

    assert out =~ "not bound to a runner"
    assert out =~ "flz state at"
  end

  test "an operative running in no namespace is refused before any round-trip" do
    stub(Flezha.Service.OperativeManager, :namespace_of, fn _id -> :error end)

    out = get("ghost/candle/BTCUSD@1m")

    assert out =~ "no running operative `ghost`"
  end

  test "an oversized read is refused reporting the bytes and the narrowing selector" do
    operative = operative_id()
    limit = Application.get_env(:flezha, :wal_high_watermark_bytes, 16 * 1024 * 1024)
    fat = ~s({"candle":") <> String.duplicate("x", limit) <> ~s("})
    stub_read(operative, {:ok, fat, nil})

    out = get("#{operative}/candle")

    assert out =~ "over the :wal_high_watermark_bytes"
    assert out =~ "narrow the read"
    assert out =~ Integer.to_string(byte_size(fat))
  end

  test "a malformed path is refused naming the shape it expects" do
    out = get("just-an-operative")

    assert out =~ "expected <operative>/<procedure>[/<entity_key>]"
  end
end
