defmodule Flezha.Observability.Export.OtlpCodecTest do
  # async: true: builds a %Signal{} and encodes it — no process, no application env,
  # no named table, so the codec is a pure function of its argument.
  use ExUnit.Case, async: true

  alias Flezha.Observability.Export.OtlpCodec
  alias Flezha.Observability.Signal

  @moduletag :observability_export

  describe "OtlpCodec" do
    test "encode_metrics produces a parseable protobuf body" do
      signal =
        Signal.new(%{
          kind: :metric,
          source: :bench,
          name: :requests,
          measurements: %{count: 5},
          metadata: %{path: "/x"}
        })

      body = OtlpCodec.encode_metrics([signal])
      bytes = IO.iodata_to_binary(body)
      assert byte_size(bytes) > 0
      # Field 1 wire-type 2 -> tag byte 0x0a
      assert <<10, _len::8, _rest::binary>> = bytes
    end

    test "encode_batch routes signal kinds to the right service request" do
      signals = [
        Signal.new(%{kind: :metric, source: :s, name: :m, measurements: %{count: 1}}),
        Signal.new(%{kind: :log, source: :s, name: :l, measurements: %{}}),
        Signal.new(%{kind: :event, source: :s, name: :e, measurements: %{count: 1}})
      ]

      %{metrics: m, logs: l, traces: t} = OtlpCodec.encode_batch(signals)
      assert m != nil
      # Both :log and :event end up in logs
      assert l != nil
      # No :trace_span signal so traces is nil
      assert t == nil
    end
  end
end
