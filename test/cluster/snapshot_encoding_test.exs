defmodule Flezha.Cluster.Snapshot.EncodingTest do
  use ExUnit.Case, async: true

  alias Flezha.Cluster.Snapshot.Encoding
  alias Flezha.Plugin
  alias Flezha.Processor.Builder

  describe "encode/1 + decode/1 round-trip" do
    test "two processors and three plugins survive encode/decode" do
      payload = %{
        taken_at_ms: 1_700_000_000_000,
        cluster: %{nodes: [:"a@127.0.0.1", :"b@127.0.0.1"]},
        processors: [
          %{
            processor: %Builder{id: "p1", factory_id: "f1", init_values: %{}, opts: []},
            namespace: "default",
            last_observed_node: :"a@127.0.0.1"
          },
          %{
            processor: %Builder{id: "p2", factory_id: "f2", init_values: %{x: 1}, opts: []},
            namespace: "default",
            last_observed_node: :"b@127.0.0.1"
          }
        ],
        plugins: [
          %{
            plugin: %Plugin{id: "k1", provider: Flezha.Plugin.Provider.Kafka.Producer},
            last_observed_node: :"a@127.0.0.1"
          },
          %{
            plugin: %Plugin{id: "k2", provider: Flezha.Plugin.Provider.Websocket},
            last_observed_node: :"a@127.0.0.1"
          },
          %{
            plugin: %Plugin{id: "k3", provider: Flezha.Plugin.Provider.StdOut},
            last_observed_node: :"b@127.0.0.1"
          }
        ],
        captured_states: %{"p1" => <<1, 2, 3>>, "p2" => <<4, 5>>},
        strict_mode: false,
        initiator_node: :"a@127.0.0.1"
      }

      bin = Encoding.encode(payload)
      assert is_binary(bin)
      assert {:ok, decoded} = Encoding.decode(bin)
      assert decoded == payload
    end
  end

  describe "decode/1 errors" do
    test "future-version envelope returns unsupported_version" do
      bin = :erlang.term_to_binary({:flezha_snapshot, 99, %{}}, [:compressed])
      assert {:error, {:unsupported_version, 99, [1]}} = Encoding.decode(bin)
    end

    test "non-tagged term returns invalid_envelope" do
      bin = :erlang.term_to_binary(%{not: :a_snapshot})
      assert {:error, :invalid_envelope} = Encoding.decode(bin)
    end

    test "garbage bytes return decode_failed" do
      assert {:error, {:decode_failed, _}} = Encoding.decode(<<255, 254, 253>>)
    end
  end
end
