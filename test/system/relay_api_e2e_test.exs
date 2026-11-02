defmodule FlezhaTest.System.RelayApiE2ETest do
  @moduledoc """
  `relay_api`: a runner announces its factories' APIs, the controller lists
  them through `Relay.api` before any instance runs, and `Relay.call_channel`
  calls a channel by value. The test app's `orders` factory declares `place`
  (JSON, described, answering through `placed`) and `audit` (text, no answer).
  The app's own code then calls `place` through its runner's `call`, from the
  local-send drill (`FLEZHA_LOCAL_SEND_SCRIPT`, an entry with `call: true`).

  Tagged `:system` (boots real runner subprocesses); run with
  `mix test --include system test/system/relay_api_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :relay_api

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 3, cleanup: 3, unique: 1, unique_id_path: 0]

  alias Flezha.Relay.Builder
  alias Flezha.Service.{Admin, RelayManager}
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 180_000

  @order %{"pair" => "EUR/USD", "amount" => 1000}
  @placed %{"pair" => "EUR/USD", "amount" => 1000, "status" => "placed"}

  test "the API is listed before any instance runs, and each channel is called by value" do
    namespace = unique("relay_api")
    relay_id = unique("orders")
    id_path = unique_id_path()
    go = trigger_path()
    on_exit(fn -> cleanup(namespace, relay_id, id_path) end)

    {port, _runner} = boot_runner(namespace, id_path, env: drill(relay_id, go))

    assert {:ok, factories} = Admin.run("Relay.api", [namespace], context(namespace), :given)
    assert %{channels: channels} = Enum.find(factories, &(&1.factory == "orders"))

    assert %{codec: "json", answer_codec: "json", description: "Places an order"} =
             Enum.find(channels, &(&1.channel == "place"))

    assert %{codec: "text", answer_codec: nil, description: nil} =
             Enum.find(channels, &(&1.channel == "audit"))

    {:ok, _started} =
      RelayManager.start_relay(namespace, %Builder{
        id: relay_id,
        factory_id: "orders",
        init_values: %{}
      })

    assert {:ok, %{answer: answer, answer_codec: "json"}} =
             call(namespace, relay_id, "place", @order)

    assert answer == @placed

    assert {:ok, %{accepted: true}} = call(namespace, relay_id, "audit", "checked")

    assert {:error, %Flezha.Error{code: "api.channel_unknown"} = unknown} =
             call(namespace, relay_id, "plce", @order)

    assert unknown.details["channels"] =~ "place"

    File.touch!(go)
    assert {:ok, ^answer} = Jason.decode(drill_answer(port))
  end

  defp call(namespace, relay_id, channel, value),
    do:
      Admin.run(
        "Relay.call_channel",
        [namespace, relay_id, channel, value, %{}],
        context(namespace),
        :given
      )

  defp context(namespace),
    do: %{source: :http, principal: "console@_system", namespace: namespace}

  defp drill(relay_id, go) do
    script = [%{relay: relay_id, channel: "place", value: @order, when: go, call: true}]
    [{"FLEZHA_LOCAL_SEND_SCRIPT", Jason.encode!(script)}]
  end

  defp trigger_path do
    dir = NodeTestHelper.claim_tmp_dir("flz_relay_api_go")
    on_exit(fn -> File.rm_rf(dir) end)
    Path.join(dir, "go")
  end

  # The drill's answer line: `call <relay> ok <answer JSON>`.
  defp drill_answer(port) do
    seen = NodeTestHelper.read_port(port, &(&1 =~ ~r/^call \S+ .+$/m), 60_000, "")
    [_, answer] = Regex.run(~r/^call \S+ ok (.+)$/m, seen) || [nil, "none in:\n#{seen}"]
    String.trim(answer)
  end
end
