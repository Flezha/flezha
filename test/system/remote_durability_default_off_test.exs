defmodule FlezhaTest.System.RemoteDurabilityDefaultOffTest do
  @moduledoc """
  The default-off pin for `remote-durability-tier`.

  The tier's whole claim to being safe to land is that `backend: none` — the
  default — is byte-identical to a build without it. This runs a real
  effectively-once operative end to end against a live runner with the tier
  unconfigured and asserts that no tier process exists at any point, that no
  entry point does anything, and that the durability path behaves exactly as it
  did before.

  Tagged `:system`; run with
  `mix test --include system test/system/remote_durability_default_off_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :effectively_once

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, await_stream_bound: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Service.OperativeManager
  alias Flezha.State.DurabilityTier
  alias Flezha.State.InputLog

  @moduletag timeout: 300_000

  test "an effectively-once operative runs unchanged and no tier process ever starts" do
    # The suite's config never sets `:durability_tier`; assert that rather than
    # assume it, so a leaked override from another case cannot make this pass
    # vacuously.
    assert Application.get_env(:flezha, :durability_tier) == nil or
             DurabilityTier.enabled?() == false

    refute DurabilityTier.enabled?()
    assert DurabilityTier.children() == []
    assert GenServer.whereis(Flezha.State.DurabilityTier.Shipper) == nil

    namespace = unique("dur_off")
    operative_id = unique("tier_store")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port, exec} = boot_runner(namespace, id_path)

    sink = %Flezha.Plugin{
      id: "off_sink",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      subscriptions: [{:on_output, []}]
    }

    builder = %Builder{
      id: operative_id,
      factory_id: "tier_store",
      init_values: %{},
      opts: [plugins: [sink]]
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)

    assert await_stream_bound(namespace, operative_id), "the data stream never bound"

    assert info.runner_id == exec

    assert wait_until(fn -> ClassRegistry.effectively_once?(operative_id) end, 30_000)

    send_inputs(operative_id, namespace, ["k1:a", "k2:a"])
    assert await_changelog(%{"k1" => "k1=a", "k2" => "k2=a"}, 60_000)

    # Per-input acknowledgement, exactly as before the tier existed.
    assert wait_until(fn -> InputLog.watermark(operative_id) == 1 end, 60_000),
           "the durable consumed-offset did not advance as it does without the tier"

    # Still nothing started, and every tier entry point is still a no-op.
    assert GenServer.whereis(Flezha.State.DurabilityTier.Shipper) == nil
    assert DurabilityTier.status() == []
    assert DurabilityTier.breaches() == []
    refute DurabilityTier.sync_class?(operative_id)
    assert DurabilityTier.hydrate_operative(operative_id) == :ok
  end
end
