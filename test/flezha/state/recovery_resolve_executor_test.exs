defmodule Flezha.State.RecoveryResolveExecutorTest do
  @moduledoc """
  §16.3 — `Recovery.resolve_executor/1` must match the *real* durable-registry
  entry shape `{relay_id, {cspec, node_pids, metadata}}` — executor =
  `metadata.tag`, relay_class = the relay's `:relay_class` opt (default: its id),
  carried in the cspec's start args — mirroring `Reconciler.Probe.relay_from_cspec/1`.

  The prior pattern expected a `%{metadata: %{relay: %{relay_class: ...}}}` map
  the registry never produces, so every Watchdog output-stall / compaction
  resync silently skipped with "no active executor owns relay_class=...". This
  is the resync that backstops the §16.2 WAL realignment for genuine wedges, so
  it must actually resolve the executor.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Relay.Builder
  alias Flezha.Service.HubManager
  alias Flezha.State.Recovery

  defp cspec(relay) do
    %{start: {Flezha.Relay.Overwatch, :start_link, [{"default", relay}]}}
  end

  defp entry(relay, tag) do
    {cspec(relay), [{node(), self()}], %{tag: tag, health: :healthy}}
  end

  test "resolves the executor tag for a relay whose :relay_class opt matches" do
    relay = %Builder{id: "btc_kraken", factory_id: "f", opts: [relay_class: "market"]}

    stub(HubManager, :get_registry, fn "default" ->
      {:ok, %{"btc_kraken" => entry(relay, "exec_123")}}
    end)

    assert Recovery.resolve_executor({"market", "0", "p1"}) == {:ok, "exec_123"}
  end

  test "falls back to the relay id when no :relay_class opt is set" do
    relay = %Builder{id: "market", factory_id: "f", opts: []}

    stub(HubManager, :get_registry, fn "default" ->
      {:ok, %{"market" => entry(relay, "exec_9")}}
    end)

    assert Recovery.resolve_executor({"market", "0", "p1"}) == {:ok, "exec_9"}
  end

  test "errors when no registered relay owns the relay_class" do
    relay = %Builder{id: "eth", factory_id: "f", opts: [relay_class: "other"]}
    stub(HubManager, :get_registry, fn "default" -> {:ok, %{"eth" => entry(relay, "exec_1")}} end)

    assert {:error, msg} = Recovery.resolve_executor({"market", "0", "p1"})
    assert msg =~ "no active executor owns relay_class=market"
  end

  test "tolerates a {cspec, node_pids} 2-tuple row with no metadata (no crash)" do
    relay = %Builder{id: "market", factory_id: "f", opts: []}
    spec = cspec(relay)

    stub(HubManager, :get_registry, fn "default" ->
      {:ok, %{"market" => {spec, [{node(), self()}]}}}
    end)

    # No metadata → no tag → not a match, and the 2-tuple shape must not crash.
    assert {:error, _} = Recovery.resolve_executor({"market", "0", "p1"})
  end

  test "surfaces a registry error" do
    stub(HubManager, :get_registry, fn "default" -> {:error, "hub not alive"} end)

    assert Recovery.resolve_executor({"market", "0", "p1"}) == {:error, "hub not alive"}
  end
end
