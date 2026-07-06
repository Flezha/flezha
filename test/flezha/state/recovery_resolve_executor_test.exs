defmodule Flezha.State.RecoveryResolveExecutorTest do
  @moduledoc """
  §16.3 — `Recovery.resolve_executor/1` must match the *real* durable-registry
  entry shape `{operative_id, {cspec, node_pids, metadata}}` — executor =
  `metadata.tag`, operative_class = the operative's `:operative_class` opt (default: its id),
  carried in the cspec's start args — mirroring `Reconciler.Probe.operative_from_cspec/1`.

  The prior pattern expected a `%{metadata: %{operative: %{operative_class: ...}}}` map
  the registry never produces, so every Watchdog output-stall / compaction
  resync silently skipped with "no active executor owns operative_class=...". This
  is the resync that backstops the §16.2 WAL realignment for genuine wedges, so
  it must actually resolve the executor.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Operative.Builder
  alias Flezha.Service.HubManager
  alias Flezha.State.Recovery

  defp cspec(operative) do
    %{start: {Flezha.Operative.Overwatch, :start_link, [{"default", operative}]}}
  end

  defp entry(operative, tag) do
    {cspec(operative), [{node(), self()}], %{tag: tag, health: :healthy}}
  end

  test "resolves the executor tag for a operative whose :operative_class opt matches" do
    operative = %Builder{id: "btc_kraken", factory_id: "f", opts: [operative_class: "market"]}

    stub(HubManager, :get_registry, fn "default" ->
      {:ok, %{"btc_kraken" => entry(operative, "exec_123")}}
    end)

    assert Recovery.resolve_executor({"market", "0", "p1"}) == {:ok, "exec_123"}
  end

  test "falls back to the operative id when no :operative_class opt is set" do
    operative = %Builder{id: "market", factory_id: "f", opts: []}

    stub(HubManager, :get_registry, fn "default" ->
      {:ok, %{"market" => entry(operative, "exec_9")}}
    end)

    assert Recovery.resolve_executor({"market", "0", "p1"}) == {:ok, "exec_9"}
  end

  test "errors when no registered operative owns the operative_class" do
    operative = %Builder{id: "eth", factory_id: "f", opts: [operative_class: "other"]}

    stub(HubManager, :get_registry, fn "default" ->
      {:ok, %{"eth" => entry(operative, "exec_1")}}
    end)

    assert {:error, msg} = Recovery.resolve_executor({"market", "0", "p1"})
    assert msg =~ "no active executor owns operative_class=market"
  end

  test "tolerates a {cspec, node_pids} 2-tuple row with no metadata (no crash)" do
    operative = %Builder{id: "market", factory_id: "f", opts: []}
    spec = cspec(operative)

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
