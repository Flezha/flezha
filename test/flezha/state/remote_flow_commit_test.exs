defmodule Flezha.State.RemoteFlowCommitTest do
  @moduledoc """
  `durable: remote` on a flow class: a commit point is acknowledged only once
  the remote copy holds it. The acknowledgement a writer owes is what its
  fsync releases: the dispatch of an input's outputs, which advances the
  input's consumed offset, and the credit it drained.

  The test process is the relay's Overwatch (`FlezhaTest.OutboxCase`), so a
  dispatched output is a message here; the backend is the in-memory stub,
  holding each manifest write until the case lets it land.
  """
  use FlezhaTest.OutboxCase, async: false

  import ExUnit.CaptureIO

  alias Flezha.Runner.StreamChannel.Prefix
  alias Flezha.Service.Cli
  alias Flezha.State.DurabilityTier
  alias Flezha.State.DurabilityTier.{Manifest, Shipper}
  alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub
  alias FlezhaTest.Helper.TierE2EHarness

  setup %{rc: rc} do
    Stub.install()
    on_exit(&Stub.uninstall/0)
    start_supervised!(Shipper)
    :ok = Shipper.sweep_now()
    :ok = ClassRegistry.put_remote(rc, true)
    :ok
  end

  test "an input is acknowledged only after the manifest naming its record is written", ctx do
    Stub.hold_manifests(self())
    pid = armed_writer(ctx.rc, ctx.cell_id)

    append_effect(pid, 0, [output("k1", "v0")], 0)
    shipper = held({:cell, ctx.cell_id})

    # The record is durable on this disk and its ship is mid-write: nothing is
    # acknowledged yet.
    refute_dispatched(0)
    assert InputLog.watermark(ctx.rc) == nil

    Stub.release_manifest(shipper)

    assert {"v0", _, _} = next_output(5_000)
    assert wait_until(fn -> InputLog.watermark(ctx.rc) == 0 end, 5_000)
    assert Manifest.shipped_through(Stub.stored_manifest({:cell, ctx.cell_id})) >= 0
  end

  test "a checkpoint interval ships once, at its close", ctx do
    :ok = ClassRegistry.put_ack_checkpoint(ctx.rc, true)
    Stub.hold_manifests(self())
    pid = armed_writer(ctx.rc, ctx.cell_id)

    # Inside the interval: no input is acknowledged, so nothing waits on a ship.
    append_effect(pid, 0, [output("k1", "a")], nil)
    assert {"a", _, _} = next_output(5_000)
    refute_received {:manifest_held, _, _}
    refute Manifest.key({:cell, ctx.cell_id}) in Stub.keys()

    # The close acknowledges the interval's inputs: it waits on one ship.
    append_effect(pid, 1, [output("k1", "b")], 1)
    shipper = held({:cell, ctx.cell_id})
    refute_dispatched(0)
    Stub.release_manifest(shipper)
    assert {"b", _, _} = next_output(5_000)

    append_effect(pid, 2, [output("k1", "c")], nil)
    assert {"c", _, _} = next_output(5_000)
    refute_received {:manifest_held, _, _}

    append_effect(pid, 3, [], 3)
    Stub.release_manifest(held({:cell, ctx.cell_id}))
    assert wait_until(fn -> InputLog.watermark(ctx.rc) == 3 end, 5_000)
    refute_received {:manifest_held, _, _}
  end

  test "a controller restored from an empty disk resumes after the acknowledged input", ctx do
    for n <- 0..1, do: {:ok, ^n} = InputLog.append(ctx.rc, "in#{n}", %{})
    pid = armed_writer(ctx.rc, ctx.cell_id)

    append_effect(pid, 0, [output("k1", "v0")], 0)
    assert {"v0", _, _} = next_output(5_000)

    lose_the_disk(ctx)
    assert :ok = DurabilityTier.hydrate_relay(ctx.rc)

    assert InputLog.watermark(ctx.rc) == 0
    assert [{1, "in1", _}] = InputLog.undelivered(ctx.rc)
  end

  test "a restore replays the inputs of the interval left open", ctx do
    :ok = ClassRegistry.put_ack_checkpoint(ctx.rc, true)
    for n <- 0..2, do: {:ok, ^n} = InputLog.append(ctx.rc, "in#{n}", %{})
    pid = armed_writer(ctx.rc, ctx.cell_id)

    append_effect(pid, 0, [output("k1", "closed")], 1)
    assert {"closed", _, _} = next_output(5_000)
    append_effect(pid, 1, [output("k1", "open")], nil)
    assert {"open", _, _} = next_output(5_000)

    lose_the_disk(ctx)
    assert :ok = DurabilityTier.hydrate_relay(ctx.rc)

    assert InputLog.watermark(ctx.rc) == 1
    assert [{2, "in2", _}] = InputLog.undelivered(ctx.rc)
  end

  test "a backend outage stalls a remote class while a local one acknowledges", ctx do
    local_rc = ctx.rc <> "_local"
    local_cell = {local_rc, "task", "k1"}
    on_exit(fn -> with {:ok, w} <- Supervisor.lookup(local_cell), do: WalWriter.close(w) end)

    Application.put_env(
      :flezha,
      :durability_tier,
      Application.get_env(:flezha, :durability_tier) |> Keyword.put(:rpo_target_ms, 1)
    )

    Stub.set_down(true)
    remote = armed_writer(ctx.rc, ctx.cell_id)
    local = armed_writer(local_rc, local_cell)

    append_effect(remote, 0, [output("k1", "remote")], 0)
    append_effect(local, 0, [output("k1", "local")], 0)

    assert {"local", _, _} = next_output(5_000)
    assert wait_until(fn -> InputLog.watermark(local_rc) == 0 end, 5_000)

    # The remote class's ship has failed at least once, so its gate has
    # answered: it holds the acknowledgement, and the RPO alarm names it.
    ref = {:cell, ctx.cell_id}

    assert wait_until(fn -> Enum.any?(DurabilityTier.breaches(), &(&1.ref == ref)) end, 5_000)
    assert wait_until(fn -> failures(ref) > 0 end, 5_000)
    refute_dispatched(0)
    assert InputLog.watermark(ctx.rc) == nil

    Stub.set_down(false)
    assert {"remote", _, _} = next_output(10_000)
    assert wait_until(fn -> InputLog.watermark(ctx.rc) == 0 end, 5_000)
  end

  test "the controller status shows each running class's level beside the RPO", ctx do
    steps = ctx.rc <> "_steps"
    on_exit(fn -> Enum.each([ctx.rc, steps], &ClassRegistry.delete/1) end)
    :ok = ClassRegistry.put(steps, :steps)

    for class <- [ctx.rc, steps], do: ClassRegistry.put_overwatch(class, self(), nil)

    # The stub is a module, not a name the status can print; the levels are
    # the class's own declaration and do not read the backend.
    Application.put_env(:flezha, :durability_tier, backend: :none)

    printed = capture_io(fn -> :ok = Cli.call("Controller.status", "[]", []) end)
    assert printed =~ ~r/durability_rpo_breaches: +0\ndurability_levels: +.*\b#{ctx.rc}=remote\b/
    assert printed =~ "#{steps}=local"

    json = capture_io(fn -> :ok = Cli.call("Controller.status", "[]", output: "json") end)
    assert %{^steps => "local"} = levels = Jason.decode!(json)["durability_levels"]
    assert levels[ctx.rc] == "remote"
  end

  describe "the credit a held batch owes its stream" do
    test "a writer that goes and stays gone keeps the marker waiting until its Cell is deleted",
         ctx do
      Stub.hold_manifests(self())
      {[], prefix} = Prefix.fenced(Prefix.new())
      pid = armed_writer(ctx.rc, ctx.cell_id)

      prefix = Prefix.received(prefix, ctx.cell_id, 10)
      append_credited(pid, 0, 10)
      _shipper = held({:cell, ctx.cell_id})

      # Record 0 stays on this disk alone: the stream still counts it owed.
      close_writer(pid, ctx.cell_id)
      assert {[], prefix} = stream(prefix)
      assert {[], prefix} = Prefix.marker(prefix, ctx.rc, 0)

      # Deleting the Cell takes record 0 off the disk: its credit is released,
      # and the marker that covered it drops unconfirmed.
      {:ok, _} = Supervisor.delete_cell(ctx.cell_id)
      assert {[], prefix} = stream(prefix)
      assert {[{_, 1, 1}], _} = Prefix.marker(prefix, ctx.rc, 1)
    end

    test "a relay that leaves drains what its writers left owed once one ship lands", ctx do
      Stub.hold_manifests(self())
      pid = armed_writer(ctx.rc, ctx.cell_id)

      append_credited(pid, 0, 10)
      first = held({:cell, ctx.cell_id})

      :ok = Supervisor.close_writers(ctx.rc)
      refute_received {:credit_released, _, _, _}

      Stub.release_manifest(first)
      assert_receive {:credit_drained, "s", 10, _}, 5_000
    end

    test "a relay that leaves while the backend refuses releases what it left owed", ctx do
      Stub.set_down(true)
      pid = armed_writer(ctx.rc, ctx.cell_id)

      append_credited(pid, 0, 10)
      assert wait_until(fn -> failures({:cell, ctx.cell_id}) > 0 end, 5_000)

      :ok = Supervisor.close_writers(ctx.rc)
      assert_receive {:credit_released, "s", 10, _}, 5_000
      refute_received {:credit_drained, _, _, _}
    end

    test "a realign releases what the discarded lineage held", ctx do
      Stub.hold_manifests(self())
      pid = armed_writer(ctx.rc, ctx.cell_id)

      :ok = WalWriter.append_async(pid, 0, effect_header(), effect_body([], 0), "r1", credit(10))
      :ok = WalWriter.fsync(pid)
      _shipper = held({:cell, ctx.cell_id})

      # A restarted runner's diverged seq discards record 0's lineage.
      :ok = WalWriter.append_async(pid, 5, effect_header(), effect_body([], 5), "r2", credit(6))
      _ = WalWriter.last_persisted_seq(pid)

      assert_received {:credit_released, "s", 10, _}
      refute_received {:credit_drained, _, _, _}
    end

    test "a rewind frees what it discards and keeps holding what survives", ctx do
      Stub.hold_manifests(self())
      pid = armed_writer(ctx.rc, ctx.cell_id)

      append_credited(pid, 0, 10)
      first = held({:cell, ctx.cell_id})
      append_credited(pid, 1, 6)

      :ok = WalWriter.rewind_to(pid, 0, 1)

      assert_received {:credit_released, "s", 6, _}
      refute_received {:credit_drained, _, _, _}

      # Record 0 survives on disk: its drain waits for a ship that covers it.
      Stub.release_manifest(first)
      Stub.release_manifest(held({:cell, ctx.cell_id}))
      assert_receive {:credit_drained, "s", 10, _}, 5_000
      refute_received {:credit_released, _, 10, _}
    end

    test "a record dropped unwritten drains behind the held batches, not before them", ctx do
      Stub.hold_manifests(self())
      pid = armed_writer(ctx.rc, ctx.cell_id)

      append_credited(pid, 0, 10)
      shipper = held({:cell, ctx.cell_id})

      # A gap: refused unwritten, its bytes owed back at once without the gate.
      :ok = WalWriter.append_async(pid, 5, effect_header(), effect_body([], 5), nil, credit(7))
      _ = WalWriter.last_persisted_seq(pid)
      refute_received {:credit_drained, _, _, _}

      # Both drain once the ship lands, held record first; whether they share a
      # message depends on how the fsyncs batched them.
      Stub.release_manifest(shipper)
      assert drained_bytes(17) == 17
    end

    test "a marker after a writer restart waits for the boot ship of what it left owed", ctx do
      Stub.hold_manifests(self())
      ref = {:cell, ctx.cell_id}
      {[], prefix} = Prefix.fenced(Prefix.new())
      pid = armed_writer(ctx.rc, ctx.cell_id)

      # Input 0's record is fsynced here; its ship is mid-write.
      prefix = Prefix.received(prefix, ctx.cell_id, 10)
      append_credited(pid, 0, 10)
      first = held(ref)
      assert {[], prefix} = Prefix.marker(prefix, ctx.rc, 0)

      close_writer(pid, ctx.cell_id)
      assert {[], prefix} = stream(prefix)

      # Restarted under the same stream, the writer ships record 0 at boot.
      restarted = start_writer(ctx.cell_id)
      _ = WalWriter.last_persisted_seq(restarted)
      refute Manifest.key(ref) in Stub.keys()

      # The runner's next marker covers input 0, still on this disk alone.
      assert {[], prefix} = Prefix.marker(prefix, ctx.rc, 1)
      assert {[], prefix} = stream(prefix)

      # The ship that was mid-write lands, so the boot ship, queued behind it,
      # finds record 0 shipped: its drain confirms both markers.
      Stub.release_manifest(first)
      assert {[{_, 1, 1}], _} = stream(prefix, 5_000)
    end
  end

  test "a rewind dispatches no kept record before its ship lands", ctx do
    Stub.hold_manifests(self())
    pid = armed_writer(ctx.rc, ctx.cell_id)

    append_effect(pid, 0, [output("k1", "kept")], 0)
    first = held({:cell, ctx.cell_id})
    :ok = WalWriter.rewind_to(pid, 0, 1)

    # Any caller of the outbox's dispatch, here its retry tick.
    send(pid, :outbox_retry)
    _ = WalWriter.last_persisted_seq(pid)
    refute_dispatched(0)
    assert InputLog.watermark(ctx.rc) == nil

    Stub.release_manifest(first)
    Stub.release_manifest(held({:cell, ctx.cell_id}))
    assert {"kept", _, _} = next_output(5_000)
  end

  test "a gated ship refused as superseded keeps the RPO alarm running", ctx do
    Application.put_env(
      :flezha,
      :durability_tier,
      Application.get_env(:flezha, :durability_tier) |> Keyword.put(:rpo_target_ms, 1)
    )

    # Another controller took the Cell's lineage, and this one never adopted it.
    ref = {:cell, ctx.cell_id}
    peer = %{Manifest.new(ref) | owner: "peer", claim: 1}
    :ok = Stub.put_if(Manifest.key(ref), Manifest.encode(peer), {:claim, 1}, [])

    pid = armed_writer(ctx.rc, ctx.cell_id)
    append_effect(pid, 0, [output("k1", "v0")], 0)

    assert wait_until(fn -> match?(%{superseded_by: "peer"}, status(ref)) end, 5_000)
    assert wait_until(fn -> Enum.any?(DurabilityTier.breaches(), &(&1.ref == ref)) end, 5_000)
    refute_dispatched(0)

    # Once nothing waits, a lineage another controller ships is not this
    # node's breach.
    close_writer(pid, ctx.cell_id)
    assert wait_until(fn -> not Enum.any?(DurabilityTier.breaches(), &(&1.ref == ref)) end, 5_000)
  end

  defp credit(bytes), do: %{channel: self(), session: "s", wire_size: bytes}

  # The test process is the writer's stream channel: the credit messages the
  # writer sent are stepped into the channel's `Prefix`, as the channel steps
  # them. `wait` bounds the wait for the first one.
  defp stream(prefix, wait \\ 0) do
    receive do
      {tag, "s", bytes, cell_id} when tag in [:credit_drained, :credit_released] ->
        step = if tag == :credit_drained, do: &Prefix.drained/3, else: &Prefix.released/3
        {confirmed, prefix} = step.(prefix, cell_id, bytes)
        {more, prefix} = stream(prefix)
        {confirmed ++ more, prefix}
    after
      wait -> {[], prefix}
    end
  end

  defp drained_bytes(want, got \\ 0)
  defp drained_bytes(want, got) when got >= want, do: got

  defp drained_bytes(want, got) do
    assert_receive {:credit_drained, "s", bytes, _}, 5_000
    drained_bytes(want, got + bytes)
  end

  defp append_credited(pid, seq, bytes) do
    :ok =
      WalWriter.append_async(pid, seq, effect_header(), effect_body([], seq), nil, credit(bytes))

    :ok = WalWriter.fsync(pid)
  end

  defp status(ref), do: Enum.find(Shipper.status(), &(&1.ref == ref))

  # The disk dies with the controller: the Cell, the input log and the floor
  # this node held in memory.
  defp lose_the_disk(ctx) do
    TierE2EHarness.destroy_local_state(ctx.rc)
    assert InputLog.watermark(ctx.rc) == nil
  end

  # The write held for `ref`'s manifest, answered with the shipper holding it.
  defp held(ref) do
    key = Manifest.key(ref)
    assert_receive {:manifest_held, ^key, shipper}, 5_000
    shipper
  end

  defp failures(ref) do
    case status(ref) do
      %{failures: n} -> n
      nil -> 0
    end
  end
end
