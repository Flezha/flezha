defmodule FlezhaTest.System.SchemaEvolutionE2ETest do
  @moduledoc """
  End-to-end proof that a Procedure's persisted state survives a change to the
  Procedure's own `State` / `Change` types — the deploy every stateful service
  eventually makes.

  The two halves of the deploy are two factories declaring the SAME procedure
  name (`evolving_cart`), so an operative started under `schema_evolution_v1`
  and restarted under `schema_evolution_v2` restores from the very WAL v1 wrote.
  One runner binary carries both, which is the only difference from a real
  deploy — the bytes crossing the restore are identical.

  Two directions, because only one of them is a migration:

    * **forward** — v2 declares `MIN_READABLE_SCHEMA_VERSION = 1` and a
      migration, so the accumulated total comes back and the field v2 added
      appears. A cold start would read 0, which is exactly the silent loss this
      change closes.
    * **backward** — v1 code meeting v2 state is a downgrade, not an
      evolution: it is refused by name rather than decoded into plausible
      nonsense.

  Tagged `:system` — boots a real runner subprocess.
  """

  use FlezhaTest.SystemCase, capability: :schema_evolution

  alias Flezha.Operative.Builder
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.TestHelpers
  alias Flezha.State.Watchdog
  alias FlezhaTest.Helper.CompactionAssertions, as: CA
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @procedure "evolving_cart"
  @v1 "schema_evolution_v1"
  @v2 "schema_evolution_v2"

  setup do
    # One namespace per scenario: a runner from a finished scenario is still
    # registered while the next one starts, and placement would hand the new
    # operative to a port that is on its way out.
    namespace = "schema_evolution_e2e_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    prev_age = Application.get_env(:flezha, :compact_after_age_ms)
    on_exit(fn -> Application.put_env(:flezha, :compact_after_age_ms, prev_age) end)

    {:ok, namespace: namespace}
  end

  @tag :schema_evolution_e2e
  test "state written at v1 comes back through the migration a bumped v2 declares", %{
    namespace: namespace
  } do
    {port, _runner_id} = NodeTestHelper.start_runner_port(namespace)
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "evo_#{System.unique_integer([:positive])}"

    parent = self()
    handler_id = {__MODULE__, :migration, System.unique_integer([:positive])}

    :telemetry.attach(
      handler_id,
      [:flezha, :state, :schema_migration],
      fn _e, _meas, meta, _ -> send(parent, {:migrated, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    # v1 accumulates 3 + 4 = 7 and persists it.
    assert {:ok, _} = start(namespace, operative_id, @v1)
    send(namespace, operative_id, "3")
    send(namespace, operative_id, "4")

    assert NodeTestHelper.wait_until(fn -> total(namespace, operative_id) == 7 end, 10_000),
           "precondition: v1 must accumulate its state before the deploy; read " <>
             inspect(state_of(namespace, operative_id))

    # Compact first: the snapshot is what carries the *state* across the
    # deploy, and a long-lived Entity always has one. Without it the restore
    # would exercise only the change-log half of the migration.
    entity_id = {operative_id, @procedure, ""}
    Application.put_env(:flezha, :compact_after_age_ms, 1)
    {:ok, writer} = Flezha.State.Supervisor.lookup(entity_id)
    TestHelpers.age_writer(writer, 50)
    :ok = Watchdog.sweep_now()

    assert CA.wait_for_snapshot_file(entity_id, 15_000),
           "precondition: v1 must leave a snapshot for the deploy to restore from"

    # The deploy: same operative id and procedure, one schema version later.
    # `preserve_state: true` is what makes this a deploy rather than a delete —
    # the same option a rescale uses when the files are about to be handed on.
    :ok = OperativeManager.stop_operative(namespace, operative_id, preserve_state: true)
    assert {:ok, _} = start(namespace, operative_id, @v2)

    assert NodeTestHelper.wait_until(fn -> total(namespace, operative_id) == 7 end, 10_000),
           "v2 restored #{inspect(state_of(namespace, operative_id))} — a cold start reads 0, " <>
             "which is the loss this change closes"

    assert state_of(namespace, operative_id)["currency"] == "EUR",
           "the field v2 added must come from the migration, not from init_state"

    assert_receive {:migrated, %{operative: ^operative_id, procedure: @procedure}},
                   5_000,
                   "the controller must observe the migration and compact behind it"

    # And the migrated Entity keeps accumulating on the new shape.
    send(namespace, operative_id, "5")
    assert NodeTestHelper.wait_until(fn -> total(namespace, operative_id) == 12 end, 10_000)

    # The migration is paid once: the compaction the controller drove on the
    # back of it rewrote the baseline at the running version, so the NEXT
    # restore of the same Entity finds nothing to migrate.
    assert CA.wait_for_snapshot_file(entity_id, 15_000),
           "the post-migration compaction must leave a snapshot at the running version"

    :ok = OperativeManager.stop_operative(namespace, operative_id, preserve_state: true)
    assert {:ok, _} = start(namespace, operative_id, @v2)
    assert NodeTestHelper.wait_until(fn -> total(namespace, operative_id) == 12 end, 10_000)

    refute_receive {:migrated, %{operative: ^operative_id}},
                   100,
                   "a second restore must find records at the running version"
  end

  @tag :schema_evolution_e2e
  test "v1 code meeting v2 state is refused, not decoded", %{namespace: namespace} do
    {port, _runner_id} = NodeTestHelper.start_runner_port(namespace)
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "evo_down_#{System.unique_integer([:positive])}"

    assert {:ok, _} = start(namespace, operative_id, @v2)
    send(namespace, operative_id, "9")
    assert NodeTestHelper.wait_until(fn -> total(namespace, operative_id) == 9 end, 10_000)

    :ok = OperativeManager.stop_operative(namespace, operative_id, preserve_state: true)

    # The bind-time restore's verdict is what the guard produces; wait for it.
    MessageTestHelper.forward_telemetry(
      [:flezha, :state, :restore_refused],
      :refused
    )

    # Placement only: `start/3`'s residency wait would raise on the very outcome
    # this scenario proves, since a refused restore never makes the Entity
    # resident.
    _ = place(namespace, operative_id, @v1)

    assert_receive {:refused, _, %{entity: {^operative_id, _, _}, reason: reason}}, 30_000

    # Refused BY NAME, which is the whole point. v2's change records encode
    # `{delta, note}` where v1's encode `(delta)`, and a positional codec reads
    # the first field happily — an unguarded restore hands v1 the value 9 as
    # though it had written it. The version stamp is what makes that impossible,
    # so the reason has to be the stamp and not an incidental decode failure.
    assert reason =~ "schema_version 2 is above the running procedure's 1"

    # `operative-state-persistence`, "A refused restore fails the bind instead of
    # cold-starting the Entity": the operative is left unbound. Whether
    # `start_operative/2`'s own reply carried the refusal is NOT part of that
    # guarantee and is a real race — `Replay.dispatch_restore/3` reports the
    # verdict off-process, so `{:restore_refused, _}` and the
    # `:registry_runner_set` call `finalize_init_operative/3` makes reach the
    # Overwatch in either order. The park is the outcome, and it is not a race.
    assert NodeTestHelper.wait_until(fn -> not serving?(namespace, operative_id) end, 10_000),
           "a refused restore must leave the operative unbound, never serving"

    refute total(namespace, operative_id) == 9,
           "v1 must not read v2's state: got #{inspect(state_of(namespace, operative_id))}"
  end

  defp place(namespace, operative_id, factory) do
    OperativeManager.start_operative(namespace, %Builder{
      id: operative_id,
      factory_id: factory,
      init_values: %{},
      opts: []
    })
  end

  # A live Overwatch that is not parked. A refused restore leaves the operative
  # one of two ways — parked in place, or stopped by `finalize_init_operative/3`
  # when the refusal beat the start's own status read — and neither is serving.
  # Binding cold and folding input WOULD be, which is what this scenario forbids.
  defp serving?(namespace, operative_id) do
    with {:ok, pid} <- OperativeManager.get_operative_pid(namespace, operative_id),
         %{init_error: nil} <- :sys.get_state(pid) do
      true
    else
      _ -> false
    end
  catch
    :exit, _ -> false
  end

  defp start(namespace, operative_id, factory) do
    result = place(namespace, operative_id, factory)

    # `start_operative` returns once the operative is placed; its Entity
    # materializes on the runner a moment later, and an input sent into that
    # gap has nothing to reach. A live state read is the readiness signal —
    # it answers only once the Entity exists — and every send below follows it.
    with {:ok, _} <- result, do: await_resident(namespace, operative_id)

    result
  end

  defp await_resident(namespace, operative_id) do
    unless NodeTestHelper.wait_until(fn -> state_of(namespace, operative_id) != nil end, 10_000) do
      raise "operative #{operative_id} never became resident on its runner"
    end
  end

  defp send(namespace, operative_id, input),
    do: OperativeManager.send_io(operative_id, namespace, input, %{})

  defp state_of(namespace, operative_id) do
    case OperativeManager.get_entity_state(namespace, operative_id) do
      {:ok, json} ->
        case Payloader.json_to_map(json) do
          %{@procedure => entity} -> entity
          other -> other
        end

      _ ->
        nil
    end
  end

  defp total(namespace, operative_id) do
    case state_of(namespace, operative_id) do
      %{"total" => t} -> t
      _ -> nil
    end
  end
end
