defmodule FlezhaTest.System.RecordedEffectsE2ETest do
  @moduledoc """
  `recorded_effects`: a function run through `ctx.effect` runs once across a
  runner crash. A durable steps relay's tasklet calls a counting function
  through `ctx.effect`, then blocks; the runner is SIGKILLed mid-invocation and a
  released replacement resumes it. The function's run count stays at one,
  because the replacement is served the result the controller kept.

  Tagged `:system` (boots real runner subprocesses); run with
  `mix test --include system test/system/recorded_effects_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :recorded_effects

  import FlezhaTest.Helper.StepsResumeHarness
  import FlezhaTest.Helper.TierE2EHarness, only: [cleanup: 3, unique: 1, unique_id_path: 0]

  alias Flezha.Convergence.Snapshot, as: ConvergenceSnapshot
  alias Flezha.Relay.ClassRegistry
  alias Flezha.State.{InputLog, Paths, TestHelpers}
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag timeout: 180_000

  for transport <- NodeTestHelper.transports() do
    describe "transport=#{transport}" do
      @describetag transport: transport

      test "a function run through ctx.effect runs once across a SIGKILL", ctx do
        counter = Path.join(NodeTestHelper.claim_tmp_dir("flz_effect_e2e"), "model.runs")

        drive_crash_resume(ctx, "effect_block_steps", "ok", &kept?(&1, counter),
          first_message: counter
        )

        assert runs(counter) == 1, "the function ran #{runs(counter)} times across the crash"
      end

      # The controller's in-memory copy goes too: the relay's Overwatch and its
      # input log stop between the crash and the rebind, so the bind reads the
      # kept result back from `effects.log`.
      test "a controller restart between the effect and the commit keeps the result", ctx do
        counter = Path.join(NodeTestHelper.claim_tmp_dir("flz_effect_e2e"), "model.runs")

        drive_crash_resume(ctx, "effect_block_steps", "ok", &kept?(&1, counter),
          first_message: counter,
          before_rebind: fn relay_id ->
            TestHelpers.close_input_log(relay_id)
            {:ok, overwatch, _owner} = ClassRegistry.owner(relay_id)
            Process.exit(overwatch, :kill)
          end
        )

        assert runs(counter) == 1, "the function ran #{runs(counter)} times across the restart"
      end
    end
  end

  describe "a crashed runner's instance moves to another (uds)" do
    @describetag transport: :uds

    # A replacement with an identity of its own: the running convergence loop
    # moves the instance onto it, and the move ships the kept result.
    test "a move to another runner finds the kept result", ctx do
      ConvergenceSnapshot.reset_recovery_states()
      start_supervised!({Flezha.Convergence.Loop, tick_ms: 1_000})
      counter = Path.join(NodeTestHelper.claim_tmp_dir("flz_effect_e2e"), "model.runs")

      drive_crash_resume(ctx, "effect_block_steps", "ok", &kept?(&1, counter),
        first_message: counter,
        rebind: :heal
      )

      assert runs(counter) == 1, "the function ran #{runs(counter)} times across the move"
    end
  end

  describe "a flow relay at the default tier (uds)" do
    @describetag transport: :uds

    # It keeps no input log: nothing re-runs an input, so nothing is kept and
    # the function runs for every input.
    test "runs the function for every input and keeps nothing", ctx do
      namespace = unique("effect_flow")
      relay_id = unique("effect_flow")
      id_path = unique_id_path()
      on_exit(fn -> cleanup(namespace, relay_id, id_path) end)
      counter = Path.join(NodeTestHelper.claim_tmp_dir("flz_effect_e2e"), "model.runs")

      boot_runner(namespace, ctx, id_path, release?: false)
      start_stream(namespace, relay_id, "effect_flow", [])
      send_input(namespace, relay_id, counter)
      send_input(namespace, relay_id, counter)

      assert NodeTestHelper.wait_until(fn -> runs(counter) == 2 end, 30_000),
             "the function ran #{runs(counter)} times for two inputs"

      assert InputLog.effect_results(relay_id, -1) == []
      refute File.exists?(Paths.effects_log_path(relay_id)), "nothing was kept"
    end
  end

  # The mid-run point: the function ran and the controller kept its result, so
  # the crash lands between the effect and the commit.
  defp kept?(relay_id, counter),
    do: runs(counter) == 1 and InputLog.effect_results(relay_id, -1) != []

  defp runs(counter) do
    case File.read(counter) do
      {:ok, lines} -> lines |> String.split("\n", trim: true) |> length()
      {:error, :enoent} -> 0
    end
  end
end
