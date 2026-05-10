defmodule Flezha.Integration.GracefulShutdownIntegrationTest do
  @moduledoc """
  Integration coverage for `controller-graceful-shutdown`. Spinning up a
  second BEAM peer and exercising HotSwap migration for real requires
  the same harness used by `quic_test` and friends. Until that harness
  is generalised for shutdown coverage, the multi-node assertions live
  here as `@tag :integration` cases that are excluded from the default
  `mix test` run.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  @tag :skip
  test "two-node cluster: drain on node A migrates Relay to node B" do
    # TODO: spin up a peer node via :peer (or reuse the testnet helper),
    # start a Relay on node A, run Coordinator.shutdown(:cli) on A, and
    # assert the Relay appears in ProcessHub.registry_dump/1 on node B
    # within the timeout. Telemetry order: :requested → :phase_started{quiesce}
    # → :phase_completed{quiesce} → :phase_started{drain} →
    # :drain_progress* → :phase_completed{drain} →
    # :phase_started{finalise} → :phase_completed{finalise} → :complete.
    flunk("integration scaffold; multi-node harness not yet generalised")
  end

  @tag :skip
  test "kill-switch: prep_stop with disabled flag goes through legacy path" do
    # TODO: boot a controller with :graceful_shutdown_enabled false,
    # invoke Application.prep_stop/1 directly, assert
    # HubManager.stop_hub_manager/0 was called and SignalHandler did not
    # install OS handlers.
    flunk("integration scaffold")
  end

  @tag :skip
  test "flz controller stop CLI exits 0 / 1 / 3 as appropriate" do
    # TODO: shell out to the built `flz controller stop` against a
    # running controller, assert exit code 0 with --no-wait omitted,
    # 0 with --force, and 3 against a non-running controller.
    flunk("integration scaffold")
  end
end
