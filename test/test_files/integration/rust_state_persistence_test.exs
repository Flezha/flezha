defmodule Flezha.Integration.RustStatePersistenceTest do
  @moduledoc """
  Integration tests for `operative-state-persistence`.

  These tests delegate to the SDK-side Rust integration tests in
  `sdk/rust/tests/operative_dispatch.rs` and the harness self-test in
  `sdk/rust/src/testing/tests/harness_smoke.rs`. End-to-end coverage
  with the controller-side WAL ships in `operative-state-persistence`.

  Scenarios covered Rust-side (verified by `mix test_rust`):

  - `state.apply` records Changes (verified via `OperativeInstance::entities`
    and the per-Entity flush buffer).
  - `ProcedureTestHarness::replay_equals` succeeds for the deterministic
    `CounterProcedure` and fails for a non-deterministic Procedure.
  - Toggle-off (`Operative::PERSISTENCE_ENABLED = false`) drains and
    discards without invoking the wire stub.
  - `Change::coalesce_key` collapses keyed Changes within a linger
    window to the latest.

  This Elixir wrapper exists so `mix test --include integration`
  surfaces a single entry point. It runs the Rust-side test binary
  via `cargo test`; the binary is skipped if the test-app crate is
  not built.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :rust_state_persistence
  # Each test shells out to `cargo test`. On a cold CI runner the
  # compile of the test binaries alone can exceed ExUnit's 60s
  # default timeout even when the workflow pre-builds with --no-run.
  @moduletag timeout: 300_000

  test "rust state-persistence library tests pass" do
    {output, status} =
      System.cmd(
        "cargo",
        ~w(test --features bench-internals -p flezha_runner --lib),
        cd: Path.expand("../../..", __DIR__),
        stderr_to_stdout: true
      )

    assert status == 0,
           "rust state-persistence lib tests failed:\n#{output}"
  end

  test "rust state-persistence integration tests pass" do
    {output, status} =
      System.cmd(
        "cargo",
        ~w(test --features bench-internals -p flezha_runner --tests),
        cd: Path.expand("../../..", __DIR__),
        stderr_to_stdout: true
      )

    assert status == 0,
           "rust state-persistence integration tests failed:\n#{output}"
  end
end
