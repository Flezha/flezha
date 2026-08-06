defmodule Flezha.Integration.JavaStatePersistenceTest do
  @moduledoc """
  Integration tests for `operative-state-persistence`.

  Delegates to the JUnit suite in
  `test/apps/java_runner/src/test/java/com/flezha/test/JavaStatePersistenceTest.java`
  and to the SDK module's own unit tests:

  - `flezha-sdk-api/.../ProcedureDefaultsTest` — Procedure interface defaults
  - `flezha-sdk-runtime/.../KryoCodecTest` — Kryo round-trip + cross-format
    detection (postcard `format_id = 1` rejected by Java SDK)
  - `flezha-sdk-runtime/.../HeaderTemplateTest` — header byte-equality with
    the Rust SDK's stamp (only `sdk_lang` and `format_id` differ)
  - `flezha-sdk-runtime/.../EntityFlusherTest` — coalescing, toggle-off
    drain, NoChange skip-registration
  - `flezha-sdk-testing/.../ProcedureTestHarnessSmokeTest` — determinism gate

  End-to-end coverage with the controller-side WAL (round-trip
  `RestoreInitState` after controller restart, time-travel queries, retention
  sweeper) runs through the existing
  `start_runner(ns, runtime: :java)` helper once the
  controller's Java integration matrix wires the new commands.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :java_sdk
  @moduletag :java_state_persistence

  test "java state-persistence module tests pass" do
    {output, status} =
      System.cmd(
        "mvn",
        ~w(-q -B -f sdk/java test
           -pl flezha-sdk-api,flezha-sdk-runtime,flezha-sdk-testing
           -am),
        cd: Path.expand("../../..", __DIR__),
        stderr_to_stdout: true
      )

    assert status == 0,
           "java state-persistence module tests failed:\n#{output}"
  end

  test "java test-app state-persistence test passes" do
    {output, status} =
      System.cmd(
        "mvn",
        ~w(-q -B -f test/apps/java_runner test
           -Dtest=JavaStatePersistenceTest),
        cd: Path.expand("../../..", __DIR__),
        stderr_to_stdout: true
      )

    assert status == 0,
           "java state-persistence test-app test failed:\n#{output}"
  end
end
