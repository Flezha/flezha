defmodule FlezhaTest.Operative.RestoreRefusalTest do
  @moduledoc """
  A refused restore fails the bind instead of cold-starting the Entity
  (`guard-cross-sdk-placement`).

  `RestoreInitState` is an awaited control round trip and the controller acts
  on its answer. Every way an SDK can refuse a fold — a language mismatch, a
  schema mismatch, a CRC failure, a truncated change log, an apply error —
  means the same thing here: the Entity's durable state did not reach the
  runner, so folding new inputs on top of empty state would lose it silently.

  A genuinely empty Entity is a SUCCESS, not a refusal, and must still bind.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Replay
  alias Flezha.State.Registry
  alias Flezha.State.Restore
  alias Flezha.State.TestHelpers

  @java 2

  setup do
    TestHelpers.tmp_app_data_dir()
    on_exit(fn -> Application.delete_env(:flezha, :state_root) end)
    :ok
  end

  defp entity(base) do
    {"#{base}_#{System.unique_integer([:positive])}", "p0", "k"}
  end

  describe "await_restore/2" do
    test "reports the SDK's refusal, with telemetry naming the entity and reason" do
      id = entity("refused")
      reason = "sdk_lang mismatch: this SDK is rust (1), the record was written by java (2)"

      stub(Flezha.State.Commands, :await_restore_init_state, fn _runner, _entity ->
        {:error, reason}
      end)

      :telemetry.attach(
        "restore-refusal-test",
        [:flezha, :state, :restore_refused],
        fn _event, measurements, metadata, pid ->
          send(pid, {:refused, measurements, metadata})
        end,
        self()
      )

      on_exit(fn -> :telemetry.detach("restore-refusal-test") end)

      assert Replay.await_restore("runner_1", id) == {:error, reason}
      assert_receive {:refused, %{count: 1}, %{entity: ^id, runner: "runner_1", reason: ^reason}}
    end

    test "an unanswered round trip is not a refusal" do
      # A dead control unit, a timeout, a runner shutting down: the SDK
      # reported nothing, so there is no verdict on the state. Parking here
      # would turn ordinary teardown churn into stuck operatives.
      for reason <- [:timeout, :no_control_unit, {:control_unit_exit, {:shutdown, :normal}}] do
        stub(Flezha.State.Commands, :await_restore_init_state, fn _r, _e -> {:error, reason} end)
        assert Replay.await_restore("runner_1", entity("unanswered")) == :ok
      end
    end

    test "a successful restore is plain :ok" do
      stub(Flezha.State.Commands, :await_restore_init_state, fn _runner, _entity ->
        {:ok, <<>>}
      end)

      assert Replay.await_restore("runner_1", entity("fine")) == :ok
    end
  end

  describe "the tier bind" do
    test "a refused restore stops before the journal replay" do
      id = entity("tier_refused")
      TestHelpers.write_wal_of_lang(id, @java)

      stub(Flezha.State.Supervisor, :find_or_start, fn _entity -> {:ok, self()} end)

      refusal = "sdk_lang mismatch: this SDK is rust (1), the record was written by java (2)"

      stub(Flezha.State.Commands, :await_restore_init_state, fn _runner, _entity ->
        {:error, refusal}
      end)

      # Re-forwarding the input tail onto an Entity whose baseline never landed
      # is the silent loss this guards, so it must not be reached.
      stub(Flezha.State.InputLog, :last_offset, fn _class ->
        flunk("the journal replay ran after a refused restore")
      end)

      assert Replay.restore_and_replay_journal(tier_state(id)) == {:error, refusal}
    end

    test "an Entity with no state binds: it is never restored in the first place" do
      id = entity("empty")
      path = Registry.wal_path(id)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "")

      refute Restore.has_persisted_state?(id),
             "a zero-length WAL is no state — an assembled payload from it would " <>
               "rebuild a live Entity to init_state"
    end
  end

  describe "Restore.sdk_lang/1" do
    test "reads byte 0 of the first WAL record when there is no snapshot" do
      id = entity("wal_only")
      TestHelpers.write_wal_of_lang(id, @java)

      assert Restore.sdk_lang(id) == @java
    end

    test "is nil for an Entity with no durable state" do
      assert Restore.sdk_lang(entity("nothing")) == nil
    end

    test "is nil for a truncated WAL rather than a guess" do
      id = entity("truncated")
      path = Registry.wal_path(id)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, <<0::64, 4::32>>)

      assert Restore.sdk_lang(id) == nil
    end
  end

  defp tier_state({operative_class, _, _}) do
    %Overwatch{
      id: operative_class,
      namespace: "default",
      runner_id: "runner_1",
      factory_id: "f",
      effectively_once: true,
      builder: %Builder{id: operative_class, factory_id: "f", kind: :stream, opts: []}
    }
  end
end
