defmodule Flezha.Api.OperatorVerbGateTest do
  @moduledoc """
  The operator verbs that reach a store with no authorization behind them.

  `Flezha.AuthZ.authorize/3` refuses an action `Flezha.AuthZ.Action` does not
  name, so a verb whose action was never added is not merely ungated — it is
  unauthorizable, and every guard above it can only check the shape of the
  caller's context. Four families were in that position: the dead-letter reads,
  the schedule verbs, and the two that move live work.

  The refusal of a caller with no identity is every row's, asserted once for
  the whole declaration in `Flezha.AuthZ.VerbsTest`. What stays here is the
  half that keeps naming an action from quietly taking a verb away from the
  callers it had: the role the action is granted to still gets through, and
  the entry read hands no payload bytes to a caller it refuses.
  """
  use ExUnit.Case, async: false

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Schedule.Store, as: ScheduleStore
  alias Flezha.Service.Dlq
  alias Flezha.Service.Schedule
  alias Flezha.Service.Workflow
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.DlqFixtures
  alias FlezhaTest.Helper.ScheduleTestHelper

  # No operative of this class runs and none has state on disk, so
  # `Flezha.State.Paths.namespace_of_class/1` resolves it to the system
  # namespace — which is the scope the DLQ verbs are judged in below.
  @class "GateTestOperative"
  @payload "parked-business-data"

  @unbound %{source: :ui}
  @operator %{source: :ui, principal: "op@_system", role: :operator, namespace: "_system"}
  @viewer %{source: :ui, principal: "vw@_system", role: :viewer, namespace: "_system"}

  setup do
    AuthTestHelper.setup_auth_stores(enabled: true, claim: true)
    ScheduleTestHelper.isolated_store_setup()
    ScheduleTestHelper.schedule_hub_setup()

    DlqFixtures.isolated_dir()

    {:ok, entry_ref} =
      DlqStore.append(%{
        operative_class: @class,
        procedure_id: "proc",
        entity_key: "k",
        channel: "input",
        origin_offset: nil,
        error: "boom",
        attempts: 3,
        payload: @payload
      })

    {:ok, entry} = ScheduleStore.add(ScheduleTestHelper.entry_fields(%{name: "gate-job"}))

    %{entry_ref: entry_ref, schedule: entry}
  end

  defp refused?(result), do: match?({:error, {:forbidden, _reason}}, result)

  describe "a caller with no identity, under enforcement" do
    test "is handed no payload bytes by the entry read", ctx do
      assert Flezha.AuthZ.enforcement_posture() == :enforced
      assert {:ok, _entry, @payload} = Dlq.get(ctx.entry_ref, @operator)
      refute inspect(Dlq.get(ctx.entry_ref, @unbound)) =~ @payload
    end
  end

  describe "the role the action is granted to" do
    test "still reads the dead-letter queue", ctx do
      assert {:ok, entries} = Dlq.list(nil, @operator)
      assert Enum.any?(entries, &(&1["operative_class"] == @class))
      assert {:ok, _entries} = Dlq.list(@class, @operator)
      assert {:ok, _entry, @payload} = Dlq.get(ctx.entry_ref, @operator)
      assert {:ok, _stats} = Dlq.stats(@operator)
      assert {:ok, _procedures} = Dlq.procedures(nil, @operator)
    end

    test "still manages schedules, and a viewer still lists them" do
      assert {:ok, _entries} = Schedule.list(@viewer)
      assert {:ok, _statuses} = Schedule.statuses(@viewer)

      assert {:ok, added} =
               Schedule.add(ScheduleTestHelper.entry_fields(%{name: "op-job"}), @operator)

      refute refused?(Schedule.assert_entry(added, @operator))
      refute refused?(Schedule.retract("op-job", @operator))
      assert Schedule.remove("op-job", @operator) == :ok
    end

    test "still reaches the two verbs that move live work" do
      # Neither target exists, so both answer their own refusal — what matters
      # is that neither is an authorization refusal.
      refute refused?(Workflow.rescale("_system", "gate-group", 2, [], @operator))
      refute refused?(Workflow.rewind("_system", @class, 1, @operator))
    end

    test "a viewer is refused the dead-letter reads and the schedule mutations", ctx do
      assert refused?(Dlq.list(nil, @viewer))
      assert refused?(Dlq.get(ctx.entry_ref, @viewer))
      assert refused?(Schedule.add(ScheduleTestHelper.entry_fields(%{name: "vw-job"}), @viewer))
      assert refused?(Schedule.remove("gate-job", @viewer))
    end
  end
end
