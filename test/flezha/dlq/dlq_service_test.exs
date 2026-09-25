defmodule Flezha.Service.DlqTest do
  @moduledoc """
  `Flezha.Dlq.Store` is a store: it deletes what it is told to delete. The
  console reaches it directly — `Flezha.Dlq.Store.drop/2` is one of the six DLQ
  call sites in `docs/api/consumers/flezha_ui.txt` — so a console drop lands
  *below* the gate `platform-authn-authz` requires of exactly that verb, and
  below the audit the CLI's own drop emits.

  The first describe block is the hole, asserted at the store: a caller with no
  identity at all deletes a parked record under full enforcement, and nothing
  records that it happened. The rest is the surface that closes it.
  """
  # async: false: writes Application env, a shared on-disk root, auth stores and
  # global telemetry handlers.
  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.Dlq.Store
  alias Flezha.Observability.{RingBufferSink, TestSinks}
  alias Flezha.Service.Dlq, as: DlqSvc
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.DlqFixtures

  setup %{tmp: dir} do
    original = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.merge(original, dir: dir, replay_grace_ms: 200))

    operator_class = "dlqsvc_" <> Integer.to_string(System.unique_integer([:positive]))

    entry = %{
      operator_class: operator_class,
      tasklet_id: "task",
      cell_key: "k",
      channel: "input",
      origin_offset: nil,
      error: "kaboom",
      attempts: 3,
      payload: "poison-bytes"
    }

    %{operator_class: operator_class, entry: entry}
  end

  describe "the hole: the store is not a gate" do
    setup do
      AuthTestHelper.setup_auth_stores(enabled: true, claim: true)
      :ok
    end

    test "a drop with no identity succeeds under enforcement, unaudited", %{entry: entry} do
      assert Flezha.AuthZ.enforcement_posture() == :enforced
      {:ok, ref} = Store.append(entry)
      boundary = System.monotonic_time(:microsecond)

      assert :ok = Store.drop(Store, ref)

      assert {:error, :not_found} = Store.get(ref),
             "the record is gone, and no caller identity was ever asked for"

      TestSinks.settle(RingBufferSink)

      assert RingBufferSink.query(:dlq_service, boundary) == [],
             "the store emitted a drop record it cannot attribute"
    end
  end

  describe "drop/2" do
    test "removes the entry, reports the caller and audits it", %{entry: entry} do
      {:ok, ref} = Store.append(entry)

      _ = RingBufferSink.query(:dlq_service, 0)
      before_ts = System.monotonic_time(:microsecond)

      assert :ok = DlqSvc.drop(ref, %{source: :ui, principal: "console@_system"})
      assert {:error, :not_found} = Store.get(ref)

      TestSinks.settle(RingBufferSink)

      assert Enum.any?(RingBufferSink.query(:dlq_service, before_ts), fn signal ->
               signal.name == :drop and signal.metadata[:entry] == ref and
                 signal.metadata[:caller_source] == :ui and
                 signal.metadata[:principal] == "console@_system"
             end),
             "a drop through the service API must be audited the way the CLI's is"
    end

    test "answers :not_found for an entry that is not there" do
      assert {:error, :not_found} = DlqSvc.drop("nope/task/1", context())
    end
  end

  describe "the reads answer data" do
    test "list, get, stats and tasklets", %{entry: entry, operator_class: operator_class} do
      {:ok, ref} = Store.append(entry)

      assert {:ok, [listed]} = DlqSvc.list(operator_class, context())
      assert listed["id"]
      assert listed["error"] == "kaboom"

      assert {:ok, got, "poison-bytes"} = DlqSvc.get(ref, context())
      assert got["operator_class"] == operator_class

      assert {:ok, stats} = DlqSvc.stats(context())

      assert Enum.any?(
               stats,
               &(&1.operator_class == operator_class and &1.tasklet_id == "task" and
                   &1.entries == 1)
             )

      assert {:ok, tasklets} = DlqSvc.tasklets(operator_class, context())
      assert {operator_class, "task"} in tasklets
    end
  end

  describe "replay/2" do
    setup do
      Flezha.Operator.ClassRegistry.ensure_table()
      :ok
    end

    test "re-enters the ordinary input path and reports removal", %{
      entry: entry,
      operator_class: operator_class
    } do
      {:ok, ref} = Store.append(entry)
      Flezha.Operator.ClassRegistry.put_overwatch(operator_class, self(), nil)

      assert {:ok, :removed} = DlqSvc.replay(ref, context())
      assert_received {:"$gen_cast", {:io_in, "poison-bytes", %{"channel" => "input"}}}
      assert {:error, :not_found} = Store.get(ref)
    end

    test "reports the generation when the replay re-parks", %{
      entry: entry,
      operator_class: operator_class
    } do
      {:ok, ref} = Store.append(entry)
      overwatch = DlqFixtures.spawn_reparking_overwatch(entry, self())
      Flezha.Operator.ClassRegistry.put_overwatch(operator_class, overwatch, nil)

      assert {:ok, {:re_parked, 2}} = DlqSvc.replay(ref, context())
      assert_received :re_parked
    end

    test "refuses an entry whose payload cannot be replayed", %{entry: entry} do
      {:ok, ref} = Store.append(%{entry | payload: nil})
      assert {:error, :payload_not_replayable} = DlqSvc.replay(ref, context())
    end
  end

  defp context, do: %{source: :ui, principal: "console@_system"}
end
