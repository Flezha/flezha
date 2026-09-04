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
  use ExUnit.Case, async: false

  alias Flezha.Dlq.Store
  alias Flezha.Observability.{RingBufferSink, TestSinks}
  alias Flezha.Service.Dlq, as: DlqSvc
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.DlqFixtures

  setup do
    dir = Path.join(System.tmp_dir!(), "flezha_dlq_svc_#{System.unique_integer([:positive])}")
    original = Application.get_env(:flezha, :dlq, [])
    Application.put_env(:flezha, :dlq, Keyword.merge(original, dir: dir, replay_grace_ms: 200))

    on_exit(fn ->
      Application.put_env(:flezha, :dlq, original)
      File.rm_rf!(dir)
    end)

    operative_class = "dlqsvc_" <> Integer.to_string(System.unique_integer([:positive]))

    entry = %{
      operative_class: operative_class,
      procedure_id: "proc",
      entity_key: "k",
      channel: "input",
      origin_offset: nil,
      error: "kaboom",
      attempts: 3,
      payload: "poison-bytes"
    }

    %{operative_class: operative_class, entry: entry}
  end

  describe "the hole: the store is not a gate" do
    setup do
      AuthTestHelper.setup_auth_stores(enabled: true, claim: true)
      :ok
    end

    test "a drop with no identity succeeds under enforcement, unaudited", %{entry: entry} do
      assert Flezha.AuthZ.enforcement_posture() == :enforced
      {:ok, ref} = Store.append(entry)

      events = watch_operator_drop()

      assert :ok = Store.drop(Store, ref)

      assert {:error, :not_found} = Store.get(ref),
             "the record is gone, and no caller identity was ever asked for"

      assert events.() == [], "the store emitted an operator-drop event it cannot attribute"
    end

    test "the same drop through the service API is refused", %{entry: entry} do
      {:ok, ref} = Store.append(entry)

      assert {:error, {:forbidden, :principal_not_bound}} = DlqSvc.drop(ref, %{source: :ui})
      assert {:ok, _entry, _payload} = Store.get(ref), "a refused drop must not delete"
    end
  end

  describe "drop/2" do
    test "removes the entry, reports the caller and audits it", %{entry: entry} do
      {:ok, ref} = Store.append(entry)

      events = watch_operator_drop()
      _ = RingBufferSink.query(:dlq_service, 0)
      before_ts = System.monotonic_time(:microsecond)

      assert :ok = DlqSvc.drop(ref, %{source: :ui, principal: "console@_system"})
      assert {:error, :not_found} = Store.get(ref)

      assert [{%{count: 1}, %{entry: ^ref, caller_source: :ui}}] = events.()

      TestSinks.settle(RingBufferSink)

      assert Enum.any?(RingBufferSink.query(:dlq_service, before_ts), fn signal ->
               signal.name == :drop and signal.metadata[:entry] == ref and
                 signal.metadata[:caller_source] == :ui
             end),
             "a drop through the service API must be audited the way the CLI's is"
    end

    test "refuses a caller with no context and leaves the entry", %{entry: entry} do
      {:ok, ref} = Store.append(entry)

      assert {:error, :invalid_caller_context} = DlqSvc.drop(ref, %{})
      assert {:ok, _entry, _payload} = Store.get(ref)
    end

    test "answers :not_found for an entry that is not there" do
      assert {:error, :not_found} = DlqSvc.drop("nope/proc/1", context())
    end
  end

  describe "the reads answer data" do
    test "list, get, stats and procedures", %{entry: entry, operative_class: operative_class} do
      {:ok, ref} = Store.append(entry)

      assert {:ok, [listed]} = DlqSvc.list(operative_class, context())
      assert listed["id"]
      assert listed["error"] == "kaboom"

      assert {:ok, got, "poison-bytes"} = DlqSvc.get(ref, context())
      assert got["operative_class"] == operative_class

      assert {:ok, stats} = DlqSvc.stats(context())

      assert Enum.any?(
               stats,
               &(&1.operative_class == operative_class and &1.procedure_id == "proc" and
                   &1.entries == 1)
             )

      assert {:ok, procedures} = DlqSvc.procedures(operative_class, context())
      assert {operative_class, "proc"} in procedures
    end

    test "every read refuses a caller with no context", %{entry: entry} do
      {:ok, ref} = Store.append(entry)

      assert {:error, :invalid_caller_context} = DlqSvc.list(nil, %{})
      assert {:error, :invalid_caller_context} = DlqSvc.get(ref, %{})
      assert {:error, :invalid_caller_context} = DlqSvc.stats(%{})
      assert {:error, :invalid_caller_context} = DlqSvc.procedures(nil, %{})
    end
  end

  describe "replay/2" do
    setup do
      Flezha.Operative.ClassRegistry.ensure_table()
      :ok
    end

    test "re-enters the ordinary input path and reports removal", %{
      entry: entry,
      operative_class: operative_class
    } do
      {:ok, ref} = Store.append(entry)
      Flezha.Operative.ClassRegistry.put_overwatch(operative_class, self())

      assert {:ok, :removed} = DlqSvc.replay(ref, context())
      assert_received {:"$gen_cast", {:io_in, "poison-bytes", %{"channel" => "input"}}}
      assert {:error, :not_found} = Store.get(ref)
    end

    test "reports the generation when the replay re-parks", %{
      entry: entry,
      operative_class: operative_class
    } do
      {:ok, ref} = Store.append(entry)
      overwatch = DlqFixtures.spawn_reparking_overwatch(entry, self())
      Flezha.Operative.ClassRegistry.put_overwatch(operative_class, overwatch)

      assert {:ok, {:re_parked, 2}} = DlqSvc.replay(ref, context())
      assert_received :re_parked
    end

    test "refuses an entry whose payload cannot be replayed", %{entry: entry} do
      {:ok, ref} = Store.append(%{entry | payload: nil})
      assert {:error, :payload_not_replayable} = DlqSvc.replay(ref, context())
    end

    test "refuses a caller with no context", %{entry: entry} do
      {:ok, ref} = Store.append(entry)
      assert {:error, :invalid_caller_context} = DlqSvc.replay(ref, %{})
    end
  end

  defp context, do: %{source: :ui, principal: "console@_system"}

  # Collect `[:flezha, :dlq, :operator_drop]` events; the returned function
  # answers what arrived so far.
  defp watch_operator_drop do
    handler = :"dlq_svc_#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:flezha, :dlq, :operator_drop],
      fn _event, measurements, metadata, _config ->
        send(parent, {:operator_drop, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    fn -> drain() end
  end

  defp drain(acc \\ []) do
    receive do
      {:operator_drop, measurements, metadata} -> drain([{measurements, metadata} | acc])
    after
      50 -> Enum.reverse(acc)
    end
  end
end
