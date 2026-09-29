defmodule Flezha.AuthZ.VerbsWalkTest do
  @moduledoc """
  The mechanism driven through the real wire entry points, under `:enforced`.

  Every gated wire verb is invoked the way `flz` invokes it — through
  `Flezha.Service.CliManager` with the credential in the trailing options —
  three ways: with no token, with a token bound to another namespace, and with
  a token holding the declared action in the declared scope. The first two arms
  walk the whole table; the third drives one verb per audit shape and reads the
  record back from the durable log, not the ring buffer. The row set is read
  from the declaration, so a new verb is covered with no edit here.
  """
  # async: false: the auth stores, the schedule store and the audit sink are
  # singletons; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:isolated_dlq_dir, :env], async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.{TokenStore, Verb, Verbs}
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Observability.AuditLog.{Retention, Rotation, Sink, Writer}
  alias Flezha.Observability.TestSinks
  alias Flezha.Schedule.Store, as: ScheduleStore
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperatorManager
  alias FlezhaTest.Helper.{AuthTestHelper, NodeTestHelper, ScheduleTestHelper}

  @elsewhere "walk-elsewhere"

  setup do
    AuthTestHelper.setup_auth_stores(prefix: "flz_verbs_walk", enabled: true, claim: true)
    ScheduleTestHelper.isolated_store_setup()
    ScheduleTestHelper.schedule_hub_setup()
    audit_dir = start_audit_log()

    _ =
      Flezha.Observability.Registry.register_source(%{
        name: :walk_source,
        kinds: [:event],
        flavour: :declarative,
        cardinality_budget: 10
      })

    on_exit(fn -> Flezha.Observability.Registry.unregister_source(:walk_source) end)

    assert Flezha.AuthZ.enforcement_posture() == :enforced

    {:ok, admin_sys, _} = TokenStore.issue("walk-admin", "_system", :admin, 3_600)
    {:ok, viewer_sys, _} = TokenStore.issue("walk-viewer", "_system", :viewer, 3_600)
    {:ok, op_sys, _} = TokenStore.issue("walk-op-sys", "_system", :ops, 3_600)
    {:ok, op_default, _} = TokenStore.issue("walk-op", "default", :ops, 3_600)
    {:ok, stranger, _} = TokenStore.issue("walk-stranger", @elsewhere, :admin, 3_600)

    %{
      audit_dir: audit_dir,
      admin_sys: admin_sys,
      viewer_sys: viewer_sys,
      op_sys: op_sys,
      op_default: op_default,
      stranger: stranger
    }
  end

  test "every gated wire verb refuses a caller with no token, on stderr only" do
    for {name, args, _row} <- gated_wire_verbs() do
      {out, err} = refusal(fn -> apply(CliManager, name, args ++ [[]]) end)

      assert err =~ "(authz.principal_not_bound)", "#{name}: #{inspect({out, err})}"
      assert out == "", "#{name} printed on stdout: #{inspect(out)}"
    end
  end

  test "every namespace-scoped wire verb refuses a token bound elsewhere", ctx do
    for {name, args, row} <- gated_wire_verbs(),
        row.scope == :system or match?({:argument, _}, row.scope) do
      {out, err} = refusal(fn -> apply(CliManager, name, args ++ [[token: ctx.stranger]]) end)

      assert err =~ ~r/\(authz\.(namespace_mismatch|cross_namespace_admin_denied)\)/,
             "#{name}: #{inspect({out, err})}"
    end
  end

  test "a public verb is served with no token" do
    assert capture_io(fn -> CliManager.boot_status([]) end) =~ "recovery_state"

    # `boot await` answers the readiness verdict, so on a node a neighbouring
    # suite left mid-recovery it exits non-zero naming the unmet row. That
    # is the verb being SERVED; what a missing credential would produce is a
    # refusal, and this asserts which of the two happened rather than pinning a
    # verdict this test is not about.
    {{_result, out}, err} =
      with_io(:stderr, fn ->
        with_io(fn ->
          try do
            CliManager.boot_await(10, [])
          catch
            # `catch_exit/1` raises when the call returns normally, and either
            # outcome is legitimate here.
            :exit, reason -> reason
          end
        end)
      end)

    served = out <> err
    assert served =~ ~r/boot await: ready|\(boot\.not_ready\)/
    refute served =~ "refused"
  end

  test "an authorized act leaves exactly one durable record with the row's declared source",
       ctx do
    add =
      capture_io(fn ->
        CliManager.schedule_add("walk-sched",
          cron: "0 8 * * *",
          tz: "Etc/UTC",
          factory: "f",
          token: ctx.admin_sys
        )
      end)

    assert add =~ "Schedule walk-sched added"

    {:ok, ref} = DlqStore.append(parked("WalkDlqClass"))

    assert capture_io(fn -> CliManager.dlq_drop(ref, token: ctx.admin_sys) end) =~
             "dropped #{ref}"

    assert capture_io(fn ->
             CliManager.state_retention("WalkClass", "2", token: ctx.admin_sys)
           end) =~
             "retention[WalkClass] = 2 hours"

    assert capture_io(fn -> CliManager.audit_log_render_stats(token: ctx.viewer_sys) end) =~
             "record_count"

    assert capture_io(fn ->
             CliManager.observability_render_enable("walk_source", token: ctx.op_default)
           end) =~
             "source walk_source enabled"

    # An authorized act that fails is still an act: the record carries the outcome.
    {_out, _err} =
      refusal(fn -> CliManager.operator_stop("default", "walk-nope", token: ctx.op_default) end)

    settle(ctx.audit_dir)

    assert [record] = records(ctx.audit_dir, "schedule_service", "add")
    assert record["metadata"]["action"] == "schedule.add"
    assert record["metadata"]["schedule"] == "walk-sched"
    assert record["metadata"]["outcome"] == "ok"
    assert record["caller"]["principal"] == "walk-admin"
    assert record["caller"]["source"] == "cli"
    assert record["namespace"] == "_system"

    assert [drop] = records(ctx.audit_dir, "dlq_service", "drop")
    assert drop["metadata"]["entry"] == ref
    assert drop["metadata"]["outcome"] == "ok"

    assert [retention] = records(ctx.audit_dir, "state_service", "retention")

    assert retention["metadata"] |> Map.take(["operator_class", "hours"]) == %{
             "operator_class" => "WalkClass",
             "hours" => "2"
           }

    assert [stats] = records(ctx.audit_dir, "audit_log", "stats")
    assert stats["caller"]["principal"] == "walk-viewer"

    assert [enable] = records(ctx.audit_dir, "observability_service", "enable_source")
    assert enable["metadata"]["source"] == "walk_source"
    assert enable["namespace"] == "default"

    assert [stop] = records(ctx.audit_dir, "operator_service", "stop")
    assert stop["metadata"]["outcome"] == "error"
    assert stop["metadata"]["operator"] == "walk-nope"
  end

  test "a row-scoped verb refuses before it reads, and a bound caller sees the row's verdict",
       ctx do
    {:ok, ref} = DlqStore.append(parked("WalkRowClass"))

    for token_opts <- [[], [token: ctx.viewer_sys]] do
      {_out, err} = refusal(fn -> CliManager.dlq_drop("nope/task/1", token_opts) end)
      assert err =~ ~r/\(authz\.(principal_not_bound|role_insufficient)\)/
      refute err =~ "dlq.not_found", "the row was read before the refusal: #{err}"
    end

    # A bound admin elsewhere passes the role check, reads the row, and is
    # refused in the row's namespace.
    {_out, err} = refusal(fn -> CliManager.dlq_drop(ref, token: ctx.stranger) end)
    assert err =~ "(authz.namespace_mismatch)"
    assert {:ok, _entry, _payload} = DlqStore.get(ref)

    # Firing a schedule is judged where the instance starts, not in `_system`.
    {:ok, _} = ScheduleStore.add(ScheduleTestHelper.entry_fields(%{name: "walk-run"}))
    test = self()

    stub(OperatorManager, :start_bounded, fn "default", id, _factory, _init, _opts ->
      send(test, {:spawned, id})
      {:ok, %{operator_id: id}}
    end)

    {_out, err} = refusal(fn -> CliManager.schedule_run("walk-run", token: ctx.op_sys) end)
    assert err =~ "(authz.namespace_mismatch)"
    refute_received {:spawned, _}

    assert capture_io(fn -> CliManager.schedule_run("walk-run", token: ctx.op_default) end) =~
             "Spawned walk-run@manual-"

    assert_received {:spawned, "walk-run@manual-" <> _}
  end

  test "whoami is a gated read: refused unbound, answered for a bound viewer", ctx do
    {_out, err} = refusal(fn -> CliManager.authz_render_whoami([]) end)
    assert err =~ "(authz.principal_not_bound)"

    assert capture_io(fn -> CliManager.authz_render_whoami(token: ctx.viewer_sys) end) =~
             "principal: walk-viewer"
  end

  # ---------- helpers ----------

  # Every wire name bound to a gated row, once at its widest arity, with one
  # binary per positional argument: the refusal comes before any body reads
  # them, and a namespace-shaped argument is what a scope rule needs.
  defp gated_wire_verbs do
    rows = Map.new(Verbs.all(), &{&1.verb, &1})

    Verb.bindings(CliManager)
    |> Enum.group_by(&elem(&1, 0))
    |> Enum.map(fn {_name, bindings} -> Enum.max_by(bindings, &elem(&1, 1)) end)
    |> Enum.reject(fn {_name, _arity, verb, _opts} -> rows[verb].action in [:public, :none] end)
    |> Enum.map(fn {name, arity, verb, _opts} ->
      {name, List.duplicate("x", arity - 1), rows[verb]}
    end)
    |> Enum.sort()
  end

  # Both streams of a verb that refuses, the exit swallowed: any class's code.
  defp refusal(fun) do
    {{{:shutdown, code}, out}, err} =
      with_io(:stderr, fn ->
        with_io(fn -> catch_exit(fun.()) end)
      end)

    assert code in 1..7
    {out, err}
  end

  defp parked(operator_class) do
    %{
      operator_class: operator_class,
      tasklet_id: "task",
      cell_key: "k",
      channel: "input",
      origin_offset: nil,
      error: "boom",
      attempts: 1,
      payload: "bytes"
    }
  end

  defp start_audit_log do
    dir =
      Path.join(System.tmp_dir!(), "flz_verbs_walk_audit_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    prev = Application.get_env(:flezha, :audit_log, [])
    Application.put_env(:flezha, :audit_log, Keyword.merge(prev, enabled: true, dir: dir))

    _ =
      Flezha.Observability.Registry.register_source(%{
        name: :audit_log,
        kinds: [:event],
        flavour: :declarative,
        cardinality_budget: 200
      })

    {:ok, sup} =
      Supervisor.start_link([Writer, Rotation, Retention, Sink], strategy: :one_for_one)

    Process.unlink(sup)

    on_exit(fn ->
      try do
        Supervisor.stop(sup, :normal, 2_000)
      catch
        :exit, _ -> :ok
      end

      _ = Flezha.Observability.Registry.unregister_sink(:audit_log)
      Application.put_env(:flezha, :audit_log, prev)
      File.rm_rf!(dir)
    end)

    dir
  end

  # Draining the sink's mailbox is only half the signal: the record is readable
  # once the sink has written the line, so wait for the walk's last act.
  defp settle(dir) do
    TestSinks.settle(Sink)

    assert NodeTestHelper.wait_until(
             fn -> records(dir, "operator_service", "stop") != [] end,
             2_000
           ),
           "the audit sink never wrote the walk's last record"
  end

  defp records(dir, source, name) do
    dir
    |> Path.join("audit-current.jsonl")
    |> File.read()
    |> case do
      {:ok, body} -> body
      {:error, _} -> ""
    end
    |> String.split("\n", trim: true)
    |> Enum.map(&Jason.decode!/1)
    |> Enum.filter(&(&1["source"] == source and &1["name"] == name))
  end
end
