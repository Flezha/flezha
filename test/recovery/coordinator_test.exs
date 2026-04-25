defmodule Flezha.Recovery.CoordinatorTest do
  @moduledoc """
  Unit tests for the Coordinator — stubs `ProcessorManager.start_processor/2`
  with Mimic and exercises the public hook handlers and the recovery loop in
  isolation from ProcessHub.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode
  alias Flezha.Recovery.Coordinator
  alias Flezha.Processor.Builder

  setup :set_mimic_global

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_coord_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    prev_dir = Application.get_env(:flezha, :recovery_data_dir)
    Application.put_env(:flezha, :recovery_data_dir, tmp)

    stop_if_running(Coordinator)
    stop_if_running(SysMode)
    stop_if_running(Store)

    {:ok, _} = Store.start_link([])
    {:ok, _} = Coordinator.start_link([])

    on_exit(fn ->
      stop_if_running(Coordinator)
      stop_if_running(Store)

      case prev_dir do
        nil -> Application.delete_env(:flezha, :recovery_data_dir)
        v -> Application.put_env(:flezha, :recovery_data_dir, v)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  describe "ProcessHub hook handlers" do
    test "on_post_children_start persists local children only" do
      current = node()

      payload = %{
        children: [
          %{
            child_id: "p-local",
            pid: self(),
            result: :ok,
            nodes: [current],
            child_spec: %{
              id: "p-local",
              start:
                {Flezha.Processor.Overwatch, :start_link,
                 [{"ns", %Builder{id: "p-local", factory_id: "f"}}]},
              restart: :transient
            },
            metadata: %{}
          },
          %{
            child_id: "p-remote",
            pid: :c.pid(0, 250, 0),
            result: :ok,
            nodes: [:other@host],
            child_spec: %{
              id: "p-remote",
              start:
                {Flezha.Processor.Overwatch, :start_link,
                 [{"ns", %Builder{id: "p-remote", factory_id: "f"}}]},
              restart: :transient
            },
            metadata: %{}
          }
        ]
      }

      assert :ok = Coordinator.on_post_children_start(payload)

      ids = Store.all() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
      assert ids == ["p-local"]
    end

    test "on_post_children_start handles the 3-tuple arg form after alter_child_data" do
      current = node()

      payload = %{
        children: [
          %{
            child_id: "p-alt",
            pid: self(),
            result: :ok,
            nodes: [current],
            child_spec: %{
              id: "p-alt",
              start:
                {Flezha.Processor.Overwatch, :start_link,
                 [{"ns", "compute_engine_1", %Builder{id: "p-alt", factory_id: "f"}}]},
              restart: :transient
            },
            metadata: %{tag: "compute_engine_1"}
          }
        ]
      }

      assert :ok = Coordinator.on_post_children_start(payload)

      [{"p-alt", row}] = Store.all()
      assert row.namespace == "ns"
      assert row.processor.id == "p-alt"
    end

    test "on_child_unregistered deletes the row" do
      :ok = Store.put("cid-to-remove", "ns", %Builder{id: "cid-to-remove", factory_id: "f"})
      assert Store.count() == 1

      assert :ok = Coordinator.on_child_unregistered(%{child_id: "cid-to-remove"})
      assert Store.count() == 0
    end
  end

  describe "run_recovery/0" do
    test "iterates all rows calling ProcessorManager.start_processor serially" do
      call_order = :ets.new(:call_order, [:public, :ordered_set])
      :ets.insert(call_order, {:counter, 0})

      Flezha.Service.ProcessorManager
      |> Mimic.expect(:start_processor, 3, fn namespace, processor ->
        n = :ets.update_counter(call_order, :counter, 1)
        :ets.insert(call_order, {n, {namespace, processor.id}})
        {:ok, %{processor_id: processor.id, compute_engine_id: "ce", node: node(), pid: self()}}
      end)

      for id <- ["a", "b", "c"] do
        :ok = Store.put(id, "ns", %Builder{id: id, factory_id: "f"})
      end

      %{recovered: 3, failed: 0, aborted: false} = Coordinator.run_recovery()

      order =
        :ets.tab2list(call_order)
        |> Enum.reject(fn {k, _} -> k == :counter end)
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map(fn {_, v} -> v end)

      assert length(order) == 3
      assert Enum.all?(order, fn {ns, _} -> ns == "ns" end)
    end

    test "per-row permanent failure is logged and row is deleted" do
      Flezha.Service.ProcessorManager
      |> Mimic.stub(:start_processor, fn _ns, proc ->
        case proc.id do
          "good" ->
            {:ok, %{processor_id: "good", compute_engine_id: "ce", node: node(), pid: self()}}

          "bad" ->
            {:error, "Factory with id non_existing_factory does not exist"}
        end
      end)

      :ok = Store.put("good", "ns", %Builder{id: "good", factory_id: "f"})
      :ok = Store.put("bad", "ns", %Builder{id: "bad", factory_id: "nope"})

      result = Coordinator.run_recovery()

      assert result.recovered == 1
      assert result.failed == 1

      ids = Store.all() |> Enum.map(&elem(&1, 0))
      refute "bad" in ids
      assert "good" in ids
    end

    test "abort flag halts the loop after the current row" do
      parent = self()

      Flezha.Service.ProcessorManager
      |> Mimic.stub(:start_processor, fn _ns, %{id: id} ->
        send(parent, {:started, id})
        # Simulate the peer_normal arriving mid-recovery by flipping the flag
        # AFTER the first row starts.
        Coordinator.set_abort_flag(true)
        {:ok, %{processor_id: id, compute_engine_id: "ce", node: node(), pid: parent}}
      end)

      for id <- ["row-1", "row-2", "row-3"] do
        :ok = Store.put(id, "ns", %Builder{id: id, factory_id: "f"})
      end

      result = Coordinator.run_recovery()

      assert result.aborted == true
      # Exactly one row was processed before abort.
      assert_received {:started, "row-1"}
      refute_received {:started, _other}
      assert result.recovered == 1

      # Reset for other tests.
      Coordinator.set_abort_flag(false)
    end

    test "emits :completed telemetry with counts and duration" do
      handler = "coord-completed-#{System.unique_integer([:positive])}"
      pid = self()

      :telemetry.attach(
        handler,
        [:flezha, :recovery, :completed],
        fn _n, m, meta, _cfg -> send(pid, {:completed, m, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      Flezha.Service.ProcessorManager
      |> Mimic.expect(:start_processor, fn _ns, %{id: id} ->
        {:ok, %{processor_id: id, compute_engine_id: "ce", node: node(), pid: self()}}
      end)

      :ok = Store.put("p", "ns", %Builder{id: "p", factory_id: "f"})

      _ = Coordinator.run_recovery()

      assert_receive {:completed, %{recovered: 1, failed: 0, duration_ms: ms}, meta}, 500
      assert is_integer(ms)
      assert meta.aborted == false
      assert meta.total_rows == 1
    end
  end

  defp stop_if_running(mod) do
    try do
      case Process.whereis(mod) do
        nil -> :ok
        pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
      end
    catch
      :exit, _ -> :ok
    end
  end
end
