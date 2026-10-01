defmodule FlezhaTest.State.AdoptionTest do
  @moduledoc """
  A switch to a newer copy that a crash interrupted, resolved at boot by
  `Adoption.settle/0`: the old copy is bound unless the switch had completed,
  never a mix of the two and never nothing.
  """
  # async: false: writes Application env; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  alias Flezha.State.{Adoption, Paths, TestHelpers}

  setup do
    class = "settle_#{System.unique_integer([:positive])}"
    cell_id = {class, "task", "k"}
    TestHelpers.write_wal(cell_id, 0..3, "old")
    TestHelpers.write_wal(staged(cell_id), 0..5, "new")

    %{
      class: class,
      cell_id: cell_id,
      old: File.read!(Paths.wal_path(cell_id)),
      new: File.read!(Paths.wal_path(staged(cell_id)))
    }
  end

  # fails when: a staged copy reads as a class of its own, or survives a boot.
  test "a crash before the switch keeps the old copy and discards the staged one", ctx do
    refute Paths.staging_class(ctx.class) in Paths.operator_classes()

    assert :ok = Adoption.settle()

    assert File.read!(Paths.wal_path(ctx.cell_id)) == ctx.old
    assert residue(ctx.class) == []
  end

  # fails when: the directory set aside is never read back, so the class boots
  # with no state while its only complete copy sits beside it.
  test "a crash between the two renames brings the old copy back", ctx do
    File.rename!(Paths.class_dir(ctx.class), Paths.class_dir(Paths.set_aside_class(ctx.class)))
    refute Paths.set_aside_class(ctx.class) in Paths.operator_classes()

    assert :ok = Adoption.settle()

    assert File.read!(Paths.wal_path(ctx.cell_id)) == ctx.old
    assert residue(ctx.class) == []
  end

  test "a crash after the switch keeps the new copy", ctx do
    File.rename!(Paths.class_dir(ctx.class), Paths.class_dir(Paths.set_aside_class(ctx.class)))
    File.rename!(Paths.class_dir(Paths.staging_class(ctx.class)), Paths.class_dir(ctx.class))

    assert :ok = Adoption.settle()

    assert File.read!(Paths.wal_path(ctx.cell_id)) == ctx.new
    assert residue(ctx.class) == []
  end

  # fails when: a writer can start while the copy is being switched, so the
  # retention sweep's input log appends into the directory set aside and
  # removed.
  test "no writer of a class starts while its copy is being switched", ctx do
    task =
      Flezha.State.Supervisor.without_new_writers(ctx.class, fn ->
        task = Task.async(fn -> Flezha.State.Supervisor.find_or_start(ctx.cell_id) end)
        refute Task.yield(task, 200), "a writer started inside the switch"
        task
      end)

    assert {:ok, writer} = Task.await(task)
    on_exit(fn -> Flezha.State.Supervisor.delete_cell(ctx.cell_id) end)
    assert Process.alive?(writer)
  end

  # fails when: writer starts of one class serialize on one lock, so a start
  # in progress holds up every other start of the class.
  test "writer starts of one class do not wait for each other", ctx do
    test_pid = self()

    held =
      Task.async(fn ->
        Flezha.State.Supervisor.starting(ctx.class, fn ->
          send(test_pid, :holding)
          receive do: (:go -> :ok)
        end)
      end)

    assert_receive :holding
    other = Task.async(fn -> Flezha.State.Supervisor.find_or_start(ctx.cell_id) end)
    assert {:ok, {:ok, writer}} = Task.yield(other, 5_000)
    on_exit(fn -> Flezha.State.Supervisor.delete_cell(ctx.cell_id) end)
    assert Process.alive?(writer)

    send(held.pid, :go)
    assert :ok = Task.await(held)
  end

  # fails when: a switch begins while a writer start is in progress, so that
  # writer opens files in the directory being set aside.
  test "a switch waits for a writer start in progress", ctx do
    test_pid = self()

    held =
      Task.async(fn ->
        Flezha.State.Supervisor.starting(ctx.class, fn ->
          send(test_pid, :holding)
          receive do: (:go -> :ok)
        end)
      end)

    assert_receive :holding

    switch =
      Task.async(fn ->
        Flezha.State.Supervisor.without_new_writers(ctx.class, fn -> :switched end)
      end)

    refute Task.yield(switch, 200), "the switch began while a start was in progress"
    send(held.pid, :go)
    assert :ok = Task.await(held)
    assert :switched = Task.await(switch)
  end

  # fails when: a switch killed before its `after` leaves its mark, and every
  # later writer start of the class waits for it forever.
  test "a switch whose owner was killed does not hold up later starts", ctx do
    test_pid = self()

    owner =
      spawn(fn ->
        Flezha.State.Supervisor.without_new_writers(ctx.class, fn ->
          send(test_pid, :switching)
          receive do: (:never -> :ok)
        end)
      end)

    assert_receive :switching
    Process.exit(owner, :kill)

    start = Task.async(fn -> Flezha.State.Supervisor.find_or_start(ctx.cell_id) end)
    assert {:ok, {:ok, _writer}} = Task.yield(start, 5_000)
    on_exit(fn -> Flezha.State.Supervisor.delete_cell(ctx.cell_id) end)
  end

  # fails when: a start killed before its `after` stays counted, and every
  # later switch of the class waits for it forever.
  test "a start whose process was killed does not hold up a switch", ctx do
    test_pid = self()

    starter =
      spawn(fn ->
        Flezha.State.Supervisor.starting(ctx.class, fn ->
          send(test_pid, :starting)
          receive do: (:never -> :ok)
        end)
      end)

    assert_receive :starting
    Process.exit(starter, :kill)

    switch =
      Task.async(fn ->
        Flezha.State.Supervisor.without_new_writers(ctx.class, fn -> :switched end)
      end)

    assert {:ok, :switched} = Task.yield(switch, 5_000)
  end

  defp staged({class, tasklet, key}), do: {Paths.staging_class(class), tasklet, key}

  defp residue(class) do
    [Paths.staging_class(class), Paths.set_aside_class(class)]
    |> Enum.map(&Paths.class_dir/1)
    |> Enum.filter(&File.exists?/1)
  end
end
