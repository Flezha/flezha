defmodule Flezha.Service.StepsPageTest do
  @moduledoc """
  The steps reads with `opts`: undelivered counted from the input log's
  offsets and read only for its page, committed iterations newest first, and
  the Cells key search.
  """
  use FlezhaTest.StateCase, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Cluster.Page
  alias Flezha.Service.Steps
  alias Flezha.State.{InputLog, Metrics, TaskJournal, TestHelpers}

  setup :set_mimic_global_until_exit

  setup %{base: base} do
    class = "pg_" <> uniq()
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))

    on_exit(fn ->
      TestHelpers.close_input_log(class)
      File.rm_rf!(base)
    end)

    %{class: class}
  end

  defp page(read, opts, declared) do
    {:ok, page} = Page.parse(opts, read, declared)
    page
  end

  defp undelivered(class, opts) do
    {:ok, answer} =
      Steps.local_undelivered(class, page(:undelivered, opts, sorts: [offset: :asc]))

    answer
  end

  test "undelivered total equals the unpaged length across appends and consumes", %{class: class} do
    for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{"input_type" => "2"})
    {:ok, all} = Steps.local_undelivered(class)
    assert undelivered(class, limit: 0).total == length(all)

    InputLog.record_consumed(class, {class, "task", "a"}, 3)
    {:ok, all} = Steps.local_undelivered(class)
    first = undelivered(class, limit: 4)
    assert first.total == length(all) and length(all) == 6
    assert Enum.map(first.rows, & &1.offset) == [4, 5, 6, 7]

    second = undelivered(class, limit: 4, after: first.next)
    assert Enum.map(second.rows, & &1.offset) == [8, 9] and second.next == nil

    {:ok, _} = InputLog.append(class, "m10", %{"input_type" => "2"})
    assert undelivered(class, limit: 0).total == 7
  end

  test "limit 0 reads no record", %{class: class} do
    for i <- 0..4, do: {:ok, _} = InputLog.append(class, "m#{i}", %{"input_type" => "2"})
    Mimic.reject(InputLog, :read_range, 3)
    assert %{rows: [], total: 5, next: nil} = undelivered(class, limit: 0)
  end

  test "committed iterations page newest first" do
    Mimic.stub(TaskJournal, :committed_iterations, fn _id -> Enum.to_list(0..119) end)
    declared = [sorts: [iteration: :desc]]

    {:ok, first} =
      Steps.local_committed_iterations(
        "i",
        page(:committed_iterations, [limit: 50], declared)
      )

    assert first.rows == Enum.to_list(119..70//-1) and first.total == 120 and
             is_binary(first.next)

    {:ok, unpaged} = Steps.local_committed_iterations("i")
    assert unpaged == Enum.to_list(0..119)
  end

  test "Cells page by key with a key search" do
    cells = for k <- ~w(eu-1 us-1 eu-2), do: {"cls_" <> uniq(), "t", k}
    class = cells |> hd() |> elem(0)
    cells = for {_c, t, k} <- cells, do: {class, t, k}
    Enum.each(cells, &Metrics.for_cell/1)
    on_exit(fn -> Enum.each(cells, &Metrics.drop_cell/1) end)

    {:ok, answer} =
      Steps.local_cells(
        class,
        page(:cells, [filter: [text: "eu"]], sorts: [key: :asc], filters: [:text])
      )

    assert Enum.map(answer.rows, & &1.cell_key) == ["eu-1", "eu-2"] and answer.total == 2
    refute Map.has_key?(answer, :cells)

    {:ok, unpaged} = Steps.local_cells(class)
    assert length(unpaged.cells) == 3
  end
end
