defmodule Flezha.Cluster.PageTest do
  @moduledoc """
  The one paging shape: the limit rule, the keyset cursor bound to its read and
  sort, the refusals made before any work, and the newest-first merge of the
  pages several controllers answer.
  """
  use ExUnit.Case, async: true

  alias Flezha.Cluster.Page

  @declared [sorts: [id: :asc, newest: :desc], filters: [:text]]

  defp parse(opts, read \\ :rows), do: Page.parse(opts, read, @declared)

  defp slice(rows, opts, read \\ :rows) do
    {:ok, page} = parse(opts, read)
    Page.slice(rows, page, & &1)
  end

  defp code({:error, %Flezha.Error{code: code}}), do: code

  test "the limit defaults to 50, stops at 500 and 0 answers only the total" do
    rows = Enum.to_list(1..600)

    assert {:ok, nil} = parse([])
    assert %{rows: first, total: 600, next: next} = slice(rows, sort: :id)
    assert first == Enum.to_list(1..50) and is_binary(next)
    assert length(slice(rows, limit: 500).rows) == 500
    assert %{rows: [], total: 600, next: nil} = slice(rows, limit: 0)
    assert code(parse(limit: 501)) == "usage.invalid_number"
    assert code(parse(limit: -1)) == "usage.invalid_number"
  end

  test "the last page carries no cursor, and a descending sort pages from the top" do
    %{next: next} = slice(Enum.to_list(1..5), limit: 3, sort: :newest)

    assert %{rows: [2, 1], next: nil} =
             slice(Enum.to_list(1..5), limit: 3, sort: "newest", after: next)
  end

  test "a cursor issued for another read or sort, or tampered with, is refused" do
    %{next: next} = slice(Enum.to_list(1..5), limit: 2)

    assert code(parse([after: next], :other)) == "usage.invalid_number"
    assert code(parse(after: next, sort: :newest)) == "usage.invalid_number"
    assert code(parse(after: "x" <> next)) == "usage.invalid_number"

    assert code(
             parse(after: Base.url_encode64(:erlang.term_to_binary({:rows, :id}), padding: false))
           ) ==
             "usage.invalid_number"

    assert code(parse(after: 7)) == "usage.invalid_number"
  end

  test "an unknown option, filter or sort key is refused" do
    assert code(parse(offset: 3)) == "control.unknown_parameter"
    assert code(parse(filter: [colour: "red"])) == "control.unknown_parameter"
    assert code(parse(sort: :age)) == "usage.unknown_word"
    assert {:ok, %Page{filter: %{text: "a"}}} = parse(%{"filter" => %{"text" => "a"}})
  end

  test "a row removed before the cursor between two pages neither repeats nor skips one" do
    %{rows: [1, 2, 3], next: next} = slice(Enum.to_list(1..9), limit: 3)
    assert %{rows: [4, 5, 6]} = slice(Enum.to_list(1..9) -- [2], limit: 3, after: next)
    assert %{rows: [5, 6, 7]} = slice(Enum.to_list(1..9) -- [4], limit: 3, after: next)
  end

  test "pages from several controllers merge newest first under one limit" do
    {:ok, page} = parse(limit: 3, sort: :newest)
    a = Page.slice([1, 4, 7], page, & &1)
    b = Page.slice([2, 3, 5, 6, 8, 9], page, & &1)

    merged = Page.merge(%{rows: [a, b], unreachable: [:gone@host]}, page, & &1)
    assert %{rows: [9, 8, 7], total: 9, unreachable: [:gone@host]} = merged

    {:ok, next} = parse(limit: 3, sort: :newest, after: merged.next)

    rest =
      Page.merge(
        %{
          rows: [Page.slice([1, 4, 7], next, & &1), Page.slice([2, 3, 5, 6, 8, 9], next, & &1)],
          unreachable: []
        },
        next,
        & &1
      )

    assert rest.rows == [6, 5, 4] and is_binary(rest.next)
  end
end
