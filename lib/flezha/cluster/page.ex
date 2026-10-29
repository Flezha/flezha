defmodule Flezha.Cluster.Page do
  @moduledoc """
  One page of a list read: the one paging shape every read that takes `opts`
  answers, so a caller learns it once.

  A read declares its sorts, each with its direction, and its filter keys. The
  caller's `opts` are `limit` (default 50, at most 500, `0` answers only the
  totals), `after` (the cursor of the previous page), `sort` (one of the read's
  sorts, the first by default) and `filter` (a keyword list or map of the read's
  filter keys). `parse/3` refuses anything else before any work, each refusal
  of class `invalid`.

  A paged answer is `%{rows:, total:, next:, unreachable:}`: at most `limit`
  rows in sort order, every row the filter matched before the limit, the cursor
  of the next page or `nil` on the last, and the controllers that did not
  answer (`[]` for a read one controller answers). The cursor is a keyset cursor: it carries the
  read, the sort and the last row's sort key, so a row added or removed before
  it between two pages neither repeats nor skips a row still there. Callers
  pass it back as they got it; a cursor issued for another read or sort, or one
  that does not decode, is refused as invalid. A row's sort key ends in its id,
  so two rows never share one.

  `merge/3` joins the pages several controllers answered for one newest-first
  read, through `Flezha.Cluster.Reads.newest/3`.
  """

  alias Flezha.Cluster.Reads

  @default_limit 50
  @max_limit 500
  @keys [:limit, :after, :sort, :filter]

  @enforce_keys [:read, :limit, :sort, :direction]
  defstruct [:read, :limit, :sort, :direction, after: nil, filter: %{}]

  @type direction :: :asc | :desc
  @type t :: %__MODULE__{
          read: atom(),
          limit: non_neg_integer() | nil,
          sort: atom(),
          direction: direction(),
          after: term() | nil,
          filter: %{atom() => term()}
        }
  @type answer :: %{
          rows: [term()],
          total: non_neg_integer(),
          next: String.t() | nil,
          unreachable: [node()]
        }

  @doc """
  The trailing `opts` argument a paged read declares in its `verb` binding:
  without it the read answers what it always did.
  """
  @spec arg() :: keyword()
  def arg(),
    do: [
      type: :keyword,
      default: [],
      doc: "`limit`, `after`, `sort` and `filter`; the whole answer when empty."
    ]

  @doc """
  The page `opts` ask of `read`, which declares `sorts` (`[name: direction]`,
  the first the default), `filters` (the filter keys it takes) and, for a read
  that only filters, `keys` (the options it takes), or the refusal of the first
  key, value or cursor it does not accept. A read with bounds of its own names
  them as `limit: {default, max}`; a `nil` default leaves `limit` `nil` when
  the caller gives none. The option its cursor comes back in is named as
  `cursor:` (`:after` unless named). Empty `opts` ask no page: `nil`, and the
  read answers what it answers without them.
  """
  @spec parse(keyword() | map(), atom(), keyword()) ::
          {:ok, t() | nil} | {:error, Flezha.Error.t()}
  def parse(opts, _read, _declared) when opts in [[], %{}], do: {:ok, nil}

  def parse(opts, read, declared) do
    sorts = Keyword.fetch!(declared, :sorts)
    {default, max} = Keyword.get(declared, :limit, {@default_limit, @max_limit})
    cursor_key = declared |> Keyword.get(:cursor, :after) |> to_string()
    opts = Map.new(opts, fn {key, value} -> {to_string(key), value} end)

    with :ok <- known(Map.keys(opts), Enum.map(declared[:keys] || @keys, &to_string/1)),
         {:ok, limit} <- limit(Map.fetch(opts, "limit"), default, max),
         {:ok, sort} <- sort(Map.get(opts, "sort"), sorts),
         {:ok, filter} <- filter(Map.get(opts, "filter", []), declared[:filters] || []),
         {:ok, cursor} <- cursor(Map.get(opts, cursor_key), read, sort, cursor_key) do
      {:ok,
       %__MODULE__{
         read: read,
         limit: limit,
         sort: sort,
         direction: Keyword.fetch!(sorts, sort),
         after: cursor,
         filter: filter
       }}
    end
  end

  @doc """
  The page of `rows` (already filtered): sorted by `key` (a function of a row
  answering its sort key) in the page's direction, the rows after its cursor,
  at most `limit` of them, every row counted in `total`.
  """
  @spec slice(Enumerable.t(), t(), (term() -> term())) :: answer()
  def slice(rows, %__MODULE__{} = page, key) do
    # Each row's key is made once; the rows after the cursor are a suffix of
    # the sorted rows.
    sorted = rows |> Enum.map(&{key.(&1), &1}) |> Enum.sort_by(&elem(&1, 0), page.direction)

    sorted
    |> Enum.drop_while(&(not after?(elem(&1, 0), page)))
    |> Enum.map(&elem(&1, 1))
    |> answer(length(sorted), false, page, key)
  end

  @doc """
  One page of `answer` (`Reads.rows/5` of a read whose local half answered
  `slice/3`, one page per controller): the controllers' rows merged newest
  first by `key`, `total` their sum, `unreachable` kept. Only a newest-first
  (`:desc`) page merges, the order `Reads.newest/3` keeps.
  """
  @spec merge(Reads.rows(), t(), (term() -> term())) :: map()
  def merge(%{rows: pages} = answer, %__MODULE__{direction: :desc} = page, key) do
    joined = Enum.flat_map(pages, & &1.rows)
    %{rows: newest} = Reads.newest(%{answer | rows: joined}, key, page.limit)
    more? = length(joined) > page.limit or Enum.any?(pages, & &1.next)
    total = pages |> Enum.map(& &1.total) |> Enum.sum()

    newest
    |> Enum.reverse()
    |> answer(total, more?, page, key)
    |> Map.put(:unreachable, answer.unreachable)
  end

  @doc """
  A filter's list of at most #{@max_limit} items, the bound every list a read
  is asked about shares, or the refusal naming `option`.
  """
  @spec bounded(term(), String.t()) :: {:ok, list()} | {:error, Flezha.Error.t()}
  def bounded(written, _option) when is_list(written) and length(written) <= @max_limit,
    do: {:ok, written}

  def bounded(written, option), do: invalid(option, written)

  @doc "The refusal of a value `option` does not take, as every paged read refuses one."
  @spec invalid(String.t(), term()) :: {:error, Flezha.Error.t()}
  def invalid(option, written),
    do:
      {:error, Flezha.Error.new("usage.invalid_number", option: option, value: inspect(written))}

  @doc """
  The page of `rows`, already in order and after the cursor, for a read that
  fetched no more than it answers: at most `limit` of them, `total` as given,
  and a cursor when rows remain beyond the kept ones or `more?` says so.
  """
  @spec answer([term()], non_neg_integer(), boolean(), t(), (term() -> term())) :: answer()
  def answer(rows, total, more?, page, key) do
    {kept, rest} = Enum.split(rows, page.limit)

    next =
      if kept != [] and (rest != [] or more?),
        do: encode(page.read, page.sort, key.(List.last(kept)))

    %{rows: kept, total: total, next: next, unreachable: []}
  end

  defp after?(_row_key, %__MODULE__{after: nil}), do: true
  defp after?(row_key, %__MODULE__{after: cursor, direction: :asc}), do: row_key > cursor
  defp after?(row_key, %__MODULE__{after: cursor, direction: :desc}), do: row_key < cursor

  defp known(keys, allowed) do
    case Enum.find(keys, &(&1 not in allowed)) do
      nil -> :ok
      unknown -> unknown(unknown)
    end
  end

  defp limit(:error, default, _max), do: {:ok, default}
  defp limit({:ok, n}, _default, max) when is_integer(n) and n >= 0 and n <= max, do: {:ok, n}
  defp limit({:ok, written}, _default, _max), do: invalid("limit", written)

  defp sort(nil, [{default, _direction} | _]), do: {:ok, default}

  defp sort(written, sorts) do
    names = Keyword.keys(sorts)

    case Enum.find(names, &(to_string(&1) == to_string(written))) do
      nil ->
        {:error,
         Flezha.Error.new("usage.unknown_word",
           noun: "sort",
           value: inspect(written),
           allowed: Enum.join(names, ", ")
         )}

      sort ->
        {:ok, sort}
    end
  end

  defp filter(written, keys) when is_list(written) or is_map(written) do
    Enum.reduce_while(written, {:ok, %{}}, fn
      {key, value}, {:ok, acc} ->
        case Enum.find(keys, &(to_string(&1) == to_string(key))) do
          nil -> {:halt, unknown("filter.#{key}")}
          key -> {:cont, {:ok, Map.put(acc, key, value)}}
        end

      other, _acc ->
        {:halt, invalid("filter", other)}
    end)
  end

  defp filter(written, _keys), do: invalid("filter", written)

  defp unknown(key),
    do: {:error, Flezha.Error.new("control.unknown_parameter", parameter: to_string(key))}

  defp cursor(nil, _read, _sort, _option), do: {:ok, nil}

  defp cursor(written, read, sort, option) when is_binary(written) do
    with {:ok, bin} <- Base.url_decode64(written, padding: false),
         {^read, ^sort, key} <- :erlang.binary_to_term(bin, [:safe]) do
      {:ok, key}
    else
      _ -> invalid(option, written)
    end
  rescue
    ArgumentError -> invalid(option, written)
  end

  defp cursor(written, _read, _sort, option), do: invalid(option, written)

  defp encode(read, sort, key),
    do: Base.url_encode64(:erlang.term_to_binary({read, sort, key}), padding: false)
end
