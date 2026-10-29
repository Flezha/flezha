defmodule Flezha.Logs.Local do
  @moduledoc """
  One controller's half of every logs read: `Flezha.Service.Logs` asks each
  controller through `Flezha.Cluster.Reads` and merges what they answer.
  Each function names its kind by id, so nothing but data crosses the wire,
  and answers empty for a kind that does not run here.

  `read/3` walks the active file, then the rotated files newest first, skips a
  file whose catalogued time range lies wholly outside the window, and stops
  once it holds one more match than the page asks, so "the newest ten" never
  opens a file older than the tenth match. A row is `{key, record}`, `key`
  being `{order_us, node, file, offset}`, the order the page's cursor is in.
  """

  alias Flezha.Cluster.Page
  alias Flezha.Logs.{Codec, Kind, Kinds, Reader, Seen, Writer}

  @typedoc """
  A read's narrowing: equality `filters` on record fields, the time window,
  and `namespace`, the only namespace answered, or `:all`.
  """
  @type query :: %{
          filters: map(),
          since_us: integer() | nil,
          until_us: integer() | nil,
          namespace: String.t() | :all
        }

  @doc "The page of `kind_id`'s records `query` matches, newest first."
  @spec read(atom(), query(), Page.t()) :: Page.answer()
  def read(kind_id, query, %Page{} = page) do
    rows =
      case running(kind_id) do
        {:ok, kind} -> collect(kind, query, page)
        :error -> []
      end

    Page.answer(rows, length(rows), false, page, &key/1)
  end

  @doc "The page sort key of a row `read/3` answers."
  @spec key({tuple(), map()}) :: tuple()
  def key({key, _record}), do: key

  @doc """
  The records of one of `kind_id`'s files oldest first, at most `limit` of
  them, or `{:error, {:file_not_found, file}}`. Only a file of the kind's
  directory itself is read: a name that walks out of it is answered missing.
  """
  @spec read_file(atom(), String.t(), query(), pos_integer()) :: [map()] | {:error, term()}
  def read_file(kind_id, file, query, limit) do
    with {:ok, kind} <- running(kind_id),
         path = Path.join(Kind.dir(kind), file),
         true <- Path.basename(file) == file and file not in [".", ".."] and File.regular?(path) do
      path |> Reader.stream(match: matcher(query), limit: limit) |> Enum.map(&with_node/1)
    else
      _ -> {:error, {:file_not_found, Path.basename(file)}}
    end
  end

  @doc "`kind_id`'s rotated files here."
  @spec files(atom()) :: [map()]
  def files(kind_id) do
    case running(kind_id) do
      {:ok, kind} -> Enum.map(Writer.catalog(kind), &Map.delete(&1, :clock))
      :error -> []
    end
  end

  @doc "`kind_id`'s operational snapshot here, or `[]` when it does not run."
  @spec stats(atom()) :: map() | []
  def stats(kind_id) do
    with {:ok, kind} <- running(kind_id) do
      snapshot = Writer.stats(kind)

      snapshot
      |> Map.take(~w(current_file current_file_bytes oldest_ts_us fsync_mode closed
                     dropped_count timeout_count post_close_dropped)a)
      |> Map.merge(%{
        total_bytes: snapshot.catalog_total_bytes + snapshot.current_file_bytes,
        total_files: snapshot.catalog_count + 1,
        record_count: snapshot.current_record_count
      })
    else
      :error -> []
    end
  end

  @doc "Rotate `kind_id` here: the file it rotated to, or the reason it did not."
  @spec rotate(atom()) :: map() | []
  def rotate(kind_id) do
    with {:ok, kind} <- running(kind_id) do
      case Writer.rotate_now(kind) do
        {:ok, info} -> %{rotated_to: info.rotated_to}
        {:error, reason} -> %{rotated_to: nil, error: inspect(reason)}
      end
    else
      :error -> []
    end
  end

  @doc "`Flezha.Logs.RelayHistory.seen/2` on this controller alone."
  @spec seen(String.t(), Seen.key()) :: Seen.answer() | nil
  def seen(namespace, key), do: Seen.lookup(namespace, key)

  defp running(kind_id) do
    with {:ok, kind} <- Kinds.fetch(kind_id),
         true <- Kinds.enabled?(kind) do
      {:ok, kind}
    else
      _ -> :error
    end
  end

  # One match more than the page keeps tells the page another one follows.
  defp collect(kind, query, page) do
    dir = Kind.dir(kind)
    want = page.limit + 1
    upper = upper(query.until_us, page.after)
    match = matcher(query)

    Enum.reduce_while(files_newest_first(kind, query.since_us, upper), [], fn file, acc ->
      found =
        dir
        |> Path.join(file)
        |> Reader.reverse_stream(match: match, limit: want)
        |> Stream.map(fn {offset, record} -> row(record, file, offset) end)
        |> Stream.filter(&before?(&1, page.after))
        |> Enum.take(want - length(acc))

      acc = acc ++ found
      if length(acc) >= want, do: {:halt, acc}, else: {:cont, acc}
    end)
    |> Enum.sort_by(&key/1, :desc)
  end

  defp files_newest_first(kind, since_us, upper) do
    rotated =
      kind
      |> Writer.catalog()
      |> Enum.reject(&outside?(&1, since_us, upper))
      |> Enum.sort_by(&(&1.last_ts_us || 0), :desc)
      |> Enum.map(& &1.filename)

    [Kind.active(kind) | rotated]
  end

  defp outside?(%{clock: :wall, first_ts_us: first, last_ts_us: last}, since, upper)
       when is_integer(first) and is_integer(last),
       do: (is_integer(since) and last < since) or (is_integer(upper) and first > upper)

  defp outside?(_uncatalogued_range, _since, _upper), do: false

  defp upper(until_us, nil), do: until_us
  defp upper(nil, {ts, _node, _file, _offset}), do: ts
  defp upper(until_us, {ts, _node, _file, _offset}), do: min(until_us, ts)

  defp before?(_row, nil), do: true
  defp before?({key, _record}, cursor), do: key < cursor

  defp row(record, file, offset) do
    record = with_node(record)
    {{Codec.order_us(record), record["node"], file, offset}, record}
  end

  defp with_node(record), do: Map.put_new(record, "node", Atom.to_string(node()))

  defp matcher(query) do
    filters =
      if query.namespace == :all,
        do: query.filters,
        else: Map.put(query.filters, "namespace", query.namespace)

    Reader.matcher(filters, query.since_us, query.until_us)
  end
end
