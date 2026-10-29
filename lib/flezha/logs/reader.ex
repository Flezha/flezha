defmodule Flezha.Logs.Reader do
  @moduledoc """
  Streaming reader for a kind's JSON Lines files, run in the caller's process,
  never the writer's.

  `stream/2` reads a file oldest first, a `.jsonl.gz` file through streaming
  decompression. `reverse_stream/2` reads it newest first: a plain file in
  64 KiB blocks from its end, so a read that wants the newest few records
  never reads the rest; a gzipped file forward, keeping only the newest
  `limit` matches. Either yields `{offset, record}` with `offsets: true`
  (always for `reverse_stream/2`), `offset` the line's byte position, which
  tells two records of one file apart.

  `matcher/3` builds the `match:` predicate both take: equality on top-level
  fields, and the record's order time (`Flezha.Logs.Codec.order_us/1`) within
  `since_us` and `until_us`. A truncated last line (no trailing `\\n`) is
  skipped.
  """

  alias Flezha.Logs.Codec

  @block 65_536

  @doc "A predicate on a record: every `filters` field equal, its time inside the window."
  @spec matcher(map(), integer() | nil, integer() | nil) :: (map() -> boolean())
  def matcher(filters, since_us, until_us) do
    filters = Enum.reject(filters, fn {_field, value} -> is_nil(value) end)

    fn record ->
      Enum.all?(filters, fn {field, value} -> Map.get(record, to_string(field)) == value end) and
        within?(Codec.order_us(record), since_us, until_us)
    end
  end

  defp within?(nil, _since, _until), do: true
  defp within?(ts, since, _until) when is_integer(since) and ts < since, do: false
  defp within?(ts, _since, until) when is_integer(until) and ts > until, do: false
  defp within?(_ts, _since, _until), do: true

  @doc "The records of `path` oldest first, through `match:` and up to `limit:`."
  @spec stream(String.t(), keyword()) :: Enumerable.t()
  def stream(path, opts \\ []) when is_binary(path) do
    stream = Stream.resource(fn -> open(path) end, &next/1, &close/1)
    match = Keyword.get(opts, :match, fn _ -> true end)

    stream
    |> Stream.filter(fn {_offset, record} -> match.(record) end)
    |> then(&if(limit = opts[:limit], do: Stream.take(&1, limit), else: &1))
    |> then(&if(opts[:offsets], do: &1, else: Stream.map(&1, fn {_offset, r} -> r end)))
  end

  @doc """
  The `{offset, record}` pairs of `path` newest first, through `match:`; a
  gzipped file keeps the newest `limit:` matches.
  """
  @spec reverse_stream(String.t(), keyword()) :: Enumerable.t()
  def reverse_stream(path, opts \\ []) when is_binary(path) do
    match = Keyword.get(opts, :match, fn _ -> true end)

    if String.ends_with?(path, ".gz") do
      path
      |> stream(match: match, offsets: true)
      |> Enum.reduce({0, []}, &keep_newest(&1, &2, opts[:limit]))
      |> elem(1)
    else
      path
      |> blocks_from_end()
      |> Stream.filter(fn {_offset, record} -> match.(record) end)
    end
  end

  defp keep_newest(pair, {n, kept}, limit) when is_integer(limit) and n >= limit,
    do: {n, [pair | Enum.take(kept, limit - 1)]}

  defp keep_newest(pair, {n, kept}, _limit), do: {n + 1, [pair | kept]}

  @doc """
  The first and last order times of a file and its record count, read end to
  end so the count is exact: `{first_us, last_us, count}`.
  """
  @spec head_metadata(String.t()) :: {integer() | nil, integer() | nil, non_neg_integer()}
  def head_metadata(path) do
    path
    |> stream()
    |> Enum.reduce({nil, nil, 0}, fn record, {first, _last, n} ->
      ts = Codec.order_us(record)
      {first || ts, ts, n + 1}
    end)
  end

  # ---------- newest first, by blocks ----------

  defp blocks_from_end(path) do
    Stream.resource(
      fn -> open_end(path) end,
      &previous_block/1,
      fn %{fd: fd} -> if fd, do: :file.close(fd) end
    )
  end

  defp open_end(path) do
    case File.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        {:ok, size} = :file.position(fd, :eof)
        %{fd: fd, pos: size, carry: "", tail?: true}

      {:error, _} ->
        %{fd: nil, pos: 0, carry: "", tail?: false}
    end
  end

  defp previous_block(%{pos: 0} = state), do: {:halt, state}

  defp previous_block(%{fd: fd, pos: pos} = state) do
    start = max(pos - @block, 0)
    {:ok, block} = :file.pread(fd, start, pos - start)
    lines(block <> state.carry, start, %{state | pos: start})
  end

  # The bytes after the last newline are a truncated line, skipped until a
  # block holds a newline; the bytes before a block's first newline belong to
  # the line the next block ends.
  defp lines(buf, start, %{tail?: true} = state) do
    case :binary.matches(buf, "\n") do
      [] when start > 0 ->
        {[], %{state | carry: ""}}

      [] ->
        {[], state}

      found ->
        lines(binary_part(buf, 0, elem(List.last(found), 0) + 1), start, %{state | tail?: false})
    end
  end

  defp lines(buf, start, state) do
    [head | rest] = :binary.split(buf, "\n", [:global])
    {complete, carry} = if start > 0, do: {rest, head}, else: {[head | rest], ""}
    first_offset = if start > 0, do: start + byte_size(head) + 1, else: 0

    {pairs, _offset} =
      Enum.reduce(complete, {[], first_offset}, fn line, {acc, offset} ->
        acc = if line == "", do: acc, else: decoded(acc, offset, line)
        {acc, offset + byte_size(line) + 1}
      end)

    {pairs, %{state | carry: carry}}
  end

  defp decoded(acc, offset, line) do
    case Codec.decode(line) do
      {:ok, record} -> [{offset, record} | acc]
      {:error, _} -> acc
    end
  end

  # ---------- oldest first ----------

  defp open(path) do
    case File.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        z = if String.ends_with?(path, ".gz"), do: inflater()
        %{fd: fd, z: z, buffer: "", offset: 0, eof?: false}

      {:error, _reason} ->
        %{fd: nil, z: nil, buffer: "", offset: 0, eof?: true}
    end
  end

  defp inflater() do
    z = :zlib.open()
    :zlib.inflateInit(z, 31)
    z
  end

  defp next(%{fd: nil} = state), do: {:halt, state}

  defp next(state) do
    case extract_line(state) do
      {:line, line, offset, state} ->
        case Codec.decode(line) do
          {:ok, record} -> {[{offset, record}], state}
          {:error, _} -> next(state)
        end

      {:eof, state} ->
        {:halt, state}
    end
  end

  defp close(%{fd: fd, z: z}) do
    if z do
      _ = safely(fn -> :zlib.inflateEnd(z) end)
      _ = safely(fn -> :zlib.close(z) end)
    end

    if fd, do: :file.close(fd)
    :ok
  end

  defp extract_line(state) do
    case :binary.split(state.buffer, "\n") do
      [line, rest] ->
        {:line, line, state.offset,
         %{state | buffer: rest, offset: state.offset + byte_size(line) + 1}}

      [_partial] when state.eof? ->
        {:eof, state}

      [_partial] ->
        case fill(state) do
          {:ok, state} -> extract_line(state)
          {:eof, state} -> extract_line(%{state | eof?: true})
        end
    end
  end

  defp fill(%{fd: fd, z: z} = state) do
    case :file.read(fd, @block) do
      {:ok, chunk} when z == nil ->
        {:ok, %{state | buffer: state.buffer <> chunk}}

      {:ok, chunk} ->
        {:ok, %{state | buffer: state.buffer <> IO.iodata_to_binary(:zlib.inflate(z, chunk))}}

      _eof_or_error when z == nil ->
        {:eof, state}

      _eof_or_error ->
        flush(state)
    end
  end

  # What the decompressor still holds once the file is read.
  defp flush(%{z: z} = state) do
    case safely(fn -> IO.iodata_to_binary(:zlib.inflate(z, <<>>)) end) do
      tail when is_binary(tail) and tail != "" ->
        {:ok, %{state | buffer: state.buffer <> tail, eof?: true}}

      _ ->
        {:eof, state}
    end
  end

  defp safely(fun) do
    fun.()
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end
end
