defmodule Flezha.State.InputLog.Effects do
  @moduledoc """
  An input log's effect log, `effects.log` beside `input.log`: the results its
  instance's `ctx.effect` calls kept, `offset => %{identity => {cell, payload}}`,
  the open file they are appended to in the `WalWriter` framing, and the next
  record seq. Records are numbered in append order, not by offset: one input
  can keep many results.

  The input log's server owns one, and syncs and answers; this module is the
  data and the file.
  """

  alias Flezha.SDK.RecordFrame, as: Frame
  alias Flezha.State.{Paths, RecordFrame}

  defstruct [:path, :file, results: %{}, seq: 0]

  @type t :: %__MODULE__{
          path: Path.t(),
          file: :file.io_device(),
          results: %{non_neg_integer() => %{binary() => {binary(), binary()}}},
          seq: non_neg_integer()
        }

  @typedoc "One kept result, flat."
  @type result :: {non_neg_integer(), binary(), binary(), binary()}

  @doc """
  Open `path`, creating it, and read its results back. A record it cannot read
  is `{:error, refusal}`, as `input.log` refuses.
  """
  @spec open(Path.t()) :: {:ok, t()} | {:error, term()}
  def open(path) do
    {:ok, file} = :file.open(path, [:raw, :read, :write, :binary])

    case RecordFrame.scan_for_last_valid_seq(file, path) do
      {:ok, last} ->
        {:ok, _eof} = :file.position(file, :eof)
        {:ok, %__MODULE__{path: path, file: file, results: read(file, path), seq: last + 1}}

      {:error, refusal} ->
        _ = :file.close(file)
        {:error, refusal}
    end
  end

  defp read(file, path) do
    {:ok, results} =
      RecordFrame.fold_records(file, path, %{}, fn {_seq, _header, body}, acc ->
        {:cont, put(acc, :erlang.binary_to_term(body))}
      end)

    results
  end

  @doc """
  What keeping `payload` under `identity` at `offset` meets: `:new`, the same
  payload kept (`:kept`), another kept first (`{:first, payload}`), or an
  input already holding `max` results (`:full`). `nil` is a log not opened.
  """
  @spec judge(t() | nil, non_neg_integer(), binary(), binary(), pos_integer()) ::
          :new | :kept | {:first, binary()} | :full
  def judge(nil, _offset, _identity, _payload, _max), do: :new

  def judge(%__MODULE__{results: results}, offset, identity, payload, max) do
    case results do
      %{^offset => %{^identity => {_cell, ^payload}}} -> :kept
      %{^offset => %{^identity => {_cell, first}}} -> {:first, first}
      %{^offset => kept} when map_size(kept) >= max -> :full
      _new -> :new
    end
  end

  @doc "Write one result after the last, framed under `header`; not yet synced."
  @spec append(t(), result(), binary()) :: t()
  def append(%__MODULE__{} = effects, result, header) do
    :ok = :file.write(effects.file, Frame.encode_record(effects.seq, header, encode(result)))
    %{effects | results: put(effects.results, result), seq: effects.seq + 1}
  end

  @doc "The kept results for inputs above `above`, in offset then identity order."
  @spec above(t() | nil, integer()) :: [result()]
  def above(nil, _above), do: []

  def above(%__MODULE__{results: results}, above),
    do: results |> flat() |> Enum.filter(&(elem(&1, 0) > above))

  @doc """
  Drop the results of the inputs below `floor`: the file is rewritten with what
  is left, synced, and swapped in, its records under `header`. Nothing below is
  nothing done.
  """
  @spec drop_below(t() | nil, non_neg_integer(), binary()) :: t() | nil
  def drop_below(nil, _floor, _header), do: nil

  def drop_below(%__MODULE__{} = effects, floor, header) do
    case Map.split_with(effects.results, fn {offset, _kept} -> offset < floor end) do
      {dropped, _kept} when map_size(dropped) == 0 ->
        effects

      {_dropped, kept} ->
        records =
          for {result, seq} <- Enum.with_index(flat(kept)),
              do: Frame.encode_record(seq, header, encode(result))

        :ok = Paths.atomic_install(effects.path, &:file.write(&1, records), :effects_log)
        _ = :file.close(effects.file)
        {:ok, file} = :file.open(effects.path, [:raw, :read, :write, :binary])
        {:ok, _eof} = :file.position(file, :eof)
        %{effects | file: file, results: kept, seq: length(records)}
    end
  end

  @doc "Close the file, if open."
  @spec close(t() | nil) :: :ok
  def close(nil), do: :ok

  def close(%__MODULE__{file: file}) do
    _ = :file.close(file)
    :ok
  end

  defp encode(result), do: :erlang.term_to_binary(result)

  defp put(results, {offset, identity, cell, payload}),
    do:
      Map.update(
        results,
        offset,
        %{identity => {cell, payload}},
        &Map.put(&1, identity, {cell, payload})
      )

  defp flat(results) do
    for {offset, kept} <- Enum.sort(results),
        {identity, {cell, payload}} <- Enum.sort(kept),
        do: {offset, identity, cell, payload}
  end
end
