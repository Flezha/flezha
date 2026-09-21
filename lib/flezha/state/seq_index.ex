defmodule Flezha.State.SeqIndex do
  @moduledoc """
  A sparse seq→file-position index over a record log, plus where the file
  ends: one sample every `@every` records, enough to find where any seq
  starts within one span instead of walking the file. Both record logs — the
  Cell WAL and the input log — keep one, so a cut at either end costs the
  records on the kept side of the boundary, never the whole file, in the loop
  the data plane waits on.

  Pure state, and no file I/O: a log that opens a file starts from `new/1` with
  the file's size and notes every record it appends; a prefix cut rebases the
  samples, a suffix cut keeps the ones below the boundary. Records written
  before a restart are not sampled, so the first cut after one walks from the
  start.
  """

  @every 256

  defstruct samples: :queue.new(), end: 0

  @type t :: %__MODULE__{
          samples: :queue.queue({non_neg_integer(), non_neg_integer()}),
          end: non_neg_integer()
        }

  @doc "An empty index over a file of `size` bytes."
  @spec new(non_neg_integer()) :: t()
  def new(size \\ 0), do: %__MODULE__{end: size}

  @doc "Record `seq` as the next `bytes` of the file. Sampled — most calls move only the end."
  @spec note(t(), non_neg_integer(), non_neg_integer()) :: t()
  def note(%__MODULE__{samples: samples, end: at} = index, seq, bytes) do
    samples = if rem(seq, @every) == 0, do: :queue.in({seq, at}, samples), else: samples
    %{index | samples: samples, end: at + bytes}
  end

  @doc """
  The file position of the newest sample at or below `floor` — where a scan for
  the first record with seq >= `floor` starts, at most one span away from it.
  """
  @spec sample_at(t(), non_neg_integer()) :: non_neg_integer()
  def sample_at(%__MODULE__{samples: samples}, floor) do
    {_seq, start} =
      samples
      |> :queue.to_list()
      |> Enum.take_while(fn {seq, _} -> seq <= floor end)
      |> List.last({0, 0})

    start
  end

  @doc """
  The index after `cut` bytes left the front of the file at `floor`: samples
  below the floor went with them, the rest moved up by `cut`.
  """
  @spec after_cut(t(), non_neg_integer(), non_neg_integer()) :: t()
  def after_cut(%__MODULE__{samples: samples, end: at}, floor, cut) do
    samples =
      samples
      |> :queue.to_list()
      |> Enum.drop_while(fn {seq, _} -> seq < floor end)
      |> Enum.map(fn {seq, position} -> {seq, position - cut} end)
      |> :queue.from_list()

    %__MODULE__{samples: samples, end: at - cut}
  end

  @doc """
  The index after the file was truncated at `bound`, the start of the first
  record with seq >= `floor`: the samples below the floor are untouched.
  """
  @spec cut_above(t(), non_neg_integer(), non_neg_integer()) :: t()
  def cut_above(%__MODULE__{samples: samples}, floor, bound) do
    samples = samples |> :queue.to_list() |> Enum.take_while(fn {seq, _} -> seq < floor end)
    %__MODULE__{samples: :queue.from_list(samples), end: bound}
  end
end
