defmodule Flezha.Runner.StreamChannel.Prefix do
  @moduledoc """
  Confirming a runner's processed-prefix markers (`durable-input-log`): a
  marker names an instance and an offset `P`, and says every record the
  instance produced for input at or below `P` is ahead of it on this stream.
  `P` is confirmed once every Cell writer of the instance has fsynced past the
  bytes this channel had handed it when the marker arrived.

  Each writer's received and drained bytes are kept only while it owes a
  drain, so a writer that has fsynced everything costs nothing. A marker waits
  behind the stream's fence: records a previous stream handed to the writers
  are fsynced by one batcher flush, which the stream's first marker asks for,
  before any marker of the stream is confirmed. A stream that carries no
  marker asks for none.

  Pure over its own struct; `Flezha.Runner.StreamChannel` performs the effects.
  """

  defstruct outstanding: %{}, markers: %{}, fence: :none

  @type cell_id :: {binary(), binary(), binary()}
  @type t :: %__MODULE__{
          outstanding: %{binary() => %{cell_id() => {non_neg_integer(), non_neg_integer()}}},
          markers: %{binary() => [{non_neg_integer(), %{cell_id() => non_neg_integer()}}]},
          fence: :none | :asked | :done
        }

  @doc "A fresh stream's bookkeeping, unfenced until its first marker's flush lands."
  @spec new() :: t()
  def new(), do: %__MODULE__{}

  @doc "`bytes` of a record for `cell_id` were handed to its writer."
  @spec received(t(), cell_id(), non_neg_integer()) :: t()
  def received(%__MODULE__{} = prefix, {rc, _, _} = cell_id, bytes) do
    writers =
      prefix.outstanding
      |> Map.get(rc, %{})
      |> Map.update(cell_id, {bytes, 0}, fn {received, drained} -> {received + bytes, drained} end)

    %{prefix | outstanding: Map.put(prefix.outstanding, rc, writers)}
  end

  @doc """
  `cell_id`'s writer drained `bytes` (fsynced them, or dropped them unwritten).
  Answers the offsets now confirmed, per instance.
  """
  @spec drained(t(), cell_id(), non_neg_integer()) ::
          {[{binary(), non_neg_integer(), pos_integer()}], t()}
  def drained(%__MODULE__{} = prefix, {rc, _, _} = cell_id, bytes) do
    writers = Map.get(prefix.outstanding, rc, %{})

    prefix =
      case Map.fetch(writers, cell_id) do
        # Everything handed over is durable: every marker waiting on this
        # writer is past it, and a later record starts its count afresh.
        {:ok, {received, drained}} when drained + bytes >= received ->
          prefix
          |> put_writers(rc, Map.delete(writers, cell_id))
          |> forget_positions(rc, cell_id)

        {:ok, {received, drained}} ->
          put_writers(prefix, rc, Map.put(writers, cell_id, {received, drained + bytes}))

        :error ->
          prefix
      end

    confirm(prefix, rc)
  end

  defp put_writers(prefix, rc, writers) when writers == %{},
    do: %{prefix | outstanding: Map.delete(prefix.outstanding, rc)}

  defp put_writers(prefix, rc, writers),
    do: %{prefix | outstanding: Map.put(prefix.outstanding, rc, writers)}

  defp forget_positions(prefix, rc, cell_id) do
    case Map.fetch(prefix.markers, rc) do
      {:ok, pending} ->
        pending =
          Enum.map(pending, fn {offset, streams, positions} ->
            {offset, streams, Map.delete(positions, cell_id)}
          end)

        %{prefix | markers: Map.put(prefix.markers, rc, pending)}

      :error ->
        prefix
    end
  end

  @doc """
  A marker for `rc` at `offset` arrived behind the records it covers on this
  stream, sent on `streams` streams in all. Answers the offsets this stream
  now confirms, per instance, with that count.
  """
  @spec marker(t(), binary(), non_neg_integer(), pos_integer()) ::
          {[{binary(), non_neg_integer(), pos_integer()}], t()}
  def marker(%__MODULE__{} = prefix, rc, offset, streams \\ 1) do
    positions =
      for {cell_id, {received, drained}} <- Map.get(prefix.outstanding, rc, %{}),
          received > drained,
          into: %{},
          do: {cell_id, received}

    pending = Map.get(prefix.markers, rc, []) ++ [{offset, streams, positions}]
    confirm(%{prefix | markers: Map.put(prefix.markers, rc, pending)}, rc)
  end

  @doc """
  Whether the stream's fence flush is still to be asked for, marking it asked:
  the caller asks when this answers `true`.
  """
  @spec ask_fence(t()) :: {boolean(), t()}
  def ask_fence(%__MODULE__{fence: :none} = prefix), do: {true, %{prefix | fence: :asked}}
  def ask_fence(%__MODULE__{} = prefix), do: {false, prefix}

  @doc "The fence flush landed: every earlier stream's record is durable."
  @spec fenced(t()) :: {[{binary(), non_neg_integer(), pos_integer()}], t()}
  def fenced(%__MODULE__{} = prefix) do
    prefix = %{prefix | fence: :done}

    Enum.reduce(Map.keys(prefix.markers), {[], prefix}, fn rc, {confirmed, prefix} ->
      {more, prefix} = confirm(prefix, rc)
      {more ++ confirmed, prefix}
    end)
  end

  # Pop the leading markers every writer has drained past; the last one popped
  # is the instance's confirmed prefix.
  defp confirm(%__MODULE__{fence: fence} = prefix, _rc) when fence != :done, do: {[], prefix}

  defp confirm(%__MODULE__{} = prefix, rc) do
    writers = Map.get(prefix.outstanding, rc, %{})

    {done, waiting} =
      prefix.markers
      |> Map.get(rc, [])
      |> Enum.split_while(fn {_offset, _streams, positions} ->
        drained_past?(writers, positions)
      end)

    markers =
      if waiting == [],
        do: Map.delete(prefix.markers, rc),
        else: Map.put(prefix.markers, rc, waiting)

    confirmed =
      case List.last(done) do
        nil -> []
        {offset, streams, _positions} -> [{rc, offset, streams}]
      end

    {confirmed, %{prefix | markers: markers}}
  end

  defp drained_past?(writers, positions) do
    Enum.all?(positions, fn {cell_id, position} ->
      match?({:ok, {_received, drained}} when drained >= position, Map.fetch(writers, cell_id))
    end)
  end
end
