defmodule Flezha.Process.Watch do
  @moduledoc """
  The monitor bookkeeping every process that watches another one writes by
  hand: a `Process.monitor/1` per pid, a `%{ref => pid}` map to read the
  `:DOWN` back, and a demonitor-and-flush when the last interest in that pid
  goes away.

  A watcher declares the TAGS it keyed on a pid — a stream ref, a relay
  id, a subscription — and gets them back when the pid dies. The REACTION
  stays with the watcher: this module never decides what a death means.

  One monitor per pid however many tags are on it, and `demonitor(ref,
  [:flush])` on the last one, so a `:DOWN` already in the mailbox for a pid
  nobody watches is dropped rather than delivered to a handler with nothing
  left to key it on.
  """

  @type tag :: term()
  @opaque t :: %__MODULE__{
            watched: %{pid() => {reference(), MapSet.t()}},
            refs: %{reference() => pid()}
          }

  # Keyed both ways: a watcher reaches its tags by pid when it adds or drops
  # one, and by ref when the `:DOWN` arrives carrying nothing else.
  defstruct watched: %{}, refs: %{}

  @spec new() :: t()
  def new(), do: %__MODULE__{}

  @doc "Watch `pid` under `tag`, monitoring it on the first tag."
  @spec watch(t(), pid(), tag()) :: t()
  def watch(%__MODULE__{} = set, pid, tag) when is_pid(pid) do
    case Map.fetch(set.watched, pid) do
      {:ok, {ref, tags}} ->
        %{set | watched: Map.put(set.watched, pid, {ref, MapSet.put(tags, tag)})}

      :error ->
        ref = Process.monitor(pid)

        %{
          set
          | watched: Map.put(set.watched, pid, {ref, MapSet.new([tag])}),
            refs: Map.put(set.refs, ref, pid)
        }
    end
  end

  @doc "Drop `tag` from `pid`, demonitoring when it was the last one."
  @spec unwatch(t(), pid(), tag()) :: t()
  def unwatch(%__MODULE__{} = set, pid, tag) do
    case Map.fetch(set.watched, pid) do
      {:ok, {ref, tags}} -> drop(set, pid, ref, MapSet.delete(tags, tag))
      :error -> set
    end
  end

  @doc """
  Drop every tag on `pid` and demonitor. For a watcher that learns the pid is
  gone from somewhere other than the `:DOWN` — a close, an unbind, a replace.
  """
  @spec forget(t(), pid()) :: {[tag()], t()}
  def forget(%__MODULE__{} = set, pid) do
    case Map.fetch(set.watched, pid) do
      {:ok, {ref, tags}} -> {MapSet.to_list(tags), drop(set, pid, ref, MapSet.new())}
      :error -> {[], set}
    end
  end

  @doc """
  The tags keyed on the pid whose `:DOWN` carried `ref`, and the set without
  it. `:unknown` for a ref this set never took — a stale `:DOWN` from a
  monitor the watcher had already flushed.
  """
  @spec down(t(), reference()) :: {[tag()] | :unknown, t()}
  def down(%__MODULE__{} = set, ref) when is_reference(ref) do
    case Map.pop(set.refs, ref) do
      {nil, _refs} ->
        {:unknown, set}

      {pid, refs} ->
        {{_ref, tags}, watched} = Map.pop(set.watched, pid, {ref, MapSet.new()})
        {MapSet.to_list(tags), %{set | watched: watched, refs: refs}}
    end
  end

  @doc "The tags currently keyed on `pid`."
  @spec tags(t(), pid()) :: [tag()]
  def tags(%__MODULE__{} = set, pid) do
    case Map.fetch(set.watched, pid) do
      {:ok, {_ref, tags}} -> MapSet.to_list(tags)
      :error -> []
    end
  end

  @doc "Every pid this set is watching."
  @spec pids(t()) :: [pid()]
  def pids(%__MODULE__{} = set), do: Map.keys(set.watched)

  @doc "True when `pid` is watched under at least one tag."
  @spec watching?(t(), pid()) :: boolean()
  def watching?(%__MODULE__{} = set, pid), do: Map.has_key?(set.watched, pid)

  # The last tag on a pid takes its monitor with it: demonitor-and-flush so a
  # `:DOWN` already queued cannot reach a handler with no tags left to make
  # sense of it.
  defp drop(set, pid, ref, tags) do
    if MapSet.size(tags) == 0 do
      Process.demonitor(ref, [:flush])
      %{set | watched: Map.delete(set.watched, pid), refs: Map.delete(set.refs, ref)}
    else
      %{set | watched: Map.put(set.watched, pid, {ref, tags})}
    end
  end
end
