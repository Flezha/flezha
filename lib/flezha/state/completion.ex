defmodule Flezha.State.Completion do
  @moduledoc """
  The per-operator *completion record* of a finished steps operator.

  On terminal teardown (status `ok | error | cancelled`) the controller writes
  a small durable term alongside the operator's retained Cell state, so the
  outcome stays inspectable via `flz operator info` until the retention window
  elapses (`Flezha.State.Retention` sweeps both).

  The record lives at
  `<state_root>/<namespace>/<operator_class>/completion.record` — a file
  sibling to the tasklet directories, inert to every
  `Flezha.State.Paths` scanner (which only recognise `.wal` /
  `.snap.<seq>` / `.outbox.dedup`). Its
  presence is also the retain-vs-delete signal for `stop_operator`.
  """

  require Logger

  alias Flezha.State.Paths

  @filename "completion.record"

  @type t :: %{
          namespace: binary(),
          run: binary(),
          status: binary(),
          error: binary(),
          result: binary(),
          finished_at: non_neg_integer()
        }

  @spec path(binary()) :: binary()
  def path(operator_class),
    do: Path.join(Paths.class_dir(operator_class), @filename)

  @spec exists?(binary()) :: boolean()
  def exists?(operator_class), do: File.exists?(path(operator_class))

  @doc """
  Atomically write the completion record (`.tmp` → rename, so a reader never
  sees a partial record). Best-effort: logs and returns `:ok` on failure so a
  teardown never crashes on a record write.
  """
  @spec write(binary(), t()) :: :ok
  def write(operator_class, %{} = record) when is_binary(operator_class) do
    final = Path.join(Paths.class_dir(operator_class), @filename)
    bytes = :erlang.term_to_binary(record)

    case Paths.atomic_install(final, &:file.write(&1, bytes), :completion) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "[state] completion record write failed for #{operator_class}: #{inspect(reason)}"
        )

        :ok
    end
  end

  @spec read(binary()) :: {:ok, t()} | :error
  def read(operator_class) when is_binary(operator_class) do
    case File.read(path(operator_class)) do
      {:ok, bin} ->
        try do
          {:ok, :erlang.binary_to_term(bin, [:safe])}
        rescue
          _ -> :error
        end

      {:error, _} ->
        :error
    end
  end

  @spec delete(binary()) :: :ok
  def delete(operator_class) when is_binary(operator_class) do
    _ = File.rm(path(operator_class))
    :ok
  end
end
