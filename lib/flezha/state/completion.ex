defmodule Flezha.State.Completion do
  @moduledoc """
  The per-operative *completion record* of a finished bounded operative.

  On terminal teardown (status `ok | error | cancelled`) the controller writes
  a small durable term alongside the operative's retained Entity state, so the
  outcome stays inspectable via `flz operative info` until the retention window
  elapses (`Flezha.State.Retention` sweeps both).

  The record lives at `<state_root>/<operative_class>/completion.record` — a
  file sibling to the procedure directories, inert to every `Flezha.State.Registry`
  scanner (which only recognise `.wal` / `.snap.<seq>` / `.outbox.dedup`). Its
  presence is also the retain-vs-delete signal for `stop_operative`.
  """

  require Logger

  alias Flezha.State.Registry

  @filename "completion.record"

  @type t :: %{
          namespace: binary(),
          status: binary(),
          error: binary(),
          result: binary(),
          finished_at: non_neg_integer()
        }

  @spec path(binary()) :: binary()
  def path(operative_class),
    do: Path.join([Registry.state_root(), operative_class, @filename])

  @spec exists?(binary()) :: boolean()
  def exists?(operative_class), do: File.exists?(path(operative_class))

  @doc """
  Atomically write the completion record (`.tmp` → rename, so a reader never
  sees a partial record). Best-effort: logs and returns `:ok` on failure so a
  teardown never crashes on a record write.
  """
  @spec write(binary(), t()) :: :ok
  def write(operative_class, %{} = record) when is_binary(operative_class) do
    dir = Path.join(Registry.state_root(), operative_class)
    final = Path.join(dir, @filename)
    tmp = final <> ".tmp"

    with :ok <- File.mkdir_p(dir),
         :ok <- File.write(tmp, :erlang.term_to_binary(record)),
         :ok <- File.rename(tmp, final) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(tmp)

        Logger.warning(
          "[state] completion record write failed for #{operative_class}: #{inspect(reason)}"
        )

        :ok
    end
  end

  @spec read(binary()) :: {:ok, t()} | :error
  def read(operative_class) when is_binary(operative_class) do
    case File.read(path(operative_class)) do
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
  def delete(operative_class) when is_binary(operative_class) do
    _ = File.rm(path(operative_class))
    :ok
  end
end
