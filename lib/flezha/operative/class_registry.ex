defmodule Flezha.Operative.ClassRegistry do
  @moduledoc """
  Resolves an `operative_class` to its `kind` for the State subsystem.

  `Flezha.State.Retention`, `Flezha.State.Watchdog`,
  `Flezha.Executor.StreamChannel`, and `Flezha.State.TimeTravel` are keyed
  purely by `operative_class` strings and never carry a `kind`. This registry
  is the seam that lets those paths gate the continuous-only capabilities
  (`time_travel?`, `dynamic_segments?`, `output_stall_watch?`) through
  `Flezha.Operative.Kind.capable?/2`.

  `get/1` resolves in three steps:

    1. the in-memory ETS table — populated at `Flezha.Operative.Overwatch`
       bind, evicted at teardown;
    2. a durable per-class `kind` sidecar under
       `<state_root>/<operative_class>/` — so a process-less `Retention` sweep
       (which walks class dirs on disk with no live Overwatch) still resolves
       the kind after teardown, while finished-job state is retained;
    3. the default `:continuous` — every class was continuous before bounded
       operatives gained durable state, so an unknown class keeps the full
       continuous behaviour and gating is only ever *subtractive*.

  The sidecar is written only for non-`:continuous` kinds, so a continuous
  operative's on-disk layout is unchanged. Like `Flezha.State.Completion`, the
  `kind` file is inert to every `Flezha.State.Registry` scanner.
  """

  alias Flezha.Operative.Kind
  alias Flezha.State.Registry

  require Logger

  @table :flezha_operative_class_registry
  @default_kind :continuous
  @sidecar "kind"

  # ---------- table lifecycle ----------

  @doc "Create the registry table if absent. Idempotent and race-tolerant."
  @spec ensure_table() :: :ok
  def ensure_table() do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end

  # ---------- writes ----------

  @doc """
  Record `operative_class`'s `kind`: an in-memory ETS entry plus, for a
  non-`:continuous` kind, a durable sidecar so a process-less `Retention`
  sweep resolves the kind after teardown. Called at `Overwatch` bind.
  """
  @spec put(binary(), atom()) :: :ok
  def put(operative_class, kind) when is_binary(operative_class) and is_atom(kind) do
    ensure_table()
    :ets.insert(@table, {operative_class, kind})
    if kind != @default_kind, do: write_sidecar(operative_class, kind)
    :ok
  end

  @doc """
  Evict the in-memory entry. The durable sidecar is kept — finished-job state
  can outlive its process, and the process-less `Retention` sweep still needs
  the kind. The sidecar is reclaimed with the class dir.
  """
  @spec delete(binary()) :: :ok
  def delete(operative_class) when is_binary(operative_class) do
    if :ets.whereis(@table) != :undefined, do: :ets.delete(@table, operative_class)
    :ok
  end

  # ---------- reads ----------

  @doc "Resolve `operative_class`'s kind: ETS → durable sidecar → `:continuous`."
  @spec get(binary()) :: atom()
  def get(operative_class) when is_binary(operative_class) do
    case ets_lookup(operative_class) do
      {:ok, kind} -> kind
      :error -> read_sidecar(operative_class) || @default_kind
    end
  end

  @doc "Whether `operative_class`'s resolved kind declares `capability`."
  @spec capable?(binary(), Kind.capability()) :: boolean()
  def capable?(operative_class, capability) when is_binary(operative_class) do
    Kind.capable?(get(operative_class), capability)
  end

  # ---------- internals ----------

  defp ets_lookup(operative_class) do
    case :ets.whereis(@table) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(@table, operative_class) do
          [{^operative_class, kind}] -> {:ok, kind}
          [] -> :error
        end
    end
  end

  defp sidecar_path(operative_class),
    do: Path.join([Registry.state_root(), operative_class, @sidecar])

  defp write_sidecar(operative_class, kind) do
    path = sidecar_path(operative_class)
    tmp = path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp, Atom.to_string(kind)),
         :ok <- File.rename(tmp, path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(tmp)

        Logger.warning(
          "[operative] kind sidecar write failed for #{operative_class}: #{inspect(reason)}"
        )

        :ok
    end
  end

  defp read_sidecar(operative_class) do
    case File.read(sidecar_path(operative_class)) do
      {:ok, bin} -> parse_kind(String.trim(bin))
      {:error, _} -> nil
    end
  end

  defp parse_kind(str) do
    kind = String.to_existing_atom(str)
    if Kind.known?(kind), do: kind, else: nil
  rescue
    ArgumentError -> nil
  end
end
