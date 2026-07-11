defmodule Flezha.Operative.ClassRegistry do
  @moduledoc """
  Resolves an `operative_class` to its `kind` — and its effectively-once
  delivery tier — for the State subsystem.

  `Flezha.State.Retention`, `Flezha.State.Watchdog`,
  `Flezha.Executor.StreamChannel`, and `Flezha.State.TimeTravel` are keyed
  purely by `operative_class` strings and never carry a `kind`. This registry
  is the seam that lets those paths gate the unbounded-only capabilities
  (`time_travel?`, `dynamic_segments?`, `output_stall_watch?`) through
  `Flezha.Operative.Kind.capable?/2`.

  `get/1` resolves in three steps:

    1. the in-memory ETS table — populated at `Flezha.Operative.Overwatch`
       bind, evicted at teardown;
    2. a durable per-class `kind` sidecar under
       `<state_root>/<operative_class>/` — so a process-less `Retention` sweep
       (which walks class dirs on disk with no live Overwatch) still resolves
       the kind after teardown, while finished-job state is retained;
    3. the default `:unbounded` — every class was unbounded before bounded
       operatives gained durable state, so an unknown class keeps the full
       unbounded behaviour and gating is only ever *subtractive*.

  The sidecar is written only for non-`:unbounded` kinds, so an unbounded
  operative's on-disk layout is unchanged. Like `Flezha.State.Completion`, the
  `kind` file is inert to every `Flezha.State.Registry` scanner.
  """

  alias Flezha.Operative.Kind
  alias Flezha.State.Registry

  require Logger

  @table :flezha_operative_class_registry
  @default_kind :unbounded
  @sidecar "kind"
  @tier_sidecar "effectively_once"

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
  non-`:unbounded` kind, a durable sidecar so a process-less `Retention`
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
  Record whether `operative_class` runs the effectively-once delivery tier,
  as declared by the SDK in the `AddOperative` response metadata. Same
  ETS + durable-sidecar shape as the kind: the sidecar exists only while
  the tier is on, so a tier-off class's on-disk layout is unchanged and a
  class that drops the declaration stops journaling on the next bind.
  """
  @spec put_effectively_once(binary(), boolean()) :: :ok
  def put_effectively_once(operative_class, tier?)
      when is_binary(operative_class) and is_boolean(tier?) do
    ensure_table()
    :ets.insert(@table, {{operative_class, :effectively_once}, tier?})

    if tier? do
      write_sidecar_file(operative_class, @tier_sidecar, "true")
    else
      _ = File.rm(sidecar_path(operative_class, @tier_sidecar))
      :ok
    end
  end

  @doc """
  Record the live Overwatch pid for `operative_class` (called at bind, from
  the Overwatch process). The State subsystem's fsync-gated outbox flush
  resolves the `:on_output` dispatch target through this — no registry scan.
  """
  @spec put_overwatch(binary(), pid()) :: :ok
  def put_overwatch(operative_class, pid) when is_binary(operative_class) and is_pid(pid) do
    ensure_table()
    :ets.insert(@table, {{operative_class, :overwatch}, pid})
    :ok
  end

  @doc "Resolve the live Overwatch pid for `operative_class`."
  @spec overwatch_pid(binary()) :: {:ok, pid()} | :error
  def overwatch_pid(operative_class) when is_binary(operative_class) do
    case ets_lookup({operative_class, :overwatch}) do
      {:ok, pid} when is_pid(pid) ->
        if Process.alive?(pid), do: {:ok, pid}, else: :error

      _ ->
        :error
    end
  end

  @doc """
  Evict the in-memory entry. The durable sidecar is kept — finished-job state
  can outlive its process, and the process-less `Retention` sweep still needs
  the kind. The sidecar is reclaimed with the class dir.
  """
  @spec delete(binary()) :: :ok
  def delete(operative_class) when is_binary(operative_class) do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table, operative_class)
      :ets.delete(@table, {operative_class, :effectively_once})
      :ets.delete(@table, {operative_class, :overwatch})
    end

    :ok
  end

  # ---------- reads ----------

  @doc "Resolve `operative_class`'s kind: ETS → durable sidecar → `:unbounded`."
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

  @doc """
  Resolve `operative_class`'s effectively-once tier: ETS → durable
  sidecar → `false` (the cost-free default).
  """
  @spec effectively_once?(binary()) :: boolean()
  def effectively_once?(operative_class) when is_binary(operative_class) do
    case ets_lookup({operative_class, :effectively_once}) do
      {:ok, tier?} -> tier?
      :error -> File.exists?(sidecar_path(operative_class, @tier_sidecar))
    end
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

  defp sidecar_path(operative_class, filename \\ @sidecar),
    do: Path.join([Registry.state_root(), operative_class, filename])

  defp write_sidecar(operative_class, kind),
    do: write_sidecar_file(operative_class, @sidecar, Atom.to_string(kind))

  defp write_sidecar_file(operative_class, filename, content) do
    path = sidecar_path(operative_class, filename)
    tmp = path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp, content),
         :ok <- File.rename(tmp, path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(tmp)

        Logger.warning(
          "[operative] #{filename} sidecar write failed for #{operative_class}: #{inspect(reason)}"
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
