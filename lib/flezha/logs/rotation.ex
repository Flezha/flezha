defmodule Flezha.Logs.Rotation do
  @moduledoc """
  One kind's periodic rotation check.

  Every `rotation_check_interval_ms` the timer reads the writer's stats; when
  the active file has reached `max_file_bytes` or `max_file_age_seconds` and
  holds a record, it rotates it. The writer announces the new file itself.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:tick]

  alias Flezha.Process.Cspec
  alias Flezha.Config.Durations
  alias Flezha.Logs.{Kind, Writer}

  def child_spec(%Kind{} = kind),
    do: Cspec.build({__MODULE__, kind.id}, {__MODULE__, :start_link, [kind]}, :worker_permanent)

  def start_link(%Kind{} = kind),
    do: GenServer.start_link(__MODULE__, kind, name: Kind.name(kind, :rotation))

  @doc "Rotate `kind` now, whatever its size and age."
  @spec rotate_now(Kind.t()) :: {:ok, map()} | {:error, term()}
  def rotate_now(%Kind{} = kind),
    do:
      GenServer.call(
        Kind.name(kind, :rotation),
        :rotate_now,
        Durations.get(:audit_log_rotate_call_ms)
      )

  @impl true
  def init(kind), do: {:ok, arm_tick(%{kind: kind})}

  @impl true
  def handle_call(:rotate_now, _from, state), do: {:reply, do_rotate(state.kind), state}

  @impl true
  def handle_info(:tick, state) do
    maybe_rotate(state.kind)
    {:noreply, arm_tick(state)}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp maybe_rotate(kind) do
    case safe_stats(kind) do
      {:ok, %{closed: false, current_record_count: count} = stats} when count > 0 ->
        age_s = div(System.system_time(:millisecond) - stats.opened_at_ms, 1_000)

        if stats.current_file_bytes >= Kind.get(kind, :max_file_bytes) or
             age_s >= Kind.get(kind, :max_file_age_seconds),
           do: do_rotate(kind)

      _closed_empty_or_down ->
        :ok
    end
  end

  defp safe_stats(kind) do
    {:ok, Writer.stats(kind)}
  catch
    :exit, _ -> :error
  end

  defp do_rotate(kind) do
    Writer.rotate_now(kind)
  catch
    :exit, reason -> {:error, {:writer_unavailable, reason}}
  end

  defp arm_tick(state),
    do: arm_timer(state, :tick, Kind.get(state.kind, :rotation_check_interval_ms), :tick)
end
