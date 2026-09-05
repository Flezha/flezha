defmodule Flezha.Observability.AuditLog.Rotation do
  @moduledoc """
  Periodic rotation check.

  Every `:rotation_check_interval_ms` (default `60_000`) the timer
  queries `Writer.stats/0`; when either the active size exceeds
  `:max_file_bytes` or the file age exceeds
  `:max_file_age_seconds`, it calls `Writer.rotate_now/1`.

  The Writer emits `:audit_rotated` itself after a successful rename.
  """

  use GenServer

  alias Flezha.Config
  alias Flezha.Observability.AuditLog.Writer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Trigger an immediate rotation regardless of size/age."
  @spec rotate_now() :: {:ok, map()} | {:error, term()}
  def rotate_now() do
    GenServer.call(__MODULE__, :rotate_now, 10_000)
  end

  @impl true
  def init(_opts) do
    state = %{
      interval_ms: Config.get(:audit_log_rotation_check_interval_ms),
      max_file_bytes: Config.get(:audit_log_max_file_bytes),
      max_file_age_seconds: Config.get(:audit_log_max_file_age_seconds)
    }

    schedule(state.interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:rotate_now, _from, state) do
    {:reply, do_rotate(), state}
  end

  @impl true
  def handle_info(:tick, state) do
    maybe_rotate(state)
    schedule(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp maybe_rotate(state) do
    case safe_stats() do
      {:ok, %{closed: true}} ->
        :ok

      {:ok, stats} ->
        age_s = div(System.system_time(:millisecond) - stats.opened_at_ms, 1_000)

        cond do
          stats.current_file_bytes >= state.max_file_bytes and stats.current_record_count > 0 ->
            do_rotate()

          age_s >= state.max_file_age_seconds and stats.current_record_count > 0 ->
            do_rotate()

          true ->
            :ok
        end

      :error ->
        :ok
    end
  end

  defp safe_stats() do
    try do
      {:ok, Writer.stats()}
    catch
      :exit, _ -> :error
    end
  end

  defp do_rotate() do
    try do
      Writer.rotate_now()
    catch
      :exit, reason -> {:error, {:writer_unavailable, reason}}
    end
  end

  defp schedule(ms), do: Process.send_after(self(), :tick, ms)
end
