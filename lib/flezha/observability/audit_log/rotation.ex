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

  alias Flezha.Observability.AuditLog.Writer

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

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
    config = Application.get_env(:flezha, :audit_log, [])

    state = %{
      interval_ms: Keyword.get(config, :rotation_check_interval_ms, 60_000),
      max_file_bytes: Keyword.get(config, :max_file_bytes, 67_108_864),
      max_file_age_seconds: Keyword.get(config, :max_file_age_seconds, 86_400)
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
