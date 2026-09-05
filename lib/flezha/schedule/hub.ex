defmodule Flezha.Schedule.Hub do
  @moduledoc """
  Owner of the `:flz_schedules` hub and the assert/retract seam onto it.

  The hub is a transient uniqueness arbiter: every controller asserts the
  schedules its node-local `Flezha.Schedule.Store` holds, a duplicate child id
  no-ops, and `Singularity` leaves exactly one `Flezha.Schedule.Job` running
  cluster-wide. Nothing here is durable — the store is.

  Every entry point is gated on hub liveness (through the one
  `HubManager.with_live_hub_id/3` guard), so a controller with the hub down —
  or `:schedules[:application_managed]` false — still serves the CLI.

  A writer must pair a store mutation with the matching call here. The
  asymmetry matters: a missed `assert/1` self-heals on the next re-assert
  sweep, a missed `retract/1` leaves a job firing with no row behind it.
  """

  use GenServer

  require Logger

  alias Flezha.Cluster.Orchestrator
  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.SafeCall
  alias Flezha.Schedule.{Job, Store}
  alias Flezha.Service.HubManager

  # A wedged job must not stall the list render; it reports node only.
  # Covers the residual case only: definitions asserted while the hub was down
  # or mid-formation. ProcessHub itself re-places children on nodedown.

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Register `entry`'s job. Fire-and-forget: an id already claimed cluster-wide
  IS the arbitration, not an error.
  """
  @spec assert(Store.entry()) :: :ok
  def assert(entry) do
    with_live_hub(:ok, fn hub_id ->
      case ProcessHub.start_child(hub_id, Job.child_spec(entry)) do
        {:ok, _} ->
          :ok

        {:error, {:already_started, _ids}} ->
          :ok

        {:error, reason} ->
          Logger.warning("[schedule] asserting #{entry.name} failed: #{inspect(reason)}")
          :ok
      end
    end)
  end

  @doc "Assert every schedule this node holds."
  @spec assert_all() :: :ok
  def assert_all() do
    Enum.each(Store.list(), &assert/1)
  end

  @doc """
  Stop `name`'s job cluster-wide. Waits, unlike `assert/1`: `flz schedule
  remove` must be effective on return, else the job keeps its armed timer and
  fires one more slot after the row is gone.
  """
  @spec retract(String.t()) :: :ok
  def retract(name) do
    with_live_hub(:ok, fn hub_id ->
      # The verdict is logged, never returned: the durable row is already gone,
      # so the caller has nothing to decide on. The sweep only ever asserts, so
      # a failed retract leaves a job with no store row — hence the log.
      with {:error, reason} <-
             hub_id
             |> ProcessHub.stop_child(name,
               awaitable: true,
               await_timeout: Durations.get(:schedule_retract_timeout_ms)
             )
             |> ProcessHub.Future.await() do
        Logger.warning("[schedule] retracting #{name} failed: #{inspect(reason)}")
      end

      :ok
    end)
  end

  @doc """
  Which node runs each registered schedule, plus each job's live `last_fire`
  (`nil` before the first fire or when the job does not answer inside the
  bound — the list must render even with a wedged job).
  """
  @spec statuses() :: %{optional(String.t()) => %{node: node(), last_fire: map() | nil}}
  def statuses() do
    with_live_hub(%{}, fn hub_id ->
      hub_id
      |> ProcessHub.process_list(:global)
      |> Enum.reduce(%{}, fn {name, node_pids}, acc ->
        case HubManager.live_node(node_pids) do
          nil -> acc
          node -> Map.put(acc, name, %{node: node, last_fire: job_last_fire(node_pids)})
        end
      end)
    end)
  end

  defp job_last_fire(node_pids) do
    with pid when is_pid(pid) <- HubManager.live_pid(node_pids),
         {:ok, %{last_fire: last_fire}} <-
           SafeCall.bounded(pid, :status, Durations.get(:schedule_status_call_ms)) do
      last_fire
    else
      _ -> nil
    end
  end

  ## ----- GenServer -----

  @impl true
  def init(_opts) do
    {:ok, %{}, {:continue, :bootstrap}}
  end

  @impl true
  def handle_continue(:bootstrap, state) do
    # Off the boot path on purpose: `await_cluster_ready/0` blocks for up to
    # the formation timeout, and creating the hub on a not-yet-clustered node
    # would diverge it from its peers.
    _ = Orchestrator.await_cluster_ready()
    {:noreply, start_hub(state)}
  end

  @impl true
  def handle_info(:retry_hub, state) do
    {:noreply, start_hub(state)}
  end

  def handle_info(:reassert, state) do
    assert_all()
    {:noreply, schedule_reassert(state)}
  end

  ## ----- private -----

  defp start_hub(state) do
    case HubManager.start_schedule_hub() do
      {:ok, _pid} ->
        assert_all()
        schedule_reassert(state)

      {:error, reason} ->
        :telemetry.execute([:flezha, :schedule, :hub_start_failed], %{count: 1}, %{
          reason: inspect(reason)
        })

        # Never crash: this owner sits under the application's one_for_one
        # supervisor, so a restart loop would take the controller down.
        Logger.error("[schedule] schedule hub failed to start: #{inspect(reason)}; retrying")
        Process.send_after(self(), :retry_hub, Backoff.arm(:hub_retry, 1, :schedule_hub))
        state
    end
  end

  defp schedule_reassert(state) do
    Process.send_after(self(), :reassert, Durations.get(:schedule_reassert_interval_ms))
    state
  end

  defp with_live_hub(on_dead, fun) do
    HubManager.with_live_hub_id(HubManager.schedule_hub_id(), on_dead, fun)
  end
end
