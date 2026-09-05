defmodule Flezha.Schedule.Job do
  @moduledoc """
  One process per schedule, hosted in the `:flz_schedules` hub. The hub's
  `Singularity` redundancy makes exactly one of these exist cluster-wide per
  schedule name, so a slot fires once without leader election.

  The process is its own watermark: it holds an armed timer for the next slot.
  The only gap is its own absence (restart, migration, node takeover), which
  the misfire-grace look-back on start repairs by firing the single most
  recent slot inside the window. Older slots are never back-filled.

  Firing goes through the shared workflow start path
  (`OperativeManager.start_workflow/5`) with deterministic instance id
  `<name>@<slot-iso>`; its `:duplicate` answer is the duplicate-fire no-op.

  A slot whose spawn fails is retried with exponential backoff under the SAME
  instance id until the next slot supersedes it — at most one slot is ever in
  flight, the misfire-grace rule ("only the most recent slot") extended from
  process start to steady state. The schedule never parks: a factory whose
  runner has not connected yet simply keeps probing.
  """

  use GenServer

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Schedule.{Cron, Store}
  alias Flezha.Service.OperativeManager

  # Backoff cap equals the finest cron granularity, so a retrying schedule
  # probes at least once per minute.

  # `Process.send_after/3` tops out near 49.7 days but the cron scan horizon is
  # 1500 days, so a distant slot is re-armed in chunks. The daily recompute is
  # also what absorbs NTP steps and DST: send_after runs on the monotonic
  # clock, cron slots on the wall clock.

  # The cspec carries the whole definition so a migrated start needs no lookup.
  def child_spec(entry) do
    %{id: entry.name, start: {__MODULE__, :start_link, [entry]}}
  end

  def start_link(entry) do
    GenServer.start_link(__MODULE__, entry)
  end

  @doc """
  Spawn one instance for `entry` with `instance_id` through the standard
  workflow start path. Also serves `flz schedule run` (manual fire).

  The entry's plugin spec is decoded per firing (provider ids resolve against
  the running controller) and attached AT SPAWN — a workflow instance finishes
  in milliseconds, so a plugin attached afterwards would miss the whole run.
  """
  @spec spawn_instance(Store.entry(), String.t(), pos_integer()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def spawn_instance(entry, instance_id, attempt \\ 1) do
    # A run without its output channel spends and reports nothing, which is
    # worse than a visibly skipped slot — so an undecodable spec starts nothing.
    case instance_plugins(entry, instance_id) do
      {:ok, plugins} ->
        start(entry, instance_id, plugins, attempt)

      {:error, reason} ->
        failed(entry, instance_id, reason, "plugin spec did not decode", attempt)
    end
  end

  # Plugin ids are namespaced by the instance id: slot ids differ, so two
  # overlapping runs must not claim one plugin id.
  defp instance_plugins(entry, instance_id) do
    with {:ok, plugins} <- SpecDecoder.decode(entry.plugins, entry.namespace, instance_id) do
      {:ok, Enum.map(plugins, &%{&1 | id: "#{instance_id}_#{&1.id}"})}
    end
  end

  defp start(entry, instance_id, plugins, attempt) do
    opts = if plugins == [], do: [], else: [plugins: plugins]

    case OperativeManager.start_workflow(
           entry.namespace,
           instance_id,
           entry.factory_id,
           entry.init_values,
           opts
         ) do
      {:ok, _} = ok ->
        :telemetry.execute([:flezha, :schedule, :fired], %{count: 1}, %{
          schedule: entry.name,
          instance_id: instance_id
        })

        Logger.info("[schedule] fired #{instance_id} (factory #{entry.factory_id})")
        ok

      :duplicate ->
        Logger.debug("[schedule] duplicate fire for #{instance_id} — no-op")
        :duplicate

      {:error, reason} ->
        failed(entry, instance_id, reason, "spawn failed", attempt)
    end
  end

  # The one place a slot attempt that produced no instance is reported.
  defp failed(entry, instance_id, reason, what, attempt) do
    :telemetry.execute([:flezha, :schedule, :spawn_failed], %{count: 1}, %{
      schedule: entry.name,
      instance_id: instance_id,
      attempt: attempt
    })

    Logger.error(
      "[schedule] #{instance_id} not started (attempt #{attempt}): #{what}: #{inspect(reason)}"
    )

    {:error, reason}
  end

  @doc false
  # The most recent slot inside the grace window, floored at `created_at` so a
  # freshly added schedule never fires history. `:none` once the window is
  # closed (`grace_ms <= 0`) or empty.
  @spec catch_up_slot(Cron.t(), Store.entry(), DateTime.t(), non_neg_integer()) ::
          {:ok, DateTime.t()} | :none
  def catch_up_slot(cron, entry, now, grace_ms) when grace_ms > 0 do
    from = catch_up_floor(entry, now, grace_ms)

    if DateTime.compare(from, now) == :lt do
      # One minute is the finest cron granularity, so this cap cannot truncate.
      cap = div(grace_ms, 60_000) + 2

      case Cron.slots_between(cron, entry.timezone, from, now, cap) do
        {[], _} -> :none
        {slots, _} -> {:ok, List.last(slots)}
      end
    else
      :none
    end
  end

  def catch_up_slot(_cron, _entry, _now, _grace_ms), do: :none

  @doc false
  # Slots beyond the send_after ceiling are approached one chunk at a time.
  @spec arm_action(DateTime.t(), DateTime.t()) ::
          {:fire, non_neg_integer()} | {:rearm, non_neg_integer()}
  def arm_action(target, now) do
    chunk = Durations.get(:schedule_rearm_chunk_ms)

    case DateTime.diff(target, now, :millisecond) do
      ms when ms <= chunk -> {:fire, max(ms, 0)}
      _ -> {:rearm, chunk}
    end
  end

  ## ----- GenServer -----

  @impl true
  def init(entry) do
    # Never raise or stop: the cspec is permanent under a supervisor whose
    # restart intensity a crash loop would exhaust, taking the hub's whole
    # distributed supervisor with it. An unusable schedule goes dormant and
    # keeps its child id claimed, so the hub still reports one owner.
    state = %{
      entry: entry,
      cron: nil,
      target: nil,
      timer: nil,
      pending_retry: nil,
      last_fire: nil
    }

    case Cron.parse(entry.cron) do
      {:ok, cron} -> {:ok, %{state | cron: cron}, {:continue, :start}}
      {:error, msg} -> {:ok, dormant(state, "unparsable cron: #{msg}")}
    end
  end

  @impl true
  def handle_continue(:start, state) do
    now = DateTime.utc_now()

    state =
      case catch_up_slot(state.cron, state.entry, now, misfire_grace_ms()) do
        {:ok, slot} -> fire_slot(state, slot)
        :none -> state
      end

    {:noreply, arm(state, now)}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, %{last_fire: state.last_fire}, state}
  end

  @impl true
  def handle_info({:fire, slot}, state) do
    now = DateTime.utc_now()

    # An early wake must not fire ahead of the slot; re-arming recomputes the
    # same target because `next_fire/3` is strictly-after.
    if DateTime.compare(now, slot) == :lt do
      {:noreply, arm(state, now)}
    else
      {:noreply, arm(fire_slot(state, slot), DateTime.utc_now())}
    end
  end

  def handle_info({:retry, slot, attempt}, state) do
    case state.pending_retry do
      %{slot: ^slot, attempt: ^attempt} ->
        {:noreply, attempt_slot(%{state | pending_retry: nil}, slot, attempt)}

      _ ->
        # A cancelled retry whose timer message was already in the mailbox.
        {:noreply, state}
    end
  end

  def handle_info(:rearm, state) do
    {:noreply, arm(state, DateTime.utc_now())}
  end

  ## ----- private -----

  # The one definition of "fire this slot": supersession first — the new slot
  # is the only one owed, so a still-pending retry for an earlier slot is
  # abandoned, never back-filled — then the first attempt.
  defp fire_slot(state, slot) do
    state |> cancel_pending_retry() |> attempt_slot(slot, 1)
  end

  # One spawn attempt for `slot`. The instance id is the slot's, so a retry
  # racing any other fire attempt no-ops on `:duplicate` — which IS success:
  # someone fired the slot.
  defp attempt_slot(state, slot, attempt) do
    instance_id = "#{state.entry.name}@#{DateTime.to_iso8601(slot)}"

    case spawn_instance(state.entry, instance_id, attempt) do
      {:ok, _} ->
        record_fire(state, instance_id, :ok)

      :duplicate ->
        record_fire(state, instance_id, :duplicate)

      {:error, reason} ->
        backoff = Backoff.arm(:schedule_spawn, attempt, state.entry.name)
        timer = Process.send_after(self(), {:retry, slot, attempt + 1}, backoff)
        state = record_fire(state, instance_id, {:retrying, inspect(reason)})
        %{state | pending_retry: %{slot: slot, attempt: attempt + 1, timer: timer}}
    end
  end

  defp record_fire(state, instance_id, outcome) do
    %{state | last_fire: %{instance_id: instance_id, outcome: outcome, at: DateTime.utc_now()}}
  end

  defp cancel_pending_retry(%{pending_retry: nil} = state), do: state

  defp cancel_pending_retry(%{pending_retry: %{timer: timer}} = state) do
    _ = Process.cancel_timer(timer)
    %{state | pending_retry: nil}
  end

  defp arm(state, now) do
    case Cron.next_fire(state.cron, state.entry.timezone, now) do
      {:ok, target} ->
        timer =
          case arm_action(target, now) do
            {:fire, ms} -> Process.send_after(self(), {:fire, target}, ms)
            {:rearm, ms} -> Process.send_after(self(), :rearm, ms)
          end

        %{state | target: target, timer: timer}

      :none ->
        dormant(state, "no future slot for #{state.entry.cron}")
    end
  end

  defp dormant(state, reason) do
    :telemetry.execute([:flezha, :schedule, :dormant], %{count: 1}, %{
      schedule: state.entry.name
    })

    Logger.warning("[schedule] #{state.entry.name} is dormant — #{reason}")
    %{state | target: nil, timer: nil}
  end

  defp catch_up_floor(entry, now, grace_ms) do
    floor = DateTime.add(now, -grace_ms, :millisecond)

    case DateTime.from_iso8601(entry.created_at) do
      {:ok, created_at, _} ->
        if DateTime.compare(created_at, floor) == :gt, do: created_at, else: floor

      _ ->
        floor
    end
  end

  defp misfire_grace_ms(), do: Durations.get(:schedule_misfire_grace_ms)
end
