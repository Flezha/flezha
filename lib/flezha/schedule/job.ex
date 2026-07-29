defmodule Flezha.Schedule.Job do
  @moduledoc """
  One process per schedule, hosted in the `:flz_schedules` hub. The hub's
  `Singularity` redundancy makes exactly one of these exist cluster-wide per
  schedule name, so a slot fires once without leader election.

  The process is its own watermark: it holds an armed timer for the next slot.
  The only gap is its own absence (restart, migration, node takeover), which
  the misfire-grace look-back on start repairs by firing the single most
  recent slot inside the window. Older slots are never back-filled.

  Firing goes through the standard bounded start path with deterministic
  instance id `<name>@<slot-iso>`; "operative id already in use" is the
  duplicate-fire no-op.
  """

  use GenServer

  require Logger

  alias Flezha.Operative.Builder
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Schedule.{Cron, Store}
  alias Flezha.Service.OperativeManager

  # `Process.send_after/3` tops out near 49.7 days but the cron scan horizon is
  # 1500 days, so a distant slot is re-armed in chunks. The daily recompute is
  # also what absorbs NTP steps and DST: send_after runs on the monotonic
  # clock, cron slots on the wall clock.
  @rearm_chunk_ms 86_400_000

  # The cspec carries the whole definition so a migrated start needs no lookup.
  def child_spec(entry) do
    %{id: entry.name, start: {__MODULE__, :start_link, [entry]}}
  end

  def start_link(entry) do
    GenServer.start_link(__MODULE__, entry)
  end

  @doc """
  Spawn one instance for `entry` with `instance_id` through the standard
  bounded start path. Also serves `flz schedule run` (manual fire).

  The entry's plugin spec is decoded per firing (provider ids resolve against
  the running controller) and attached AT SPAWN — a bounded instance finishes
  in milliseconds, so a plugin attached afterwards would miss the whole run.
  """
  @spec spawn_instance(Store.entry(), String.t()) ::
          {:ok, map()} | :duplicate | {:error, term()}
  def spawn_instance(entry, instance_id) do
    # A run without its output channel spends and reports nothing, which is
    # worse than a visibly skipped slot — so an undecodable spec starts nothing.
    case instance_plugins(entry, instance_id) do
      {:ok, plugins} -> start(entry, instance_id, plugins)
      {:error, reason} -> failed(entry, instance_id, reason, "plugin spec did not decode")
    end
  end

  # Plugin ids are namespaced by the instance id: slot ids differ, so two
  # overlapping runs must not claim one plugin id.
  defp instance_plugins(entry, instance_id) do
    with {:ok, plugins} <- SpecDecoder.decode(entry.plugins, entry.namespace, instance_id) do
      {:ok, Enum.map(plugins, &%{&1 | id: "#{instance_id}_#{&1.id}"})}
    end
  end

  defp start(entry, instance_id, plugins) do
    builder = %Builder{
      id: instance_id,
      factory_id: entry.factory_id,
      kind: :bounded,
      init_values: entry.init_values,
      opts: if(plugins == [], do: [], else: [plugins: plugins])
    }

    case OperativeManager.start_operative(entry.namespace, builder) do
      {:ok, _} = ok ->
        :telemetry.execute([:flezha, :schedule, :fired], %{count: 1}, %{
          schedule: entry.name,
          instance_id: instance_id
        })

        Logger.info("[schedule] fired #{instance_id} (factory #{entry.factory_id})")
        ok

      {:error, %{reason: "operative id already in use"}} ->
        Logger.debug("[schedule] duplicate fire for #{instance_id} — no-op")
        :duplicate

      {:error, reason} ->
        failed(entry, instance_id, reason, "spawn failed")
    end
  end

  # The one place a slot that produced no instance is reported.
  defp failed(entry, instance_id, reason, what) do
    :telemetry.execute([:flezha, :schedule, :spawn_failed], %{count: 1}, %{
      schedule: entry.name,
      instance_id: instance_id
    })

    Logger.error("[schedule] #{instance_id} not started: #{what}: #{inspect(reason)}")
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
    case DateTime.diff(target, now, :millisecond) do
      ms when ms <= @rearm_chunk_ms -> {:fire, max(ms, 0)}
      _ -> {:rearm, @rearm_chunk_ms}
    end
  end

  ## ----- GenServer -----

  @impl true
  def init(entry) do
    # Never raise or stop: the cspec is permanent under a supervisor whose
    # restart intensity a crash loop would exhaust, taking the hub's whole
    # distributed supervisor with it. An unusable schedule goes dormant and
    # keeps its child id claimed, so the hub still reports one owner.
    state = %{entry: entry, cron: nil, target: nil, timer: nil}

    case Cron.parse(entry.cron) do
      {:ok, cron} -> {:ok, %{state | cron: cron}, {:continue, :start}}
      {:error, msg} -> {:ok, dormant(state, "unparsable cron: #{msg}")}
    end
  end

  @impl true
  def handle_continue(:start, state) do
    now = DateTime.utc_now()

    case catch_up_slot(state.cron, state.entry, now, misfire_grace_ms()) do
      {:ok, slot} -> fire(state.entry, slot)
      :none -> :ok
    end

    {:noreply, arm(state, now)}
  end

  @impl true
  def handle_info({:fire, slot}, state) do
    now = DateTime.utc_now()

    # An early wake must not fire ahead of the slot; re-arming recomputes the
    # same target because `next_fire/3` is strictly-after.
    if DateTime.compare(now, slot) == :lt do
      {:noreply, arm(state, now)}
    else
      fire(state.entry, slot)
      {:noreply, arm(state, DateTime.utc_now())}
    end
  end

  def handle_info(:rearm, state) do
    {:noreply, arm(state, DateTime.utc_now())}
  end

  ## ----- private -----

  defp fire(entry, slot) do
    _ = spawn_instance(entry, "#{entry.name}@#{DateTime.to_iso8601(slot)}")
    :ok
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

  defp misfire_grace_ms() do
    Application.get_env(:flezha, :schedules, []) |> Keyword.get(:misfire_grace_ms, 120_000)
  end
end
