defmodule FlezhaTest.Helper.ScheduleTestHelper do
  @moduledoc """
  Shared setup for the schedule suites: an isolated
  `Flezha.Schedule.Store` against a per-test tmp dir, the `:flz_schedules`
  hub, and entry builders.
  """

  import ExUnit.Callbacks
  import ExUnit.Assertions

  alias Flezha.Service.HubManager

  @doc """
  Point `:schedules` at a tmp dir and start a supervised Store. `opts` are
  merged into the `:schedules` config (e.g. `misfire_grace_ms: 0`).
  """
  def isolated_store_setup(opts \\ []) do
    tmp = Path.join(System.tmp_dir!(), "flz_sched_#{System.unique_integer([:positive])}")
    prev = Application.get_env(:flezha, :schedules, [])

    Application.put_env(
      :flezha,
      :schedules,
      prev |> Keyword.merge(dir: tmp) |> Keyword.merge(opts)
    )

    on_exit(fn ->
      Application.put_env(:flezha, :schedules, prev)
      File.rm_rf!(tmp)
    end)

    start_supervised!(Flezha.Schedule.Store)
    :ok
  end

  @doc "Start the `:flz_schedules` hub for this test."
  def schedule_hub_setup do
    {:ok, _pid} = HubManager.start_schedule_hub()
    on_exit(&HubManager.stop_schedule_hub/0)
    :ok
  end

  @doc "The `Store.add/1` field map, with overrides."
  def entry_fields(overrides \\ %{}) do
    Map.merge(
      %{
        name: "job",
        cron: "* * * * *",
        timezone: "Etc/UTC",
        factory_id: "fac",
        namespace: "default",
        init_values: %{"a" => 1}
      },
      Map.new(overrides)
    )
  end

  @doc """
  A full store entry without touching DETS. `created_at` defaults far enough
  back that it never clamps a grace look-back.
  """
  def entry(overrides \\ %{}) do
    overrides
    |> entry_fields()
    |> Map.put_new(:created_at, "2020-01-01T00:00:00Z")
  end

  # A slot far enough out that no job fires mid-test — a due schedule would
  # spawn into the fire path while an assertion is running.
  @quiet_cron "0 0 1 1 *"

  @doc "An unstored entry that will not fire during the test."
  def quiet_entry(overrides \\ %{}) do
    entry(Map.merge(%{cron: @quiet_cron}, Map.new(overrides)))
  end

  @doc "A stored schedule that will not fire during the test."
  def add_quiet(name) do
    {:ok, entry} = Flezha.Schedule.Store.add(entry_fields(%{name: name, cron: @quiet_cron}))
    entry
  end

  @doc """
  Poll `fun` until it answers `{:ok, value}`, then return that value; `flunk`
  with `describe` at the deadline. `fun` answers `:retry` to keep waiting.

  Every schedule wait goes through this — `start_child`/`stop_child` and
  cross-node hub discovery are all asynchronous, so a bare assert races.
  """
  @poll_interval_ms 50
  def await_until(timeout_ms, describe, fun) when is_function(fun, 0) do
    do_await_until(System.monotonic_time(:millisecond) + timeout_ms, describe, fun)
  end

  defp do_await_until(deadline, describe, fun) do
    case fun.() do
      {:ok, value} ->
        value

      :retry ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("timed out waiting for #{describe.()}")
        else
          Process.sleep(@poll_interval_ms)
          do_await_until(deadline, describe, fun)
        end
    end
  end

  @doc "Block until `name` has a live pid in the schedule hub."
  def await_registered(name, timeout_ms \\ 2_000) do
    await_pids(name, timeout_ms, "#{name} to be registered", &(&1 != []))
  end

  @doc "Block until `name` has no pid in the schedule hub."
  def await_unregistered(name, timeout_ms \\ 2_000) do
    await_pids(name, timeout_ms, "#{name} to be unregistered", &(&1 == []))
  end

  defp await_pids(name, timeout_ms, describe, done?) do
    await_until(timeout_ms, fn -> "#{describe}; got #{inspect(schedule_pids(name))}" end, fn ->
      pids = schedule_pids(name)
      if done?.(pids), do: {:ok, pids}, else: :retry
    end)
  end

  defp schedule_pids(name) do
    ProcessHub.get_pids(HubManager.schedule_hub_id(), name)
  end
end
