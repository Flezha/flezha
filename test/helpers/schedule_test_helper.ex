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

  @doc "Block until `name` has a live pid in the schedule hub."
  def await_registered(name, timeout_ms \\ 2_000) do
    await_pids(name, timeout_ms, &(&1 != []))
  end

  @doc "Block until `name` has no pid in the schedule hub."
  def await_unregistered(name, timeout_ms \\ 2_000) do
    await_pids(name, timeout_ms, &(&1 == []))
  end

  # `start_child`/`stop_child` are asynchronous, so a bare assert would race.
  defp await_pids(name, timeout_ms, done?) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_pids(name, deadline, done?)
  end

  defp do_await_pids(name, deadline, done?) do
    pids = ProcessHub.get_pids(HubManager.schedule_hub_id(), name)

    cond do
      done?.(pids) ->
        pids

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(
          "schedule #{name} did not reach the expected registration state; got #{inspect(pids)}"
        )

      true ->
        Process.sleep(20)
        do_await_pids(name, deadline, done?)
    end
  end
end
