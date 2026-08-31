defmodule Flezha.Plugin.Extension.TableProviderRestartTest do
  @moduledoc """
  Regression test for the post-hibernation outage shipped 2026-05-01.

  Asserts that `Flezha.Plugin.Extension.Table.attempt_restart/3` invokes
  `init_provider/1` for Provider-kind plugins so a fresh owned pid is
  always spawned after the watchdog kills the previous one. Before the
  fix, `attempt_restart` only reshuffled the subscription tables and the
  WS stayed dead until the container was restarted manually.
  """

  # fails when: provider plugin stays dead after watchdog kill (no new owned pid spawned)

  # async: false: writes Application env.

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.State.TestHelpers

  @catalog Host.build_catalog(
             [
               {:on_input, :transform, Ctx.IO}
             ],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  setup do
    # The production base is 4 s; the retry ladder's shape is what is under
    # test, not its wall clock.
    on_exit(TestHelpers.override_env(:plugin_retry_base_delay_ms, 20))
    :ok
  end

  defmodule FakeProvider do
    def init_provider(opts) do
      pid =
        spawn(fn ->
          receive do
            _ -> :ok
          end
        end)

      {:ok, Map.put(opts || %{}, :conn, pid), pid}
    end

    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  test "attempt_restart re-spawns owned pid after :idle_timeout shutdown" do
    plugin_id = "fake_provider_ws"

    plugin = %Plugin{
      id: plugin_id,
      provider: FakeProvider,
      opts: %{},
      subscriptions: [{:on_input, []}]
    }

    {:ok, table} = Table.subscribe(Table.new(@catalog), plugin_id, plugin)

    # First-spawn pid is tracked.
    {original_pid, original_ref} = Map.fetch!(table.owned_pids, plugin_id)
    assert is_pid(original_pid)
    assert is_reference(original_ref)
    assert Process.alive?(original_pid)
    assert Map.fetch!(table.monitors, original_ref) == plugin_id

    # Simulate the watchdog firing: kill the pid with the same shutdown
    # reason `Flezha.Plugin.Provider.Websocket.Client.handle_info(:idle_check)`
    # uses, then drive `Table.handle_down` with the :DOWN we'd receive
    # from the BEAM monitor.
    Process.exit(original_pid, {:shutdown, :idle_timeout})
    assert_receive {:DOWN, ^original_ref, :process, ^original_pid, _}, 1_000

    # with_log silences the expected [Extension.Table] subscriber-crashed
    # error log emitted by handle_down for the deliberately-killed provider.
    {{:ok, table}, _log} =
      with_log(fn ->
        Table.handle_down(table, original_ref, original_pid, {:shutdown, :idle_timeout})
      end)

    # After handle_down, the dead pid is evicted and a retry was scheduled.
    refute Map.has_key?(table.owned_pids, plugin_id)
    refute Map.has_key?(table.monitors, original_ref)
    assert Map.has_key?(table.plugins, plugin_id)

    # `Table.retry` is what runs when the host process consumes the
    # `{:retry_plugin_restart, plugin_id}` message scheduled above. Drive
    # it directly here so the test does not race the timer.
    table = Table.retry(table, plugin_id)

    # NEW pid must be in owned_pids — distinct from the dead one — and a
    # fresh monitor reference must point back to the same plugin_id.
    {new_pid, new_ref} = Map.fetch!(table.owned_pids, plugin_id)
    assert is_pid(new_pid)
    assert new_pid != original_pid
    assert Process.alive?(new_pid)
    assert is_reference(new_ref)
    assert new_ref != original_ref
    assert Map.fetch!(table.monitors, new_ref) == plugin_id

    # Plugin entry's opts.conn now points at the new pid (proves
    # init_provider's return value replaced the stale plugin entry).
    fresh_plugin = Map.fetch!(table.plugins, plugin_id)
    assert fresh_plugin.opts.conn == new_pid
  end

  test "init_provider failure during retry logs and reschedules without leaving the plugin dead" do
    plugin_id = "failing_provider"

    plugin = %Plugin{
      id: plugin_id,
      provider: FailingProvider,
      opts: %{},
      subscriptions: [{:on_input, []}]
    }

    # Bootstrap by stuffing the plugin into the table directly — `subscribe`
    # would refuse a plugin whose first init_provider call returns :error.
    table =
      Table.new(@catalog)
      |> Map.update!(:plugins, &Map.put(&1, plugin_id, plugin))

    # with_log silences the expected [Extension.Table] init_provider-failed
    # error log; FailingProvider intentionally returns :error.
    {table, _log} = with_log(fn -> Table.retry(table, plugin_id) end)

    # No owned pid was spawned (init_provider returned :error) but the
    # retry counter incremented so the next backoff applies.
    refute Map.has_key?(table.owned_pids, plugin_id)
    assert Map.has_key?(table.plugins, plugin_id)
    assert Map.fetch!(table.retries, plugin_id) >= 1

    # The host's mailbox now holds a {:retry_plugin_restart, plugin_id}
    # message courtesy of schedule_retry; drain it so the ExUnit cleanup
    # doesn't trip on a stray timer.
    assert_receive {:retry_plugin_restart, ^plugin_id}, 1_000
  end

  defmodule FailingProvider do
    def init_provider(_opts), do: {:error, :econnrefused}
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  # fails when: a fast-failing provider (e.g. a 429-throttled WS) retries at the
  # base delay forever because the backoff counter is reset on every restart
  test "fast-failing provider accumulates backoff instead of resetting every restart" do
    plugin_id = "flapping_ws"

    plugin = %Plugin{
      id: plugin_id,
      provider: FakeProvider,
      opts: %{},
      subscriptions: [{:on_input, []}]
    }

    {:ok, table} = Table.subscribe(Table.new(@catalog), plugin_id, plugin)

    # Three rapid crash→retry cycles, each with ~0 ms uptime (well under the
    # stable-uptime window) so the backoff counter must climb 1 → 2 → 3.
    table = crash_and_retry(table, plugin_id)
    assert Map.fetch!(table.retries, plugin_id) == 1

    table = crash_and_retry(table, plugin_id)
    assert Map.fetch!(table.retries, plugin_id) == 2

    table = crash_and_retry(table, plugin_id)
    assert Map.fetch!(table.retries, plugin_id) == 3
  end

  # fails when: a provider that recovered and ran healthily is punished with a
  # long backoff on its next unrelated crash
  test "backoff counter resets after an owned pid stays up past the stable threshold" do
    plugin_id = "recovering_ws"

    plugin = %Plugin{
      id: plugin_id,
      provider: FakeProvider,
      opts: %{},
      subscriptions: [{:on_input, []}]
    }

    {:ok, table} = Table.subscribe(Table.new(@catalog), plugin_id, plugin)

    table = crash_and_retry(table, plugin_id)
    table = crash_and_retry(table, plugin_id)
    assert Map.fetch!(table.retries, plugin_id) == 2

    # Backdate the freshly-restarted pid's start time so its next crash counts
    # as a long, healthy run rather than a flap.
    old = :erlang.monotonic_time(:millisecond) - 70_000
    table = %{table | started_at: Map.put(table.started_at, plugin_id, old)}

    table = crash_and_retry(table, plugin_id)
    assert Map.fetch!(table.retries, plugin_id) == 1
  end

  # One crash→retry cycle: kill the current owned pid, drive handle_down for the
  # monitor :DOWN, then run the scheduled retry. Returns the updated table.
  defp crash_and_retry(table, plugin_id) do
    {pid, ref} = Map.fetch!(table.owned_pids, plugin_id)
    Process.exit(pid, {:shutdown, :idle_timeout})
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000

    {{:ok, table}, _log} =
      with_log(fn ->
        Table.handle_down(table, ref, pid, {:shutdown, :idle_timeout})
      end)

    Table.retry(table, plugin_id)
  end
end
