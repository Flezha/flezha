defmodule Flezha.Plugin.Extension.TableProviderRestartTest do
  @moduledoc """
  `Flezha.Plugin.Extension.Table.attempt_restart/3` — the one restart path,
  reached by the convergence loop's `restart_plugin` — re-runs `init_provider/1`
  for Provider-kind plugins, so a fresh owned pid is spawned after the one
  before it died. Reshuffling the subscription tables alone leaves the socket
  dead until the container is restarted by hand.

  The table itself restarts nothing: a `DOWN` is recorded as `{:down, reason}`
  and no timer is armed.
  """

  # fails when: provider plugin stays dead after watchdog kill (no new owned pid spawned)

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host

  @catalog Host.build_catalog(
             [
               {:on_input, :transform, Ctx.IO}
             ],
             %{file: __ENV__.file, line: __ENV__.line}
           )

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

  defmodule FailingProvider do
    def init_provider(_opts), do: {:error, :econnrefused}
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defp plugin(plugin_id, provider),
    do: %Plugin{id: plugin_id, provider: provider, opts: %{}, subscriptions: [{:on_input, []}]}

  test "a crashed subscriber is recorded down, armed with nothing, and attempt_restart re-spawns it" do
    plugin_id = "fake_provider_ws"
    plugin = plugin(plugin_id, FakeProvider)

    {:ok, table} = Table.subscribe(Table.new(@catalog), plugin_id, plugin)

    # First-spawn pid is tracked.
    {original_pid, original_ref} = Map.fetch!(table.owned_pids, plugin_id)
    assert is_pid(original_pid)
    assert Process.alive?(original_pid)
    assert Map.fetch!(table.monitors, original_ref) == plugin_id

    # The watchdog fires: kill the pid with the same shutdown reason
    # `Flezha.Plugin.Provider.Websocket.Client.handle_info(:idle_check)` uses,
    # then drive `Table.handle_down` with the :DOWN the BEAM monitor delivers.
    Process.exit(original_pid, {:shutdown, :idle_timeout})
    assert_receive {:DOWN, ^original_ref, :process, ^original_pid, _}, 1_000

    {{:ok, table}, _log} =
      with_log(fn ->
        Table.handle_down(table, original_ref, original_pid, {:shutdown, :idle_timeout})
      end)

    # The dead pid is evicted and the plugin recorded down — nothing scheduled.
    refute Map.has_key?(table.owned_pids, plugin_id)
    refute Map.has_key?(table.monitors, original_ref)
    assert Map.has_key?(table.plugins, plugin_id)
    assert Table.phase(table, plugin_id) == {:down, {:shutdown, :idle_timeout}}
    assert Table.down_ids(table) == %{plugin_id => {:shutdown, :idle_timeout}}
    assert Table.running_ids(table) == []
    assert {:messages, []} = Process.info(self(), :messages), "the table armed a timer"

    # The loop's restart: a NEW pid, distinct from the dead one, under a fresh
    # monitor pointing back at the plugin, and the phase a provider without
    # `activate/1` re-enters directly.
    {:ok, table} = Table.attempt_restart(table, plugin_id, plugin)

    {new_pid, new_ref} = Map.fetch!(table.owned_pids, plugin_id)
    assert is_pid(new_pid)
    assert new_pid != original_pid
    assert Process.alive?(new_pid)
    assert new_ref != original_ref
    assert Map.fetch!(table.monitors, new_ref) == plugin_id
    assert Table.phase(table, plugin_id) == :running
    assert Table.down_ids(table) == %{}

    # Plugin entry's opts.conn now points at the new pid (proves
    # init_provider's return value replaced the stale plugin entry).
    assert Map.fetch!(table.plugins, plugin_id).opts.conn == new_pid
  end

  test "a restart whose init_provider fails leaves the plugin recorded down for the next rung" do
    plugin_id = "failing_provider"
    plugin = plugin(plugin_id, FailingProvider)

    # Bootstrap by stuffing the plugin into the table directly — `subscribe`
    # would refuse a plugin whose first init_provider call returns :error.
    table =
      Table.new(@catalog)
      |> Map.update!(:plugins, &Map.put(&1, plugin_id, plugin))

    {{:error, :econnrefused, table}, _log} =
      with_log(fn -> Table.attempt_restart(table, plugin_id, plugin) end)

    refute Map.has_key?(table.owned_pids, plugin_id)
    assert Map.has_key?(table.plugins, plugin_id)
    assert Table.phase(table, plugin_id) == {:down, :econnrefused}
  end

  test "a running subscriber's uptime is reported; a down one's is not" do
    plugin_id = "uptime_ws"

    {:ok, table} =
      Table.subscribe(Table.new(@catalog), plugin_id, plugin(plugin_id, FakeProvider))

    {table, []} = Table.bulk_activate(table)

    assert %{^plugin_id => uptime} = Table.uptimes(table)
    assert is_integer(uptime) and uptime >= 0

    {pid, ref} = Map.fetch!(table.owned_pids, plugin_id)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    {{:ok, table}, _log} = with_log(fn -> Table.handle_down(table, ref, pid, :killed) end)

    assert Table.uptimes(table) == %{}
  end
end
