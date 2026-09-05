defmodule Flezha.Plugin.Extension.TableHealthTest do
  @moduledoc """
  Plugin health contract (self-healing-control-plane, Phase 7.2).
  `Table.health/1` queries each *running* plugin's provider `health/1`,
  defaulting to "owned pid alive (or absent) ⇒ healthy"; a `:prepared` plugin or
  a missing/raising probe never reports unhealthy (no escalation on uncertainty).
  """
  use ExUnit.Case, async: true

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias FlezhaTest.Helper.NodeTestHelper

  defmodule HealthyProvider do
    def health(_opts), do: :healthy
  end

  defmodule UnhealthyProvider do
    def health(_opts), do: {:unhealthy, :parked}
  end

  defmodule RaisingProvider do
    def health(_opts), do: raise("boom")
  end

  defmodule NoHealthProvider do
    # intentionally exports no health/1 — the default alive-check applies
  end

  defp table(plugins, phases, owned_pids \\ %{}) do
    %Table{plugins: plugins, phases: phases, owned_pids: owned_pids}
  end

  defp plugin(provider), do: %Plugin{id: "p", provider: provider, opts: %{}}

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    pid
  end

  test "running plugins with health/1 report their verdicts" do
    t =
      table(
        %{"a" => plugin(HealthyProvider), "b" => plugin(UnhealthyProvider)},
        %{"a" => :running, "b" => :running}
      )

    assert Table.health(t) == %{"a" => :healthy, "b" => {:unhealthy, :parked}}
  end

  test "prepared plugins are reported healthy (not yet active)" do
    t = table(%{"b" => plugin(UnhealthyProvider)}, %{"b" => :prepared})
    assert Table.health(t) == %{"b" => :healthy}
  end

  test "a raising health/1 answers fail-open, unjudged — the fold is the loop's" do
    t = table(%{"r" => plugin(RaisingProvider)}, %{"r" => :running})
    assert %{"r" => {:fail_open, {:raised, %RuntimeError{}}}} = Table.health(t)
  end

  test "provider without health/1: an alive owned pid is healthy" do
    pid = spawn(&NodeTestHelper.park/0)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)

    t = table(%{"n" => plugin(NoHealthProvider)}, %{"n" => :running}, %{"n" => {pid, make_ref()}})
    assert Table.health(t) == %{"n" => :healthy}
  end

  test "provider without health/1: a dead owned pid is unhealthy" do
    t =
      table(
        %{"n" => plugin(NoHealthProvider)},
        %{"n" => :running},
        %{"n" => {dead_pid(), make_ref()}}
      )

    assert Table.health(t) == %{"n" => {:unhealthy, :owned_pid_dead}}
  end

  test "provider without health/1 and no owned pid is healthy" do
    t = table(%{"n" => plugin(NoHealthProvider)}, %{"n" => :running})
    assert Table.health(t) == %{"n" => :healthy}
  end
end
