defmodule Flezha.Plugin.Extension.TableHealthTest do
  @moduledoc """
  The plugin health contract, and the three fail-open rules it keeps.

  `Table.health/1` calls every running plugin's declared `health/1` with the
  processes the table started injected. A provider that overrides nothing takes
  the default, "the worker alive, or absent, is healthy". A `:prepared` plugin
  and a probe that could not be completed never report unhealthy: the loop must
  not escalate on uncertainty.
  """
  use ExUnit.Case, async: true

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.ProviderBehaviour
  alias FlezhaTest.Helper.NodeTestHelper

  defmodule HealthyProvider do
    @moduledoc false
    use ProviderBehaviour

    @impl true
    def health(_opts), do: :healthy
  end

  defmodule UnhealthyProvider do
    @moduledoc false
    use ProviderBehaviour

    @impl true
    def health(_opts), do: {:unhealthy, :parked}
  end

  defmodule RaisingProvider do
    @moduledoc false
    use ProviderBehaviour

    @impl true
    def health(_opts), do: raise("boom")
  end

  defmodule DefaultHealthProvider do
    @moduledoc false
    # Overrides nothing: the declaration's default verdict applies.
    use ProviderBehaviour
  end

  defp table(plugins, phases, workers \\ %{}) do
    entries =
      Map.new(phases, fn {id, phase} ->
        {id, %Entry{phase: phase, workers: Map.get(workers, id, %{})}}
      end)

    %Table{plugins: plugins, entries: entries}
  end

  defp plugin(provider), do: %Plugin{id: "p", provider: provider, opts: %{}}

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    pid
  end

  test "running plugins report their declared verdicts" do
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

  test "a raising health/1 answers fail-open, unjudged: the fold is the loop's" do
    t = table(%{"r" => plugin(RaisingProvider)}, %{"r" => :running})
    assert %{"r" => {:fail_open, {:raised, %RuntimeError{}}}} = Table.health(t)
  end

  test "the default verdict: an alive worker is healthy" do
    pid = spawn(&NodeTestHelper.park/0)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)

    t =
      table(
        %{"n" => plugin(DefaultHealthProvider)},
        %{"n" => :running},
        %{"n" => %{"n" => {pid, make_ref()}}}
      )

    assert Table.health(t) == %{"n" => :healthy}
  end

  test "the default verdict: a dead worker is unhealthy" do
    t =
      table(
        %{"n" => plugin(DefaultHealthProvider)},
        %{"n" => :running},
        %{"n" => %{"n" => {dead_pid(), make_ref()}}}
      )

    assert Table.health(t) == %{"n" => {:unhealthy, :worker_dead}}
  end

  test "the default verdict: no worker at all is healthy" do
    t = table(%{"n" => plugin(DefaultHealthProvider)}, %{"n" => :running})
    assert Table.health(t) == %{"n" => :healthy}
  end
end
