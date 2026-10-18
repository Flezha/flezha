defmodule FlezhaTest.Helper.TimingFormatter do
  @moduledoc """
  Opt-in profiling formatter for finding where suite wall-time actually goes.

      mix test --formatter FlezhaTest.Helper.TimingFormatter \\
               --formatter ExUnit.CLIFormatter

  Writes TSV to `$TIMING_OUT` (default `/tmp/flezha_test_timing.tsv`):

      MODULE<TAB>name<TAB>wall_ms<TAB>test_count<TAB>async?
      TEST<TAB>module<TAB>name<TAB>time_ms

  Two reasons this exists rather than `mix test --slowest`: that flag sets
  `--trace`, which serializes every module and so reports timings no real run
  would produce; and it reports test bodies only, missing `setup_all`. Module
  wall time here is measured start-to-finish, so per-module fixture cost shows up.

  Async modules overlap, so their wall times include time spent waiting for a
  scheduler slot and will not sum to the suite total — compare them against each
  other, not against the clock.

  ## Conformance report

  When `FLEZHA_E2E_RUNTIME` is set or any `:system` test ran, the formatter also
  prints the SDK conformance table — every capability of the vocabulary with its
  outcome (`pass`, `fail`, `deferred` or `untested`) — and
  merges the same table into the JSON at `$CONFORMANCE_OUT` when that is set,
  each capability keeping its worst outcome across the runs that wrote there.
  The nightly uploads it per runtime.
  """
  use GenServer

  alias FlezhaTest.Helper.NodeTestHelper

  @default_out "/tmp/flezha_test_timing.tsv"

  @impl true
  def init(_opts) do
    {:ok, %{mod_start: %{}, modules: [], tests: [], capabilities: %{}, system?: false}}
  end

  @impl true
  def handle_cast({:module_started, %ExUnit.TestModule{} = mod}, state) do
    {:noreply, put_in(state.mod_start[mod.name], System.monotonic_time(:microsecond))}
  end

  def handle_cast({:module_finished, %ExUnit.TestModule{} = mod}, state) do
    now = System.monotonic_time(:microsecond)
    wall = now - Map.get(state.mod_start, mod.name, now)
    async? = Enum.any?(mod.tests, & &1.tags[:async])

    {:noreply, %{state | modules: [{mod.name, wall, length(mod.tests), async?} | state.modules]}}
  end

  def handle_cast({:test_finished, %ExUnit.Test{} = test}, state) do
    state = %{state | tests: [{test.module, test.name, test.time || 0} | state.tests]}
    {:noreply, record_outcome(state, test)}
  end

  def handle_cast({:suite_finished, _times}, state) do
    path = System.get_env("TIMING_OUT", @default_out)
    report_conformance(state)

    modules = Enum.sort_by(state.modules, fn {_, wall, _, _} -> -wall end)

    rows =
      Enum.map(modules, fn {name, wall, count, async?} ->
        "MODULE\t#{inspect(name)}\t#{div(wall, 1000)}\t#{count}\t#{async?}\n"
      end) ++
        (state.tests
         |> Enum.sort_by(fn {_, _, t} -> -t end)
         |> Enum.take(40)
         |> Enum.map(fn {mod, name, t} ->
           "TEST\t#{inspect(mod)}\t#{name}\t#{div(t, 1000)}\n"
         end))

    File.write!(path, rows)
    IO.puts("\n[timing] #{length(modules)} modules -> #{path}\n#{summary(modules)}")

    {:noreply, state}
  end

  def handle_cast(_event, state), do: {:noreply, state}

  # ---------- conformance report ----------

  # A capability's outcome is the worst thing that happened to it: one failure
  # makes the capability red however many of its tests passed, and a capability
  # is `deferred` only when nothing of it ran.
  @outcome_rank %{fail: 0, pass: 1, deferred: 2, untested: 3}

  defp record_outcome(state, %ExUnit.Test{tags: tags} = test) do
    case tags[:capability] do
      nil ->
        %{state | system?: state.system? or tags[:system] == true}

      capability ->
        %{
          state
          | system?: state.system? or tags[:system] == true,
            capabilities:
              Map.update(state.capabilities, capability, outcome(test), &worst(&1, outcome(test)))
        }
    end
  end

  defp outcome(%ExUnit.Test{state: nil}), do: :pass
  # A deferred capability rides `:skip` (`FlezhaTest.SystemCase`); an exclusion
  # means the run simply did not ask for that test.
  defp outcome(%ExUnit.Test{state: {:excluded, _}}), do: :untested
  defp outcome(%ExUnit.Test{state: {:skipped, _}}), do: :deferred
  defp outcome(%ExUnit.Test{state: {:invalid, _}}), do: :fail
  defp outcome(%ExUnit.Test{state: {:failed, _}}), do: :fail

  defp worst(a, b), do: Enum.min_by([a, b], &@outcome_rank[&1])

  # An excluded test still reaches `test_finished`, so a deferred capability is
  # reported whenever the run was aimed at a runtime or ran anything `:system`.
  defp report_conformance(state) do
    if state.system? or System.get_env("FLEZHA_E2E_RUNTIME") != nil do
      print_conformance(state)
    end
  end

  defp print_conformance(state) do
    runtime = NodeTestHelper.runtime()

    table =
      Map.new(NodeTestHelper.capabilities(), fn capability ->
        {capability, Map.get(state.capabilities, capability, :untested)}
      end)

    rows =
      table
      |> Enum.sort_by(fn {capability, outcome} -> {@outcome_rank[outcome], capability} end)
      |> Enum.map_join("\n", fn {capability, outcome} ->
        "  #{String.pad_trailing(Atom.to_string(capability), 20)} #{outcome}"
      end)

    IO.puts("\n[conformance] runtime=#{runtime}\n#{rows}")

    case System.get_env("CONFORMANCE_OUT") do
      nil ->
        :ok

      path ->
        # A sharded run's gate rows each write here: keep every capability's
        # worst outcome across them.
        table = Map.merge(written_outcomes(path), table, fn _, a, b -> worst(a, b) end)
        File.write!(path, Jason.encode!(%{runtime: runtime, capabilities: table}, pretty: true))
        IO.puts("[conformance] #{path}")
    end
  end

  defp written_outcomes(path) do
    if File.exists?(path) do
      Map.new(Jason.decode!(File.read!(path))["capabilities"], fn {capability, outcome} ->
        {String.to_existing_atom(capability), String.to_existing_atom(outcome)}
      end)
    else
      %{}
    end
  end

  defp summary(modules) do
    {async, sync} = Enum.split_with(modules, fn {_, _, _, a?} -> a? end)
    total = fn ms -> Enum.reduce(ms, 0, fn {_, w, _, _}, acc -> acc + w end) / 1_000_000 end

    top =
      modules
      |> Enum.take(5)
      |> Enum.map_join("\n", fn {name, wall, count, _} ->
        "  #{Float.round(wall / 1000, 0)} ms  #{count} tests  #{inspect(name)}"
      end)

    "[timing] async #{length(async)} modules #{Float.round(total.(async), 1)}s | " <>
      "sync #{length(sync)} modules #{Float.round(total.(sync), 1)}s\n" <> top
  end
end
