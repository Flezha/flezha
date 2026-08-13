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
  """
  use GenServer

  @default_out "/tmp/flezha_test_timing.tsv"

  @impl true
  def init(_opts), do: {:ok, %{mod_start: %{}, modules: [], tests: []}}

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
    {:noreply, %{state | tests: [{test.module, test.name, test.time || 0} | state.tests]}}
  end

  def handle_cast({:suite_finished, _times}, state) do
    path = System.get_env("TIMING_OUT", @default_out)

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
    IO.puts(:stderr, "\n[timing] #{length(modules)} modules -> #{path}\n#{summary(modules)}")

    {:noreply, state}
  end

  def handle_cast(_event, state), do: {:noreply, state}

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
