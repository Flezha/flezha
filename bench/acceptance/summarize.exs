# Median-per-case summary + regression compare / baseline save.
# Usage: mix run bench/acceptance/summarize.exs <results.tsv> <cases.tsv> <baseline.tsv> <save:0|1>
[results_path, cases_path, baseline_path, save] = System.argv()
save? = save == "1"
dir = Path.dirname(results_path)

profile =
  results_path
  |> Path.basename()
  |> String.replace_prefix("results.", "")
  |> String.replace_suffix(".tsv", "")

summary_path = Path.join(dir, "summary.#{profile}.md")

parse_fields = fn rest ->
  rest
  |> Enum.flat_map(fn kv ->
    case String.split(kv, "=", parts: 2) do
      [k, v] -> [{k, v}]
      _ -> []
    end
  end)
  |> Map.new()
end

# {name => %{runtime, unit, tp: [..], p50: [..], peak: [..], fsync: [..], integ: [..]}}
rows =
  results_path
  |> File.stream!()
  |> Enum.reduce(%{}, fn line, acc ->
    case String.split(String.trim_trailing(line), "\t") do
      [name, runtime | rest] ->
        # `rest` is one tab-column of space-joined `k=v` fields.
        f = rest |> Enum.join(" ") |> String.split() |> parse_fields.()

        num = fn v ->
          case v && Float.parse(v) do
            {n, _} -> n
            _ -> nil
          end
        end

        tp = num.(Map.get(f, "throughput")) || 0.0
        p50 = num.(Map.get(f, "p50_ms"))
        peak = num.(Map.get(f, "peak_mb"))
        fsync = num.(Map.get(f, "fsync_p99_ms"))
        unit = Map.get(f, "unit", "/s")
        integ = Map.get(f, "integrity", "stall")
        upd = fn l, v -> if(v, do: [v | l], else: l) end

        Map.update(
          acc,
          name,
          %{
            runtime: runtime,
            unit: unit,
            tp: [tp],
            p50: upd.([], p50),
            peak: upd.([], peak),
            fsync: upd.([], fsync),
            integ: [integ]
          },
          fn e ->
            %{
              e
              | unit: unit,
                tp: [tp | e.tp],
                p50: upd.(e.p50, p50),
                peak: upd.(e.peak, peak),
                fsync: upd.(e.fsync, fsync),
                integ: [integ | e.integ]
            }
          end
        )

      _ ->
        acc
    end
  end)

median = fn
  [] ->
    0.0

  xs ->
    s = Enum.sort(xs)
    n = length(s)
    mid = div(n, 2)
    if rem(n, 2) == 1, do: Enum.at(s, mid), else: (Enum.at(s, mid - 1) + Enum.at(s, mid)) / 2
end

# order cases as they appear in the profile's cases file
order =
  cases_path
  |> File.stream!()
  |> Enum.flat_map(fn l ->
    l = String.trim(l)
    if l == "" or String.starts_with?(l, "#"), do: [], else: [l |> String.split("\t") |> hd()]
  end)

meds =
  Enum.map(order, fn name ->
    e =
      Map.get(rows, name, %{
        runtime: "?",
        unit: "/s",
        tp: [0.0],
        p50: [],
        peak: [],
        fsync: [],
        integ: ["stall"]
      })

    worst_integ =
      cond do
        "stall" in e.integ -> "stall"
        "loss" in e.integ -> "loss"
        true -> "clean"
      end

    med = fn xs -> if(xs == [], do: nil, else: median.(xs)) end

    {name, e.runtime, med.(e.tp) || 0.0, e.unit, med.(e.p50), med.(e.peak), med.(e.fsync),
     worst_integ, length(e.tp)}
  end)

baseline =
  if File.exists?(baseline_path) do
    File.stream!(baseline_path)
    |> Enum.flat_map(fn l ->
      case String.split(String.trim(l), "\t") do
        [n, tp | _] when n != "" -> [{n, String.to_float(tp)}]
        _ -> []
      end
    end)
    |> Map.new()
  else
    %{}
  end

verdict = fn name, tp, integ ->
  cond do
    integ != "clean" ->
      "❌ #{integ}"

    Map.has_key?(baseline, name) and baseline[name] > 0 ->
      drop = (baseline[name] - tp) / baseline[name] * 100

      cond do
        drop > 15 -> "❌ -#{Float.round(drop, 1)}% vs #{Float.round(baseline[name], 1)}"
        drop > 5 -> "⚠ -#{Float.round(drop, 1)}%"
        drop < -5 -> "✅ +#{Float.round(-drop, 1)}%"
        true -> "✅ ~baseline (#{Float.round(-drop, 1)}%)"
      end

    Map.has_key?(baseline, name) ->
      "—"

    true ->
      "(no baseline)"
  end
end

# Higher throughput is better; lower latency/fsync/mem is better. Units live in
# each case so a reader never has to guess what 293.9 means.
opt = fn
  nil -> "—"
  v -> v
end

title =
  "Workflow bench — profile #{profile} — #{DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_string()}"

header =
  "| case | runtime | throughput (median) | latency p50 | peak mem | fsync p99 | integrity | runs | vs baseline |\n" <>
    "|---|---|---|---|---|---|---|---|---|\n"

body =
  Enum.map_join(meds, "\n", fn {name, rt, tp, unit, p50, peak, fsync, integ, n} ->
    "| #{name} | #{rt} | #{Float.round(tp, 1)} #{unit} | #{opt.(p50 && "#{Float.round(p50, 1)} ms")} " <>
      "| #{opt.(peak && "#{round(peak)} MiB")} | #{opt.(fsync && "#{round(fsync)} ms")} | #{integ} " <>
      "| #{n} | #{verdict.(name, tp, integ)} |"
  end)

legend = """

**What the columns mean**

- **throughput** — work finished per second: `workflows/s` (chain), `turns/s`
  (chat turns), `wakes/s` (sleep wake storm). Higher is better.
- **latency p50** — median end-to-end for that work: spawn→completed (chain),
  turn→reply (turns), wake→reply (sleep).
- **peak mem** — the controller BEAM's peak memory during the run (capacity
  signal; not the Rust/Java runner processes' RSS, and not CPU).
- **fsync p99** — the slowest Cell's WAL fsync p99. A high value means the
  **disk**, not the engine, is the ceiling.
- **integrity** — `clean` = every record accounted for; `loss` = a record went
  missing or duplicated (a bug, fails regardless of speed); `stall` = the case
  did not finish.
- **vs baseline** — median throughput against the saved baseline: ❌ if >15 %
  slower or not clean, ⚠ 5–15 % slower, ✅ within 5 % or faster. `(no baseline)`
  until you run `--save-baseline`.

Numbers drift ±20 % on a shared box — trust medians over `--repeat`, not one run.
"""

any_fail =
  Enum.any?(meds, fn {name, _rt, tp, _u, _p50, _peak, _fsync, integ, _n} ->
    integ != "clean" or
      (Map.has_key?(baseline, name) and baseline[name] > 0 and
         (baseline[name] - tp) / baseline[name] * 100 > 15)
  end)

md = "# #{title}\n\n" <> header <> body <> "\n" <> legend
File.write!(summary_path, md)

IO.puts("")
IO.puts(title)
IO.puts("")
IO.puts(header <> body)
IO.puts(legend)

if save? do
  File.write!(
    baseline_path,
    Enum.map_join(meds, "\n", fn {n, _rt, tp, _u, _p50, _pk, _fs, _i, _c} ->
      "#{n}\t#{Float.round(tp, 2)}"
    end) <> "\n"
  )

  IO.puts("Baseline saved to #{baseline_path} (#{length(meds)} cases).")
else
  IO.puts(
    if(any_fail,
      do: "RESULT: ❌ REGRESSION / INTEGRITY FAILURE",
      else: "RESULT: ✅ all cases within tolerance"
    )
  )

  IO.puts("Summary written to #{summary_path}.")
end
