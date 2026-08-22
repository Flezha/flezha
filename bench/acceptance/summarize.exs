# Median-per-cell summary + regression compare / baseline save.
# Usage: mix run bench/acceptance/summarize.exs <results.tsv> <save:0|1>
[results_path, cells_path, baseline_path, save] = System.argv()
save? = save == "1"
dir = Path.dirname(results_path)
profile = results_path |> Path.basename() |> String.replace_prefix("results.", "") |> String.replace_suffix(".tsv", "")
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

# {name => %{runtime, throughputs: [..], p50s: [..], integrities: [..]}}
rows =
  results_path
  |> File.stream!()
  |> Enum.reduce(%{}, fn line, acc ->
    case String.split(String.trim_trailing(line), "\t") do
      [name, runtime | rest] ->
        # `rest` is one tab-column of space-joined `k=v` fields.
        f = rest |> Enum.join(" ") |> String.split() |> parse_fields.()
        num = fn v -> case v && Float.parse(v) do {n, _} -> n; _ -> nil end end
        tp = num.(Map.get(f, "throughput")) || 0.0
        p50 = num.(Map.get(f, "p50_ms"))
        integ = Map.get(f, "integrity", "stall")
        Map.update(acc, name, %{runtime: runtime, tp: [tp], p50: (p50 && [p50]) || [], integ: [integ]}, fn e ->
          %{e | tp: [tp | e.tp], p50: (p50 && [p50 | e.p50]) || e.p50, integ: [integ | e.integ]}
        end)

      _ ->
        acc
    end
  end)

median = fn [] -> 0.0
  xs ->
    s = Enum.sort(xs); n = length(s); mid = div(n, 2)
    if rem(n, 2) == 1, do: Enum.at(s, mid), else: (Enum.at(s, mid - 1) + Enum.at(s, mid)) / 2
end

# order by cells.tsv
order =
  cells_path |> File.stream!()
  |> Enum.flat_map(fn l ->
    l = String.trim(l)
    if l == "" or String.starts_with?(l, "#"), do: [], else: [l |> String.split("\t") |> hd()]
  end)

meds =
  Enum.map(order, fn name ->
    e = Map.get(rows, name, %{runtime: "?", tp: [0.0], p50: [], integ: ["stall"]})
    worst_integ = cond do
      "stall" in e.integ -> "stall"
      "loss" in e.integ -> "loss"
      true -> "clean"
    end
    {name, e.runtime, median.(e.tp), (e.p50 != [] && median.(e.p50)) || nil, worst_integ, length(e.tp)}
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
    integ != "clean" -> "❌ #{integ}"
    Map.has_key?(baseline, name) and baseline[name] > 0 ->
      drop = (baseline[name] - tp) / baseline[name] * 100
      cond do
        drop > 15 -> "❌ -#{Float.round(drop, 1)}% vs baseline #{Float.round(baseline[name], 1)}"
        drop > 5 -> "⚠ -#{Float.round(drop, 1)}%"
        drop < -5 -> "✅ +#{Float.round(-drop, 1)}%"
        true -> "✅ within #{Float.round(abs(drop), 1)}%"
      end
    Map.has_key?(baseline, name) -> "—"
    true -> "(no baseline)"
  end
end

header = "| cell | runtime | median throughput | p50 ms | integrity | n | vs baseline |\n|---|---|---|---|---|---|---|\n"
body =
  Enum.map_join(meds, "\n", fn {name, rt, tp, p50, integ, n} ->
    "| #{name} | #{rt} | #{Float.round(tp, 1)} | #{p50 || "—"} | #{integ} | #{n} | #{verdict.(name, tp, integ)} |"
  end)

any_fail = Enum.any?(meds, fn {name, _rt, tp, _p50, integ, _n} ->
  integ != "clean" or (Map.has_key?(baseline, name) and baseline[name] > 0 and (baseline[name] - tp) / baseline[name] * 100 > 15)
end)

md = "# Workflow bench acceptance — #{DateTime.utc_now() |> DateTime.to_string()}\n\n" <> header <> body <> "\n"
File.write!(summary_path, md)

IO.puts("")
IO.puts(header <> body)
IO.puts("")

if save? do
  File.write!(baseline_path, Enum.map_join(meds, "\n", fn {n, _rt, tp, _p50, _i, _c} -> "#{n}\t#{Float.round(tp, 2)}" end) <> "\n")
  IO.puts("Baseline saved to #{baseline_path} (#{length(meds)} cells).")
else
  IO.puts(if(any_fail, do: "RESULT: REGRESSION / INTEGRITY FAILURE", else: "RESULT: all cells within tolerance"))
  IO.puts("Summary written to #{summary_path}.")
end
