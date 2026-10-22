defmodule Mix.Tasks.Bench.ReportTest do
  @moduledoc """
  The pure half of the bench report: rendering, formatting and the preflight
  warnings. A bench number is only comparable if the report that carries it is
  stable, so these assert the properties a reader and a `diff` depend on —
  fixed precision, alignment, and no raw float reaching the output.
  """

  use ExUnit.Case, async: true

  alias Flezha.Service.PortPilot
  alias Mix.Tasks.Bench.Report
  alias Mix.Tasks.Bench.Tally
  alias Mix.Tasks.Bench.Steps.Run

  describe "table/3" do
    test "columns align to their widest field and the rule matches" do
      rendered =
        Report.table(
          ["phase", "elapsed"],
          [["relay creation", "217.1 ms"], ["plugin setup", "42.3 ms"]],
          [:left, :right]
        )

      [header, rule, first, second] = String.split(rendered, "\n")

      # Every column starts at the same offset on every row.
      for line <- [header, first, second] do
        assert String.length(line) <= String.length(header) + 8
      end

      assert String.starts_with?(rule, String.duplicate("─", String.length("relay creation")))
      assert String.ends_with?(first, "217.1 ms")
      # Right-aligned: the shorter value is padded on the left, so both end
      # at the same column.
      assert String.length(first) == String.length(second)
    end

    test "no box drawing survives — a diff of two runs must show only the numbers" do
      rendered = Report.table(["a", "b"], [["1", "2"]])

      for glyph <- ["│", "┌", "┐", "└", "┘", "├", "┤", "┬", "┴", "┼", "|"] do
        refute String.contains?(rendered, glyph), "#{glyph} would make a tee/diff noisy"
      end
    end

    test "a row is never padded past its last field" do
      rendered = Report.table(["phase", "note"], [["send", ""]])
      assert rendered |> String.split("\n") |> List.last() == "send"
    end
  end

  describe "formatting" do
    test "a duration renders to one decimal, never as a raw float" do
      # The figure that prompted this: `12281.683999999998 ms`.
      assert Report.ms(12_281_683.999999998) == "12281.7"
      assert Report.ms(0) == "0.0"
      assert Report.ms_from_ns(1_500_000) == "1.5"

      # A window that lands on a round millisecond: Elixir renders a float of
      # magnitude 1000 or more that lands on a round value as `1.0e3`.
      assert Report.ms(1_000_000) == "1000.0"
      assert Report.ms_from_ns(1_000_000_000) == "1000.0"
    end

    test "a row that needs more resolution asks for decimals, not its own rounding" do
      assert Report.ms_from_ns(1_000_000_000, 2) == "1000.00"
      assert Report.ms_from_ns(1_234_567, 3) == "1.235"
      assert Report.ms(1_000_000, 3) == "1000.000"

      # `decimal/2` is the same rule without the millisecond conversion: the
      # `BENCH_RESULT` line carries floats that are not durations, beside
      # atoms and integers that pass through.
      assert Report.decimal(1000.0, 2) == "1000.00"
      assert Report.decimal(0.35, 2) == "0.35"
      assert Report.decimal(:clean, 2) == "clean"
      assert Report.decimal(42, 2) == "42"
    end

    test "the steps task's own latency line never falls into scientific notation" do
      # `max` is the exact observation, not a bucket midpoint, so a round
      # millisecond is reachable — which is the only input that shows this.
      tally = Tally.record({0, 1_000_000_000}, Tally.new(1))
      line = Tally.latency_line(tally)

      refute line =~ ~r/\de\d/
      assert line =~ "max 1000.00 ms"
    end

    test "a rate and a per-unit size reach text through the helper, not by interpolation" do
      # `Run.rate/2` feeds a printed line AND the `metrics:` block, so the
      # rendering belongs at the interpolation while the value stays a float —
      # a fixed-decimal string is not a number a summariser can take a median
      # over. 100 000 in one second is the input that shows it: `1.0e5`.
      rate = Run.rate(100_000, 1_000_000_000)

      assert is_float(rate)
      assert Report.decimal(rate) == "100000.0"

      # The other two members of the family: a per-sleeper size and a
      # per-steps count, both of which land on a round thousand.
      assert Report.decimal(1_024_000 / 1024, 1) == "1000.0"
      assert Report.decimal(2000 / 2, 2) == "1000.00"

      for text <- [Report.decimal(rate), Report.decimal(1_024_000 / 1024, 1)] do
        refute text =~ ~r/\de\d/
      end
    end

    test "a rate is a whole number with thousands separators" do
      assert Report.rate(95_178.03) == "95,178"
      assert Report.rate(1_000_000) == "1,000,000"
      assert Report.rate(999) == "999"
      assert Report.rate(0) == "0"
    end

    test "bytes pick the largest unit that keeps the value above one" do
      assert Report.bytes(512) == "512 B"
      assert Report.bytes(2048) == "2.0 KiB"
      assert Report.bytes(33_522_520_064) == "31.2 GiB"
    end

    test "a ratio needs two measurements and a non-zero floor" do
      assert Report.ratio([12_119]) == nil
      assert Report.ratio([]) == nil
      assert Report.ratio([1000, 4000]) == 4.0
      assert Report.ratio([4000, 1000]) == 4.0
      assert Report.ratio([0, 4000]) == nil
    end
  end

  describe "kv/1" do
    test "keys pad to a common width and an empty list renders empty" do
      assert Report.kv([]) == ""

      assert Report.kv([{"cpu", "x"}, {"memory", "y"}]) ==
               "  cpu     x\n  memory  y"
    end
  end

  describe "warnings/1" do
    test "an unfit box names every reason, and none of them abort" do
      warnings =
        Report.warnings(%{
          system: %{cpu_governor: "powersave", load_1m: 12.0, cores: 8, git_dirty: true},
          durability: "none",
          artifacts: []
        })

      assert {:box, _} = Enum.find(warnings, &(elem(&1, 1) =~ "powersave"))
      assert {:box, _} = Enum.find(warnings, &(elem(&1, 1) =~ "load average"))
      assert {:box, _} = Enum.find(warnings, &(elem(&1, 1) =~ "dirty"))
      assert {:workload, text} = Enum.find(warnings, &(elem(&1, 1) =~ "durability"))
      # A stateful shape still fsyncs locally with the tier off.
      assert text =~ "remote durability tier is off"
      refute text =~ "persisting anything"
    end

    test "a store the bench cannot clear is a workload warning" do
      warnings = Report.warnings(%{system: %{}, durability: "s3", durability_kept: true})
      assert [{:workload, text}] = Enum.reject(warnings, &(elem(&1, 1) =~ "MIX_ENV"))
      assert text =~ "may be restored"
    end

    test "a fit box warns about nothing it need not" do
      warnings =
        Report.warnings(%{
          system: %{cpu_governor: "performance", load_1m: 0.5, cores: 8, git_dirty: false},
          durability: "postgres",
          artifacts: []
        })

      # `MIX_ENV` is `test` under the suite, so exactly that one condition
      # holds and nothing else does.
      assert [{:box, text}] = warnings
      assert text =~ "MIX_ENV"
    end

    test "a fallback or debug artifact is called out by name" do
      warnings =
        Report.warnings(%{
          system: %{cpu_governor: "performance", load_1m: 0.5, cores: 8, git_dirty: false},
          durability: "postgres",
          artifacts: [
            %{
              runtime: "rust",
              path: "target/debug/rust_runner",
              fallback_for: "target/release/rust_runner"
            }
          ]
        })

      texts = for {:box, text} <- warnings, do: text
      assert Enum.any?(texts, &(&1 =~ "target/release/rust_runner does not exist"))
      assert Enum.any?(texts, &(&1 =~ "is a debug build"))
    end

    test "an absent system field never raises" do
      assert is_list(Report.warnings(%{system: %{}, artifacts: []}))
    end
  end

  describe "artifact/3" do
    test "records what was resolved, including that it was a fallback" do
      entry = Report.artifact(:rust, {"mix.exs", {:fallback, "nope.exs"}}, :built)

      assert entry.runtime == "rust"
      assert entry.path == "mix.exs"
      assert entry.build == "built"
      assert entry.fallback_for == "nope.exs"
      assert entry.size_bytes > 0
      assert is_integer(entry.mtime)
    end

    test "a path that does not exist reports no size rather than raising" do
      entry = Report.artifact(:rust, {"no/such/artifact", :first}, :skipped)

      assert entry.size_bytes == nil
      assert entry.mtime == nil
      assert entry.fallback_for == nil
    end
  end

  defp tmp_report_path() do
    path =
      Path.join(
        System.tmp_dir!(),
        "flezha-bench-report-#{System.unique_integer([:positive])}/report.json"
      )

    on_exit(fn -> File.rm_rf!(Path.dirname(path)) end)
    path
  end

  describe "the run lifecycle" do
    test "open → emit → finish carries the header into the report" do
      path = tmp_report_path()

      output =
        ExUnit.CaptureIO.capture_io(fn ->
          Report.open("test bench", [{"runtime", "rust"}, {"shape", "regression"}])
          Report.emit(%{durability: "none"})

          assert Report.finish(path, %{
                   metrics: %{throughput_msg_per_sec: 150_348, first_out_ms: 1000.0}
                 }) == path
        end)

      assert output =~ "=== test bench ==="
      assert output =~ "runtime"
      assert output =~ "Report: #{path}"

      decoded = path |> File.read!() |> Jason.decode!()

      # The four header blocks plus metrics — the shape CI reads.
      assert Map.keys(decoded) |> Enum.sort() == ~w(artifacts metrics run system warnings)
      assert decoded["run"]["runtime"] == "rust"
      assert decoded["system"]["cores"] == :erlang.system_info(:logical_processors)
      assert decoded["metrics"]["throughput_msg_per_sec"] == 150_348
      # A measurement the report ships stays a JSON number: `summarize.py` takes
      # a median over this field and a fixed-decimal string is not one.
      assert decoded["metrics"]["first_out_ms"] === 1000.0
      # The run block's own context reaches the warnings.
      assert Enum.any?(decoded["warnings"], &(&1 =~ "durability"))
    end

    test "strict refuses a box warning after printing it" do
      output =
        ExUnit.CaptureIO.capture_io(fn ->
          Report.open("test bench", [{"runtime", "rust"}])

          # `MIX_ENV` is `test` under the suite: a box warning always holds.
          error = assert_raise Mix.Error, fn -> Report.emit(%{durability: "s3", strict: true}) end
          assert error.message =~ "--strict"
          assert error.message =~ "MIX_ENV"
          refute error.message =~ "durability"
        end)

      assert output =~ "! MIX_ENV=test"
    end

    test "a warning learned after measuring is printed and reported" do
      path = tmp_report_path()

      output =
        ExUnit.CaptureIO.capture_io(fn ->
          Report.open("test bench", [{"runtime", "rust"}])
          Report.emit(%{durability: "none"})
          Report.warn("the paced sender fell behind")
          Report.finish(path, %{metrics: %{}})
        end)

      assert output =~ "! the paced sender fell behind"
      warnings = path |> File.read!() |> Jason.decode!() |> Map.fetch!("warnings")
      assert List.last(warnings) == "the paced sender fell behind"
      assert Enum.any?(warnings, &(&1 =~ "durability"))
    end

    test "emit is idempotent — a per-pool sweep prints one header" do
      output =
        ExUnit.CaptureIO.capture_io(fn ->
          Report.open("test bench", [{"runtime", "rust"}])
          Report.emit()
          Report.emit()
          Report.emit()
        end)

      assert output |> String.split("=== test bench ===") |> length() == 2
    end

    test "finish stamps a bare stem with the run id and places it under local_dev/bench/reports" do
      ExUnit.CaptureIO.capture_io(fn ->
        Report.open("test bench", [])
        Report.emit()
        path = Report.finish("stem-#{System.unique_integer([:positive])}", %{metrics: %{}})
        on_exit(fn -> File.rm_rf!(path) end)

        assert Path.dirname(path) == "local_dev/bench/reports"
        assert File.exists?(path)
      end)
    end
  end

  describe "dispatch_note/1" do
    test "reads the model from the manifest, not from a clause here" do
      # Every SDK's topology comes from its own manifest; the gate keeps the
      # models inside a closed vocabulary.
      for runtime <- PortPilot.runtimes() do
        note = Report.dispatch_note(runtime)
        assert note =~ ~r/thread-pool|event-loop|process-per-cell/
      end
    end

    test "names each selecting variable and its current value" do
      assert Report.dispatch_note(:rust) =~ "FLEZHA_WORKER_POOL_SIZE=default"
      assert Report.dispatch_note(:elixir) =~ "thread-pool, FLEZHA_WORKER_POOL_MAX=default"
    end
  end

  describe "toolchain/1" do
    test "runs the version command the manifest declares" do
      # Elixir's own toolchain is by definition installed wherever this runs.
      assert Report.toolchain(:elixir) =~ "Erlang/OTP"
    end

    test "a toolchain that is not installed is unavailable, not an error" do
      # The manifest names a real command; a host without it must still bench.
      assert is_binary(Report.toolchain(:rust))
    end
  end
end
