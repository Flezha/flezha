defmodule Mix.Tasks.Bench.Report do
  @moduledoc """
  What a bench run reports: the header that situates a number, the preflight
  warnings that qualify it, the tables that present it, and the structured
  report that is the machine contract CI reads.

  Every bench task renders through here. A throughput figure is only worth
  comparing against another if both say what they ran, on what, built when,
  and under which dispatch topology — and a figure nobody can situate is the
  one that gets quoted anyway.

  The rendering is deliberately plain: aligned columns, one rule, no box
  drawing and no colour. The documented comparison workflow pipes bench output
  through `tee` and `diff`, and box characters make that diff noisy while
  adding nothing a rule does not.
  """

  alias Flezha.Observability.OtpBackend.ProcParser
  alias Flezha.Service.PortPilot

  @governor_path "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
  @unavailable "unavailable"

  # ---------------------------------------------------------------- render --

  @doc """
  An aligned table. `headers` are column labels; `rows` are lists of the same
  arity; `aligns` is `:left`/`:right` per column, defaulting to left.
  """
  @spec table([String.t()], [[String.t()]], [:left | :right]) :: String.t()
  def table(headers, rows, aligns \\ []) do
    cells = [headers | rows]
    widths = widths(cells)
    aligns = Enum.map(0..(length(headers) - 1)//1, &Enum.at(aligns, &1, :left))

    [
      render_row(headers, widths, aligns),
      widths |> Enum.map(&String.duplicate("─", &1)) |> Enum.join("  ")
      | Enum.map(rows, &render_row(&1, widths, aligns))
    ]
    |> Enum.join("\n")
  end

  @doc """
  A two-column key/value block — the shape the header sections take, where a
  column header would say nothing the keys do not.
  """
  @spec kv([{String.t(), String.t()}]) :: String.t()
  def kv(pairs) do
    width = pairs |> Enum.map(fn {k, _} -> String.length(k) end) |> Enum.max(fn -> 0 end)

    Enum.map_join(pairs, "\n", fn {k, v} ->
      "  " <> String.pad_trailing(k, width) <> "  " <> to_string(v)
    end)
  end

  @doc "Print `title` over `body`, indented, with a trailing blank line."
  @spec section(String.t(), String.t()) :: :ok
  def section(_title, ""), do: :ok

  def section(title, body) do
    IO.puts("#{title}\n#{indent(body)}\n")
  end

  defp indent(body) do
    body
    |> String.split("\n")
    |> Enum.map_join("\n", fn
      "  " <> _ = line -> line
      line -> "  " <> line
    end)
  end

  defp widths(cells) do
    cells
    |> Enum.zip_with(& &1)
    |> Enum.map(fn column -> column |> Enum.map(&String.length/1) |> Enum.max() end)
  end

  defp render_row(cells, widths, aligns) do
    [cells, widths, aligns]
    |> Enum.zip_with(fn [cell, width, align] ->
      case align do
        :right -> String.pad_leading(cell, width)
        :left -> String.pad_trailing(cell, width)
      end
    end)
    |> Enum.join("  ")
    |> String.trim_trailing()
  end

  # ------------------------------------------------------------- formatting --

  @doc """
  A float as text with a fixed number of decimals; anything else passes
  through. Nothing renders a measured float by interpolation: Elixir writes a
  float of magnitude 1000 or more that lands on a round value in scientific
  notation, so `1000.0` reaches a terminal and a `diff` as `1.0e3`.
  """
  @spec decimal(term(), pos_integer()) :: String.t()
  def decimal(value, decimals \\ 1)

  def decimal(value, decimals) when is_float(value),
    do: :erlang.float_to_binary(value, decimals: decimals)

  def decimal(value, _decimals), do: to_string(value)

  @doc "Microseconds as milliseconds, one decimal unless the row needs more."
  @spec ms(number(), pos_integer()) :: String.t()
  def ms(microseconds, decimals \\ 1), do: decimal(microseconds / 1000, decimals)

  @doc "Nanoseconds as milliseconds, one decimal unless the row needs more."
  @spec ms_from_ns(number(), pos_integer()) :: String.t()
  def ms_from_ns(nanoseconds, decimals \\ 1), do: ms(nanoseconds / 1000, decimals)

  @doc "A rate as a whole number with thousands separators."
  @spec rate(number()) :: String.t()
  def rate(value) do
    value
    |> round()
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1,")
    |> String.reverse()
  end

  @doc """
  The spread of a set of measurements as `max/min`, or `nil` when there is
  nothing to compare. A scaling sweep's headline.
  """
  @spec ratio([number()]) :: float() | nil
  def ratio(values) when length(values) < 2, do: nil

  def ratio(values) do
    case Enum.min(values) do
      min when min > 0 -> Float.round(Enum.max(values) / min, 2)
      _ -> nil
    end
  end

  @doc "A byte count in the largest unit that keeps it above 1."
  @spec bytes(non_neg_integer()) :: String.t()
  def bytes(n) when n < 1024, do: "#{n} B"

  def bytes(n) do
    {value, unit} =
      Enum.reduce_while([{1024 ** 3, "GiB"}, {1024 ** 2, "MiB"}, {1024, "KiB"}], nil, fn
        {scale, unit}, _ when n >= scale -> {:halt, {n / scale, unit}}
        _, acc -> {:cont, acc}
      end)

    "#{:erlang.float_to_binary(value, decimals: 1)} #{unit}"
  end

  # -------------------------------------------------------------- lifecycle --
  #
  # A bench run is opened before the controller boots, emitted once the fleet
  # is up (the artifacts block reports what the run will actually launch, and
  # its build has only just produced it), and finished when the numbers exist.
  # That sequence lives here, once, with its process-dictionary keys, its
  # default payload and its log-level lookup.

  @doc """
  Open a run: record its title and header rows, mint its id, and apply the
  logger level. Called at argument-parse time, before the controller boots.
  """
  @spec open(String.t(), [{String.t(), String.t()}]) :: :ok
  def open(title, run) do
    Process.put(:bench_report, %{title: title, run: run, id: run_id(), artifacts: []})
    apply_log_level()
  end

  @doc """
  Apply `FLEZHA_BENCH_LOG` (default `warning`) to the logger.

  Called again after `app.start`, which reloads the level from config.
  Nothing here filters a log line: a warning a bench provokes is either a real
  condition to fix at its source, or evidence that a classifier is wrong.
  """
  @spec apply_log_level() :: :ok
  def apply_log_level() do
    level =
      case System.get_env("FLEZHA_BENCH_LOG") do
        nil -> :warning
        name -> String.to_existing_atom(name)
      end

    Logger.configure(level: level)
    Application.put_env(:logger, :level, level)
  end

  @doc """
  Record what a launched runtime's artifact turned out to be. Called by the
  fleet once it has built one, because that is when the artifact on disk is
  the one the run will launch.
  """
  @spec record_artifact(map()) :: :ok
  def record_artifact(entry) do
    state = state()
    Process.put(:bench_report, Map.update(state, :artifacts, [entry], &(&1 ++ [entry])))
    :ok
  end

  @doc """
  Print the run, system, artifacts and warnings blocks, once. `ctx` carries
  what only the calling run knows — today `:durability`; the artifacts come
  from `record_artifact/1`.
  """
  @spec emit(map()) :: :ok
  def emit(ctx \\ %{}) do
    state = state()

    if state[:header] do
      :ok
    else
      artifacts = state[:artifacts] || []
      run = state[:run] || []
      system = system()

      IO.puts("=== #{state[:title]} ===\n")
      section("run", kv(run))
      section("system", kv(system_rows(system)))
      section("artifacts", artifacts_table(artifacts))

      warnings =
        ctx
        |> Map.merge(%{system: system, artifacts: artifacts})
        |> warnings()
        |> print_warnings()

      header = %{
        run: Map.new(run, fn {k, v} -> {k, to_string(v)} end),
        system: system,
        artifacts: artifacts,
        warnings: warnings
      }

      Process.put(:bench_report, Map.put(state, :header, header))
      :ok
    end
  end

  @doc """
  Write the run's report. `name_or_path` is either a bare stem — placed under
  `bench/reports/` and stamped with the run id — or a full path, for a report
  with consumers at a fixed location.
  """
  @spec finish(String.t(), map()) :: String.t()
  def finish(name_or_path, sections) do
    state = state()

    path =
      if String.ends_with?(name_or_path, ".json"),
        do: name_or_path,
        else: Path.join("bench/reports", "#{name_or_path}-#{state[:id] || run_id()}.json")

    write(path, Map.merge(state[:header] || empty_header(), sections))
  end

  @doc "This run's id — stable within the run, distinct between runs."
  @spec id() :: String.t()
  def id(), do: state()[:id] || run_id()

  defp state(), do: Process.get(:bench_report, %{})

  defp empty_header(), do: %{run: %{}, system: %{}, artifacts: [], warnings: []}

  @doc """
  One artifacts-block entry. `resolution` is `PortPilot.resolve_artifact/2`'s
  answer and `build` is `:built | :skipped | :none` — an artifact that exists
  is not an artifact that is current, and only the run knows which it had.
  """
  @spec artifact(atom(), {String.t(), :first | {:fallback, String.t()}}, atom()) :: map()
  def artifact(runtime, {path, resolution}, build) do
    {size, mtime} =
      case File.stat(path, time: :posix) do
        {:ok, %{size: size, mtime: mtime}} -> {size, mtime}
        _ -> {nil, nil}
      end

    %{
      runtime: to_string(runtime),
      path: Path.relative_to_cwd(path),
      size_bytes: size,
      mtime: mtime,
      build: to_string(build),
      fallback_for:
        case resolution do
          {:fallback, first} -> Path.relative_to_cwd(first)
          :first -> nil
        end
    }
  end

  defp artifacts_table([]), do: ""

  defp artifacts_table(artifacts) do
    table(
      ["runtime", "artifact", "size", "modified", "build"],
      Enum.map(artifacts, fn a ->
        [
          a.runtime,
          a.path <> if(a.fallback_for, do: " (fallback for #{a.fallback_for})", else: ""),
          if(a.size_bytes, do: bytes(a.size_bytes), else: @unavailable),
          stamp(a.mtime),
          a.build
        ]
      end),
      [:left, :left, :right, :left, :left]
    )
  end

  defp stamp(nil), do: @unavailable

  defp stamp(posix) do
    posix |> DateTime.from_unix!() |> Calendar.strftime("%Y-%m-%d %H:%M")
  end

  @doc """
  How `runtime` dispatches, read from its conformance manifest, with the
  current value of every environment variable the manifest says selects it.
  """
  @spec dispatch_note(atom()) :: String.t()
  def dispatch_note(runtime) do
    dispatch = PortPilot.dispatch(runtime)

    selected =
      for {var, _means} <- dispatch["env"] || %{} do
        "#{var}=#{System.get_env(var) || "default"}"
      end

    Enum.join([dispatch["model"] || @unavailable | selected], ", ")
  end

  # ----------------------------------------------------------------- system --

  @doc """
  What this machine is. Every field degrades to `#{@unavailable}` rather than
  raising: a bench must run on a host whose `/proc` does not look like Linux's.
  """
  @spec system() :: map()
  def system() do
    cpu = read_proc("/proc/cpuinfo", &ProcParser.parse_cpuinfo/1)
    mem = read_proc("/proc/meminfo", &ProcParser.parse_meminfo/1)
    load = read_proc("/proc/loadavg", &ProcParser.parse_loadavg/1)
    {sha, dirty?} = git()

    %{
      cpu_model: cpu[:model],
      cores: :erlang.system_info(:logical_processors),
      mem_total_bytes: mem[:mem_total_bytes],
      load_1m: load[:load_1m],
      cpu_governor: governor(),
      otp_release: List.to_string(:erlang.system_info(:otp_release)),
      elixir_version: System.version(),
      git_sha: sha,
      git_dirty: dirty?
    }
  end

  defp system_rows(s) do
    [
      {"cpu",
       "#{s.cpu_model || @unavailable} (#{s.cores} logical cores, governor #{s.cpu_governor || @unavailable})"},
      {"memory", if(s.mem_total_bytes, do: bytes(s.mem_total_bytes), else: @unavailable)},
      {"load (1m)", if(s.load_1m, do: to_string(s.load_1m), else: @unavailable)},
      {"otp / elixir", "#{s.otp_release} / #{s.elixir_version}"},
      {"git", "#{s.git_sha || @unavailable}#{if s.git_dirty, do: " (dirty)", else: ""}"}
    ]
  end

  @doc """
  The version of the toolchain that runs `runtime`, from the version command
  its conformance manifest declares. One short shell-out per run; a toolchain
  that is not installed reports `#{@unavailable}` rather than failing a bench
  that is already under way.

  Read from the manifest for the same reason the dispatch model and the launch
  line are: a per-runtime clause here is a list a new SDK has to remember to
  join, and the manifest is where a runtime already says what it is.
  """
  @spec toolchain(atom()) :: String.t()
  def toolchain(runtime) do
    case PortPilot.manifest(runtime)["toolchain"] do
      command when is_binary(command) -> first_line(command)
      _ -> @unavailable
    end
  end

  defp first_line(command) do
    case System.cmd("sh", ["-c", command], stderr_to_stdout: true) do
      {out, 0} -> out |> String.split("\n") |> hd() |> String.trim()
      _ -> @unavailable
    end
  end

  defp read_proc(path, parser) do
    with {:ok, text} <- File.read(path),
         {:ok, parsed} <- parser.(text) do
      parsed
    else
      _ -> %{}
    end
  end

  defp governor() do
    case File.read(@governor_path) do
      {:ok, value} -> String.trim(value)
      _ -> nil
    end
  end

  defp git() do
    case {first_line("git rev-parse --short HEAD"), first_line("git status --porcelain")} do
      {@unavailable, _} -> {nil, false}
      {sha, status} -> {sha, status != ""}
    end
  end

  # --------------------------------------------------------------- warnings --

  @doc """
  Conditions that make a number harder to trust, printed above it and carried
  in the report. Never fatal: a number produced on an unfit box is still that
  box's number, and refusing would break a CI runner whose governor and load
  are not ours to set.

  `ctx` carries what only the calling run knows: `:artifacts` (from
  `artifact/3`) and `:durability`.
  """
  @spec warnings(map()) :: [String.t()]
  def warnings(ctx) do
    system = Map.get_lazy(ctx, :system, &system/0)
    artifacts = Map.get(ctx, :artifacts, [])

    [
      governor_warning(system),
      load_warning(system),
      mix_env_warning(),
      dirty_warning(system),
      durability_warning(Map.get(ctx, :durability)),
      skip_build_warning()
    ]
    |> Enum.concat(Enum.flat_map(artifacts, &artifact_warnings/1))
    |> Enum.reject(&is_nil/1)
  end

  @doc "Print `warnings/1`'s result, if any, and return it."
  @spec print_warnings([String.t()]) :: [String.t()]
  def print_warnings([]), do: []

  def print_warnings(warnings) do
    section("warnings", Enum.map_join(warnings, "\n", &("! " <> &1)))
    warnings
  end

  defp governor_warning(%{cpu_governor: g}) when is_binary(g) and g != "performance",
    do: "cpu governor is #{g}, not performance — clock scaling makes this number irreproducible"

  defp governor_warning(_), do: nil

  defp load_warning(%{load_1m: load, cores: cores})
       when is_number(load) and is_integer(cores) and cores > 0 do
    if load > cores,
      do: "load average #{load} exceeds #{cores} cores — the box is contended"
  end

  defp load_warning(_), do: nil

  defp mix_env_warning() do
    env = Mix.env()

    if env != :prod,
      do: "MIX_ENV=#{env}, not prod — the controller is not running the code it ships"
  end

  defp dirty_warning(%{git_dirty: true}),
    do: "working tree is dirty — this number cannot be attributed to a commit"

  defp dirty_warning(_), do: nil

  defp durability_warning(backend) when backend in [nil, "none", :none],
    do: "durability backend is off — the number excludes the cost of persisting anything"

  defp durability_warning(_), do: nil

  defp skip_build_warning() do
    if System.get_env("FLEZHA_BENCH_SKIP_BUILD") in ["1", "true"],
      do:
        "FLEZHA_BENCH_SKIP_BUILD is set — the artifact measured is whatever was on disk, " <>
          "which is not necessarily the tree you are in"
  end

  defp artifact_warnings(%{} = a) do
    [
      if(a.fallback_for,
        do:
          "the #{a.runtime} runner is #{a.path} because #{a.fallback_for} does not exist — " <>
            "build the profile's own artifact"
      ),
      if(String.contains?(a.path, "/debug/") or String.contains?(a.path, "_build/dev/"),
        do: "the #{a.runtime} artifact #{a.path} is a debug build — it is not a bench artifact"
      )
    ]
  end

  # ----------------------------------------------------------------- report --

  # Write the payload and name the file. `finish/2` is the caller; a report
  # with consumers at a fixed location passes that path straight through.
  defp write(path, payload) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, Jason.encode!(payload, pretty: true))
    IO.puts("\nReport: #{path}")
    path
  end

  # Names the report file, so a sweep's runs do not overwrite each other.
  defp run_id() do
    DateTime.utc_now()
    |> Calendar.strftime("%Y%m%dT%H%M%S")
    |> Kernel.<>("-#{System.unique_integer([:positive])}")
  end
end
