defmodule Flezha.Diagnostics.MemoryReporterTest do
  # async: false: the primary Logger; registered singletons; OS environment variables.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Diagnostics.MemoryReporter

  @env_var "FLEZHA_MEMORY_REPORT_INTERVAL_MS"

  describe "configured_interval_ms/0" do
    test "returns :disabled when env var unset" do
      System.delete_env(@env_var)
      assert MemoryReporter.configured_interval_ms() == :disabled
    end

    test "returns :disabled for non-positive or non-integer values" do
      for raw <- ["0", "-1", "abc", "1.5", "10ms", ""] do
        System.put_env(@env_var, raw)
        assert MemoryReporter.configured_interval_ms() == :disabled, "raw=#{inspect(raw)}"
      end

      System.delete_env(@env_var)
    end

    test "returns {:ok, ms} for a positive integer" do
      System.put_env(@env_var, "60000")
      assert MemoryReporter.configured_interval_ms() == {:ok, 60_000}
      System.delete_env(@env_var)
    end
  end

  describe "child_spec/1" do
    test "returns :ignore when env var unset and no explicit interval" do
      System.delete_env(@env_var)
      assert MemoryReporter.child_spec([]) == :ignore
    end

    test "returns a child spec when interval_ms is explicit" do
      spec = MemoryReporter.child_spec(interval_ms: 1_000)
      assert is_map(spec)
      assert spec.id == MemoryReporter
    end
  end

  describe "capture/0" do
    test "returns the documented keys with non-negative integer values" do
      reading = MemoryReporter.capture()

      for key <- [
            :erlang_memory_total,
            :erlang_memory_binary,
            :erlang_memory_processes,
            :erlang_memory_ets,
            :erlang_memory_atom,
            :erlang_memory_code,
            :process_count,
            :port_count,
            :binary_alloc_carriers_size
          ] do
        value = Map.fetch!(reading, key)
        assert is_integer(value), "#{key} not integer: #{inspect(value)}"
        assert value >= 0, "#{key} negative: #{value}"
      end

      assert reading.module == MemoryReporter
      # erlang_memory_total is the sum of multiple subsystems; this is a
      # weak invariant (allocator carriers can push it up), but a useful
      # tripwire against accidental zero/negative values.
      assert reading.erlang_memory_total > 0
    end
  end

  describe "to_log_line/1" do
    test "emits parseable key=value pairs on a single logical line" do
      reading = MemoryReporter.capture()
      line = MemoryReporter.to_log_line(reading) |> IO.iodata_to_binary()

      refute String.contains?(line, "\n")
      assert String.starts_with?(line, "module=Flezha.Diagnostics.MemoryReporter ")

      # Round-trip the line into a key-value map and confirm every
      # documented field is present and parses as an integer.
      parsed =
        line
        |> String.split(" ", trim: true)
        |> Enum.map(fn pair ->
          case String.split(pair, "=", parts: 2) do
            [k, v] -> {k, v}
            _ -> nil
          end
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.into(%{})

      for key <- [
            "erlang_memory_total",
            "erlang_memory_binary",
            "erlang_memory_processes",
            "erlang_memory_ets",
            "erlang_memory_atom",
            "erlang_memory_code",
            "process_count",
            "port_count",
            "binary_alloc_carriers_size"
          ] do
        assert Map.has_key?(parsed, key), "missing key=#{key} in line: #{line}"
        assert {_, ""} = Integer.parse(Map.fetch!(parsed, key))
      end
    end
  end

  describe "reporter GenServer" do
    test "handle_info(:report, _) stays alive and re-arms the timer" do
      {:ok, pid} = GenServer.start_link(MemoryReporter, %{interval_ms: 10_000})
      send(pid, :report)
      :sys.get_state(pid)
      assert Process.alive?(pid)
      assert :sys.get_state(pid) == %{interval_ms: 10_000}
      GenServer.stop(pid)
    end

    test "emits the reading line at :info so it reaches Loki" do
      # Test config pins the primary level to :error; lower it so the :info
      # line is emitted, then restore.
      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:info))

      {:ok, pid} = GenServer.start_link(MemoryReporter, %{interval_ms: 10_000})

      log =
        capture_log([level: :info], fn ->
          send(pid, :report)
          :sys.get_state(pid)
        end)

      GenServer.stop(pid)

      assert log =~ "[info]"
      assert log =~ "module=Flezha.Diagnostics.MemoryReporter "
    end
  end
end
