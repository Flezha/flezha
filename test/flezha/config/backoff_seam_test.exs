defmodule Flezha.Config.BackoffSeamTest do
  @moduledoc """
  The two tables are the only place in `lib/flezha` a wait or a duration is
  written.

  Eight clauses: the tree carries no ladder arithmetic and no millisecond
  literal beside them; every row is read; every derivation holds; the
  environment translates each row once; the SDK's rows equal the constants
  `flz_core` compiles; the ladder's own arithmetic behaves; and the deployment
  manifests carry the grace the table derives.

  The SDK rows pin the two ladders every SDK is held to, not every sleep in
  `flz_core`. Its UDS socket-connect poll, in
  `flz_core/src/command/streams_connect.rs`, is deliberately not a row: it is
  no shared default, and nothing observes it across SDKs.
  """
  use ExUnit.Case, async: true

  alias Flezha.Config.Backoff
  alias Flezha.Config.Durations
  alias Flezha.SDK.Knob
  alias FlezhaTest.Helper.SourceWalk

  @ladder_seam "lib/flezha/config/backoff.ex"
  @sdk_runner_server "sdk/elixir/lib/flezha/sdk/runner_server.ex"
  @duration_seam "lib/flezha/config/durations.ex"

  # The two shapes of the seam, as Erlang regular expressions: every spelling
  # the tree used for a backoff ladder and for a named duration before each was
  # folded into one place. Enforced on every run of the suite.
  @ladder_shape ~r/ProviderBehaviour\.retry_backoff_ms\(|backoff_with_jitter\(|wait_with_jitter\(|Integer\.pow\(2, ?(?:retries|attempt)|Bitwise\.bsl\(1, ?(?:min\(attempt|min\(n|attempt)|@backoffs_ms|@schedule_ms|@retry_base_ms|@retry_cap_ms|@retry_max_ms|@max_retry_delay_ms|@retry_base_delay_ms|Jitter\.jitter\(|@no_response_retry_initial_ms|@reconnect_cap_ms|@retry_backoff_cap_ms|@default_retry_backoff_ms|@stable_uptime_ms|@force_replace_throttle_ms|@park_resend_ms|@hub_retry_ms|@backoff_ms|jitter_fn\.\(/
  @duration_shape ~r/^\s*@[a-z_]+_ms\s+[0-9_]+|get_env\(:flezha, :[a-z_]+_ms, [0-9_]+\)/m

  defp lib_files(), do: SourceWalk.files(["lib/flezha"], [".ex"])

  defp offences(shape, allow) do
    for path <- lib_files(),
        path != allow,
        {line, number} <- Enum.with_index(String.split(File.read!(path), "\n"), 1),
        Regex.match?(shape, line),
        do: "#{path}:#{number}"
  end

  describe "the tree holds no ladder and no duration of its own" do
    test "a retry arithmetic outside the backoff table fails, naming file and line" do
      assert offences(@ladder_shape, @ladder_seam) == []
    end

    test "a millisecond literal outside the duration table fails, naming file and line" do
      assert offences(@duration_shape, @duration_seam) == []
    end
  end

  describe "every row is read" do
    test "every ladder key is named by a site" do
      sites = read_text(lib_files() -- [@ladder_seam])

      unread = for key <- Map.keys(Backoff.ladders()), not named?(sites, key), do: key
      assert unread == []
    end

    test "every duration row is named by a site, a derivation or the config that fills it" do
      readers =
        read_text(
          (lib_files() -- [@duration_seam]) ++
            SourceWalk.files(["config"], [".exs"]) ++ [__ENV__.file]
        )

      parents =
        for {_name, row} <- Durations.rows(),
            match?({_, _, _}, row.derived),
            do: elem(row.derived, 1),
            into: MapSet.new()

      unread =
        for name <- Map.keys(Durations.rows()),
            not named?(readers, name),
            not MapSet.member?(parents, name),
            do: name

      assert unread == []
    end
  end

  describe "a row's reset rule is a claim about its site" do
    test "every :on_success row announces its reset, and no :per_call row does" do
      sites = read_text(lib_files() -- [@ladder_seam])

      for {key, %{reset: reset}} <- Backoff.ladders() do
        announced? =
          String.contains?(sites, "Backoff.reset(:#{key}") or
            String.contains?(sites, "Backoff.new(:#{key})")

        case reset do
          :on_success ->
            assert announced?,
                   "#{key} declares reset: :on_success and no site announces one"

          :per_call ->
            refute announced?,
                   "#{key} declares reset: :per_call and a site announces a reset"
        end
      end
    end
  end

  describe "the derivations hold" do
    test "every derived row's parent exists and get/1 is the derivation" do
      for {name, %{derived: derived}} <- Durations.rows(), derived != nil do
        {kind, parent, value} = derived
        assert Durations.row(parent), "#{name} derives from #{parent}, which is not a row"

        expected =
          case kind do
            :factor -> Durations.get(parent) * value
            :headroom -> Durations.get(parent) + value
          end

        assert Durations.get(name) == expected
      end
    end

    test "a derived row carries no default of its own, so the relation cannot drift" do
      for {name, row} <- Durations.rows(), row.derived != nil do
        assert row.default == nil, "#{name} declares both a derivation and a default"
      end
    end
  end

  describe "the environment translates each row once" do
    test "runtime.exs reads every env-backed row through the table and carries no default" do
      runtime = File.read!("config/runtime.exs")

      for {name, %{env: env}} <- Durations.rows(), env != nil do
        # The SDK reads its own two rows from the environment directly; the
        # controller's config translates the rest.
        if String.contains?(runtime, env) or String.contains?(runtime, "Durations.env(:#{name})") do
          assert String.contains?(runtime, "Durations.env(:#{name})"),
                 "#{env} is read in runtime.exs without the row's default"
        end
      end

      refute Regex.match?(~r/get_env\("FLEZHA_[A-Z_]+_MS", "\d/, runtime),
             "runtime.exs carries a millisecond default the table owns"
    end
  end

  describe "the Elixir SDK's rows equal the constants the core compiles" do
    test "reconnect, retry, keepalive, heartbeat and the commit ack are one value per runtime" do
      channel = File.read!("flz_core/src/channel.rs")
      control_unit = File.read!("flz_core/src/control_unit.rs")
      commit = File.read!("flz_core/src/workflow/commit.rs")
      liveness = File.read!("flz_transport/src/transport/liveness.rs")
      shutdown = File.read!("flz_core/src/shutdown.rs")
      sleep = File.read!("flz_core/src/operative/sleep.rs")

      retry = Knob.ladder(:sdk_retry)
      reconnect = Knob.ladder(:sdk_reconnect)

      assert retry.cap == rust_const(channel, "RETRY_BACKOFF_MAX_MS")
      assert reconnect.cap == 30_000

      assert String.contains?(control_unit, "100 * 2u64.pow(attempt.min(8))"),
             "flz_core's reconnect ladder moved; the :sdk_reconnect row is pinned to it"

      assert reconnect.base == 100
      assert reconnect.exponent_cap == 8

      assert Durations.default(:sdk_commit_ack_ms) == rust_const(commit, "ACK_TIMEOUT_DEFAULT_MS")

      assert Durations.default(:control_keepalive_ms) ==
               rust_const(liveness, "DEFAULT_KEEPALIVE_MS")

      assert Durations.default(:data_heartbeat_ms) ==
               rust_const(liveness, "DEFAULT_DATA_HEARTBEAT_MS")

      assert Durations.default(:control_liveness_window_ms) ==
               rust_const(liveness, "DEFAULT_IDLE_TIMEOUT_MS")

      assert Durations.default(:sdk_spawn_reply_ms) == rust_const(shutdown, "REPLY_TIMEOUT")
      assert Durations.default(:sdk_owed_inbox_wait_ms) == rust_const(sleep, "OWED_INBOX_WAIT")
    end

    test "both runtimes reset the reconnect ladder on a completed handshake, and only there" do
      core = File.read!("flz_core/src/control_unit.rs")
      sdk = File.read!(@sdk_runner_server)

      core_rule? = String.contains?(core, "note_session_end(live.handshake_completed())")

      sdk_rule? =
        String.contains?(sdk, "defp complete_handshake(%State{handshake: :answered") and
          length(String.split(sdk, "Knob.reset(:sdk_reconnect")) == 2

      assert core_rule? and sdk_rule?,
             "the ladder's reset rule moved in one runtime alone: " <>
               "flz_core/src/control_unit.rs resets on the completed handshake: #{core_rule?}; " <>
               "#{@sdk_runner_server} does, and nowhere else: #{sdk_rule?}"
    end
  end

  describe "the ladder's arithmetic" do
    test "a rung row answers its list, and holds at the last rung" do
      for {key, %{rungs: rungs}} <- Backoff.ladders(), is_list(rungs) do
        for {rung, index} <- Enum.with_index(rungs, 1),
            do: assert(Backoff.delay(key, index) == rung)

        assert Backoff.delay(key, length(rungs) + 3) == List.last(rungs)
      end
    end

    test "a geometric row ascends to its cap and never past it" do
      for {key, %{rungs: nil} = row} <- Backoff.ladders() do
        delays = for attempt <- 1..12, do: Backoff.delay(key, attempt)
        cap = if is_atom(row.cap), do: Durations.get(row.cap), else: row.cap

        assert Enum.all?(delays, &(&1 <= cap)), "#{key} exceeded its cap"

        unless row.jitter do
          assert delays == Enum.sort(delays), "#{key} is not monotone in the attempt"
          assert hd(delays) == min(row.base, cap)
        end
      end
    end

    test "jitter is uniform in [d/2, 3d/2) and then capped" do
      assert Knob.jitter(1_000, fn _ -> 1 end) == 501
      assert Knob.jitter(1_000, fn max -> max end) == 1_500

      for {key, row} <- Backoff.ladders(), row.jitter, cap = cap_of(row) do
        for attempt <- 1..10 do
          delay = Backoff.delay(key, attempt)
          plain = Backoff.delay(key, attempt, rand: fn _ -> 1 end)
          assert delay >= div(plain - 1, 2) and delay <= cap
        end
      end
    end

    test "a give-up bound is the row's, and :never never gives up" do
      assert Backoff.give_up?(:no_response, 8)
      refute Backoff.give_up?(:no_response, 7)
      refute Backoff.give_up?(:heal, 1_000)
    end
  end

  describe "the two convergence rows are pinned by the clock they promise" do
    # fails when: the heal rungs are retuned and the Watchdog's compaction
    # handshake follows them. Compaction gives up by ATTEMPT COUNT, so its four
    # rungs ARE the wall clock it waits before abandoning a compaction — a
    # denser ladder would shorten that from about 395 s to about 32 s, which is
    # a change to durability behaviour and not to healing.
    test "the heal row walks twelve rungs and then holds at the top" do
      assert waits(:heal, 13) == [
               2_000,
               5_000,
               10_000,
               15_000,
               20_000,
               30_000,
               45_000,
               60_000,
               90_000,
               120_000,
               180_000,
               300_000,
               300_000
             ]
    end

    test "the compaction row keeps its four, and gives up only after the fourth" do
      assert waits(:compaction, 4) == [5_000, 30_000, 60_000, 300_000]
      refute Backoff.give_up?(:compaction, 3)
      assert Backoff.give_up?(:compaction, 4)
    end
  end

  # The waits `ladder` announces for its first `attempts` failures, read off
  # `[:flezha, :backoff, :wait]` — the published surface the rungs are a claim
  # about, rather than the table they are written in. The target is unique per
  # call so a row driven by another test in this async file cannot be read as
  # this one's.
  defp waits(ladder, attempts) do
    parent = self()
    target = {__MODULE__, ladder, System.unique_integer([:positive])}
    handler = {:pin, target}

    :telemetry.attach(
      handler,
      [:flezha, :backoff, :wait],
      fn _event, %{delay_ms: ms}, meta, _config ->
        if meta.target == target, do: send(parent, {:wait, target, ms})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    Enum.reduce(1..attempts, Backoff.new(ladder), fn _attempt, table ->
      Backoff.update(table, target, :error, 0)
    end)

    for _ <- 1..attempts do
      receive do
        {:wait, ^target, ms} -> ms
      after
        1_000 -> flunk("#{ladder} announced fewer than #{attempts} waits")
      end
    end
  end

  describe "the deployment manifests carry the derived grace" do
    test "the DaemonSet and both compose files equal orchestrator_grace_ms" do
      seconds = div(Durations.default(:orchestrator_grace_ms), 1_000)

      assert manifest_value(
               "kubernetes/flezha-daemonset.yaml",
               ~r/terminationGracePeriodSeconds:\s*(\d+)/
             ) ==
               seconds

      for file <- ["docker/prod/docker-compose.yml", "docker/dev/docker-compose.yml"] do
        assert manifest_value(file, ~r/stop_grace_period:\s*(\d+)s/) == seconds,
               "#{file} departs from the orchestrator_grace_ms row"
      end
    end
  end

  defp cap_of(%{cap: cap}) when is_atom(cap) and not is_nil(cap), do: Durations.get(cap)
  defp cap_of(%{cap: cap}), do: cap

  defp read_text(paths), do: SourceWalk.text(paths)

  defp named?(text, name), do: String.contains?(text, ":#{name}")

  # A `u64` millisecond constant, or a `Duration` of whole seconds read in milliseconds.
  defp rust_const(source, name) do
    case Regex.scan(~r/#{name}: u64 = ([0-9_]+)/, source) do
      [[_, ms]] ->
        digits(ms)

      [] ->
        secs =
          ~r/#{name}: (?:std::time::)?Duration = (?:std::time::)?Duration::from_secs\(([0-9_]+)\)/

        [[_, value]] = Regex.scan(secs, source)
        digits(value) * 1_000
    end
  end

  defp digits(value), do: value |> String.replace("_", "") |> String.to_integer()

  defp manifest_value(file, shape) do
    [[_, value]] = Regex.scan(shape, File.read!(file))
    String.to_integer(value)
  end
end
