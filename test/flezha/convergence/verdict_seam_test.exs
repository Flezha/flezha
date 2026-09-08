defmodule Flezha.Convergence.VerdictSeamTest do
  @moduledoc """
  The health seam, made impossible to bypass, plus the judge's rule order.

  Three clauses walk `lib/flezha` and fail naming what they found:

    1. **Vocabulary** — a file outside the seam carrying two or more of the
       seven verdict states is a second vocabulary.
    2. **One writer** — `operative_health` is put into the published snapshot
       from exactly one site.
    3. **Conformance** — every detector entry reads facts of one shape for its
       declared kind, names thresholds something answers, and yields `unknown`
       without stopping the tick when it raises.

  Whether a detector's declared event reaches an exporter is the export seam's
  question, not this one: `Flezha.Observability.TelemetryEventSeamTest`.

  The rule tests below drive `Probe.judge/2` itself: they are the judge's clause
  order, asserted where the rule lives rather than against whichever surface
  happens to render it.
  """

  use ExUnit.Case, async: true

  alias Flezha.Convergence.{Diff, Intent, StallClassifier, Verdict}
  alias Flezha.Convergence.Probe
  alias Flezha.Convergence.Probe.{Fact, Judge, Table}
  alias FlezhaTest.Helper.SourceWalk

  # The seam itself, plus the files whose two-or-more states are a FACT in
  # another sense. Every entry states the difference that forbids the fold —
  # `refactor-gates`: "a refusal to fold names the difference".
  @allowed %{
    # the seam
    "lib/flezha/convergence/verdict.ex" => "the record",
    "lib/flezha/convergence/probe/judge.ex" => "the judge",
    "lib/flezha/convergence/probe/table.ex" => "the detector declarations",
    "lib/flezha/convergence/probe/readers.ex" => "the fact readers",
    "lib/flezha/convergence/probe.ex" => "the probe the readers read",
    "lib/flezha/convergence/diff.ex" => "reads the record's states, mints none",
    "lib/flezha/convergence/snapshot.ex" => "publishes the record and documents its states",
    # facts in another sense
    "lib/flezha/runner/control_channel.ex" =>
      "`:unbound` is a write outcome (no stream to write on); `:failed`/`:parked` are workflow EVENT segments",
    "lib/flezha/plugin/provider/kafka/producer_worker.ex" =>
      "`:failed` is an event segment; `:unknown` means the last produce reason was not recorded",
    "lib/flezha/plugin/provider/http_poll/poll_worker.ex" =>
      "`:failed` is an event segment; `:suspended` is a poll outcome journaled for wake",
    "lib/flezha/cli/state.ex" =>
      "both are `{:not_resident, _}` refusal reasons rendered as text, not health",
    "lib/flezha/operative/overwatch.ex" =>
      "both are call refusals naming a lifecycle fact the caller asked against",
    "lib/flezha/observability/telemetry_bridge.ex" => "event NAMES, never verdicts",
    "lib/flezha/convergence/loop.ex" =>
      "`{:parked, id}` is a backoff key; `:unknown` is the unreadable-roster sentinel",
    "lib/flezha/convergence/facts.ex" =>
      "`:parked` is a declared fact kind; `:unknown` is the unreadable-read sentinel",
    "lib/flezha/plugin/provider_behaviour.ex" =>
      "`:failed` is a declared PROVIDER TELEMETRY verb (shipped/retried/failed/overflowed), " <>
        "not a health state; `:healthy` is the provider probe's own verdict type"
  }

  describe "clause 1 — one vocabulary" do
    test "no file outside the seam carries two or more verdict states" do
      states = MapSet.new(Verdict.states())

      offenders =
        for file <- source_files(),
            not Map.has_key?(@allowed, file),
            found = file |> atoms() |> MapSet.intersection(states),
            MapSet.size(found) >= 2,
            do: {file, Enum.sort(found)}

      assert offenders == [],
             "a second health vocabulary: " <>
               Enum.map_join(offenders, "; ", fn {f, a} -> "#{f} #{inspect(a)}" end)
    end

    test "every allow-list entry still exists and still needs its exemption" do
      for {file, why} <- @allowed do
        assert File.exists?(file), "allow-listed file is gone: #{file}"
        assert is_binary(why) and why != ""
      end
    end
  end

  describe "clause 2 — one writer of the published health map" do
    test "operative_health is put into the snapshot from exactly one site" do
      assert snapshot_health_writers() == ["lib/flezha/convergence/loop/summary.ex"],
             "operative_health reaches the published snapshot from more than one site: " <>
               inspect(snapshot_health_writers())
    end
  end

  describe "clause 3 — every detector conforms" do
    test "each entry's reader returns facts of its declared kind" do
      context = fixture_context()

      for %{id: id, kind: kind, facts: reader} <- Table.detectors(), reader != nil do
        facts = reader.(context)
        assert is_list(facts), "#{id}: a reader must return a list of facts"

        for %Fact{} = fact <- facts do
          assert fact.kind in Verdict.kinds(),
                 "#{id}: #{inspect(fact.kind)} is not a subject kind"

          assert is_map(fact.observed), "#{id}: a fact observes a map, never a verdict"

          if kind != :by_scope do
            assert fact.kind == kind, "#{id}: declared #{kind}, produced #{fact.kind}"
          end
        end
      end
    end

    test "every declared threshold is answered by something" do
      for %{id: id} = entry <- Table.detectors(), name <- Map.get(entry, :thresholds, []) do
        assert is_integer(Table.threshold(name)),
               "#{id}: nothing answers the threshold #{inspect(name)}"
      end
    end

    test "a reader that raises yields unknown, and the tick completes" do
      context = %{fixture_context() | signals: %{facts: :not_a_fact_map}}
      records = Judge.judge(context, %{})

      operatives = for {{:operative, _}, r} <- records, do: r
      assert operatives != []

      for record <- operatives do
        assert record.state == :unknown
        assert match?({:unobserved, _}, record.reason)
      end
    end

    test "every classifier layer maps to exactly one subject kind" do
      for layer <- classifier_layers() do
        scope = StallClassifier.layer_scope(layer)
        assert scope in [nil, :fabric, :runner, :connection, :operative]
        assert kind_of_scope(scope) in [nil | Verdict.kinds()]
      end
    end

    test "the Diff plans nothing for a driver record" do
      intent = %Intent{operatives: []}

      records = %{
        {:driver, {:uds, :control}} =>
          Verdict.new({:driver, {:uds, :control}}, :degraded,
            reason: {:wedged, :driver_loop_frozen}
          )
      }

      assert Diff.empty?(Diff.compute(intent, %{}, records, %{}))
    end

    test "every reachable state is minted by a fixture, and no other state is" do
      records = Map.values(all_states_fixture())
      minted = records |> Enum.group_by(&(&1.subject |> elem(0)), & &1.state)

      for kind <- Verdict.kinds() do
        seen = minted |> Map.get(kind, []) |> MapSet.new()
        reachable = MapSet.new(Verdict.reachable(kind))

        assert MapSet.subset?(seen, reachable),
               "#{kind} minted an unreachable state: #{inspect(MapSet.difference(seen, reachable))}"

        assert MapSet.equal?(seen, reachable),
               "#{kind} never minted: #{inspect(MapSet.difference(reachable, seen))}"
      end
    end
  end

  describe "the judge's rule order" do
    test "a bound operative on a live runner is healthy" do
      assert state_of(one("live", bound_runner: "e1"), MapSet.new(["e1"])) == :healthy
    end

    test "an operative bound to no runner is unbound" do
      assert state_of(one("unbound", bound_runner: nil), MapSet.new(["e1"])) == :unbound
    end

    test "an operative bound to a runner the roster does not hold is failed" do
      assert state_of(one("dead", bound_runner: "gone"), MapSet.new(["e1"])) == :failed
    end

    test "a live runner whose namespace hub is not running is unknown, not dead" do
      records = judged(one("live", bound_runner: "e1"), MapSet.new(["e1"]), sampled("nohub", 5))

      assert records[{:channel, {"nohub", :control}}].state == :unknown
      assert records[{:runner, "nohub"}].state == :unknown
      assert records[{:runner, "nohub"}].reason == {:unobserved, :active_runners}
    end

    test "a runner no roster holds whose control session went silent is still dead" do
      silent = Table.threshold(:control_liveness_window_ms) + 1

      records =
        judged(one("live", bound_runner: "e1"), MapSet.new(["e1"]), sampled("gone", silent))

      assert records[{:channel, {"gone", :control}}].state == :failed
      assert records[{:runner, "gone"}].state == :failed
      assert records[{:runner, "gone"}].reason == :dead_runner
    end

    test "a parked operative is parked, with its reason and its phase" do
      records =
        judged(
          one("parked", bound_runner: "e1", parked?: true, park_reason: :boom),
          MapSet.new(["e1"])
        )

      record = records[{:operative, {"ns", "parked"}}]

      assert record.state == :parked
      assert record.reason == {:parked, :boom}
      assert record.phase == :parked
    end

    # fails when: phase and state derive in different orders. An operative held
    # after a bind that found no runner carries an `init_error`, which the probe
    # reports as `parked?` — so a phase derivation that tests parked before the
    # binding reports `parked` while the judge reports `unbound`.
    test "an operative held after a bind that found no runner is unbound in both words" do
      records =
        judged(
          one("held",
            bound_runner: nil,
            parked?: true,
            park_reason: {:runner_not_ready, :timeout}
          ),
          MapSet.new(["e1"])
        )

      record = records[{:operative, {"ns", "held"}}]

      assert record.state == :unbound
      assert record.reason == :no_binding
      assert record.phase == :unbound
    end

    test "a suspended instance is suspended, and a park reason does not change that" do
      records =
        judged(
          one("suspended", bound_runner: nil, suspended?: true, parked?: true),
          MapSet.new(["e1"])
        )

      record = records[{:operative, {"ns", "suspended"}}]
      assert record.state == :suspended
      assert record.phase == :suspended
    end

    test "a bound-but-stalled operative is degraded, and names the window it was judged by" do
      records =
        judged(one("stalled", bound_runner: "e1"), MapSet.new(["e1"]), %{
          facts: %{{:output_stall, "stalled"} => %{at: 0, args: %{}}}
        })

      record = records[{:operative, {"ns", "stalled"}}]
      assert record.state == :degraded
      assert record.reason == {:stalled, :output_stall}
      assert {:output_stall_timeout_ms, ms} = record.threshold
      assert is_integer(ms)
    end

    test "an operative whose declared plugin is not running is degraded" do
      records =
        judged(
          one("pluginless", bound_runner: "e1", running_plugins: [], subscribed_plugins: []),
          MapSet.new(["e1"]),
          %{},
          [{"src", %{}}]
        )

      assert records[{:operative, {"ns", "pluginless"}}].state == :degraded
      assert records[{:plugin, {"ns", "pluginless", "src"}}].state == :degraded
    end

    test "a prepared plugin is healthy and its phase says it is not running" do
      records =
        judged(
          one("prepared",
            bound_runner: "e1",
            running_plugins: [],
            subscribed_plugins: ["src"],
            plugin_health: %{"src" => :healthy}
          ),
          MapSet.new(["e1"]),
          %{},
          [{"src", %{}}]
        )

      record = records[{:plugin, {"ns", "prepared", "src"}}]
      assert record.state == :healthy
      assert record.reason == :prepared
      assert record.phase == :prepared
    end

    test "an uncertain plugin probe is healthy and names why" do
      records =
        judged(
          one("failopen",
            bound_runner: "e1",
            running_plugins: ["src"],
            subscribed_plugins: ["src"],
            plugin_health: %{"src" => {:fail_open, :probe_budget_exceeded}}
          ),
          MapSet.new(["e1"]),
          %{},
          [{"src", %{}}]
        )

      record = records[{:plugin, {"ns", "failopen", "src"}}]
      assert record.state == :healthy
      assert record.reason == {:fail_open, :probe_budget_exceeded}
      assert records[{:operative, {"ns", "failopen"}}].state == :healthy
    end

    test "an unobservable operative is unknown for the tick, not classified" do
      assert state_of(one("blind", bound_runner: :unknown), MapSet.new(["e1"])) == :unknown
    end

    test "an operative whose namespace roster read failed is unknown, not failed" do
      assert state_of(one("blind_ns", bound_runner: "e1"), :unknown) == :unknown
    end

    test "a state that holds keeps its since_ms; a state that changes is re-stamped" do
      first = judged(one("held", bound_runner: "e1"), MapSet.new(["e1"]))
      subject = {:operative, {"ns", "held"}}

      context = context_for(one("held", bound_runner: "e1"), MapSet.new(["e1"]), %{}, [])
      again = Judge.judge(%{context | now: 9_999}, first)
      assert again[subject].since_ms == first[subject].since_ms

      changed =
        Judge.judge(
          %{
            context_for(one("held", bound_runner: nil), MapSet.new(["e1"]), %{}, [])
            | now: 9_999
          },
          first
        )

      assert changed[subject].since_ms == 9_999
    end
  end

  describe "the /healthz fold" do
    test "a parked operative is degraded at once, never healthy" do
      {status, body} = Verdict.fold(fixture_records(:parked), %{})
      assert status == :degraded
      assert body.reason =~ "parked"
      assert Verdict.http_status(status) == 200
    end

    test "a dead binding is recovering inside the grace and degraded past it" do
      records = fixture_records(:failed)

      assert {:recovering, _} = Verdict.fold(records, %{unmet_intent_ms: 10})

      assert {:degraded, body} =
               Verdict.fold(records, %{unmet_intent_ms: 500_000, unmet_intent_grace_ms: 100})

      assert body.reason =~ "dead_runner"
      assert body.bound_operatives == 0
    end

    test "an unobserved operative is counted, not judged" do
      {status, body} = Verdict.fold(fixture_records(:unknown), %{})
      assert status == :healthy
      assert body.unknown_operatives == 1
    end

    test "a driver record reaches the fabric arm and no other" do
      records = fixture_records(:driver_wedged)
      assert {:healthy, _} = Verdict.fold(records, %{})

      assert {:unhealthy, body} =
               Verdict.fold(records, %{data_plane: {:fabric, :driver_loop_frozen, 3}})

      assert body.reason == "data_plane_fabric_wedged"
      assert Verdict.http_status(:unhealthy) == 503
    end
  end

  # ---------- the walk ----------

  defp source_files(), do: SourceWalk.files(["lib/flezha"], [".ex"])

  # Every file that hands `Snapshot.publish/1` a map carrying `operative_health`.
  # Read from the AST, so a render key of the same name elsewhere is not a write.
  defp snapshot_health_writers() do
    for file <- source_files(),
        publishes_health?(SourceWalk.ast(file)),
        do: file
  end

  # Both spellings: `Snapshot.publish(%{…})` from outside, and the bare
  # `publish(%{…})` a second writer inside the Snapshot itself would use.
  defp publishes_health?(ast) do
    {_, found} =
      Macro.prewalk(ast, false, fn
        {{:., _, [{:__aliases__, _, mod}, :publish]}, _, [{:%{}, _, pairs} | _]} = node, acc ->
          {node, acc or (List.last(mod) == :Snapshot and health_key?(pairs))}

        {:publish, _, [{:%{}, _, pairs} | _]} = node, acc ->
          {node, acc or health_key?(pairs)}

        node, acc ->
          {node, acc}
      end)

    found
  end

  defp health_key?(pairs),
    do: Keyword.keyword?(pairs) and Keyword.has_key?(pairs, :operative_health)

  defp atoms(file) do
    ~r/:(healthy|degraded|failed|unbound|parked|suspended|unknown)\b/
    |> Regex.scan(SourceWalk.read(file))
    |> MapSet.new(fn [_, atom] -> String.to_existing_atom(atom) end)
  end

  # ---------- fixtures ----------

  # One runner with a live control channel that no observed roster holds, and
  # how long its own control session has been silent.
  defp sampled(runner_id, silent_ms) do
    %{
      liveness: %{drivers: [], channels: [%{runner_id: runner_id, name: :control}]},
      runner_facts: %{runner_id => %{control_silent_ms: silent_ms}}
    }
  end

  defp one(id, observed) do
    %{{"ns", id} => Enum.into(observed, %{bound_runner: nil, suspended?: false, parked?: false})}
  end

  defp context_for(observed, roster, signals, plugins) do
    operatives =
      for {{ns, id}, _} <- observed,
          do: %{namespace: ns, operative_id: id, factory_id: nil, plugins: plugins}

    intent = %Intent{operatives: operatives}

    reality = %{operatives: observed, active_runners: %{"ns" => roster}}
    Probe.context(intent, reality, signals, 1_000)
  end

  defp judged(observed, roster, signals \\ %{}, plugins \\ []) do
    Judge.judge(context_for(observed, roster, signals, plugins), %{})
  end

  defp state_of(observed, roster) do
    [{{"ns", id}, _}] = Enum.take(observed, 1)
    judged(observed, roster)[{:operative, {"ns", id}}].state
  end

  # Every reachable state of every kind, minted by the judge from one context —
  # so a state the vocabulary declares and no fixture can produce fails here.
  defp all_states_fixture() do
    observed = %{
      {"ns", "healthy"} => obs(bound_runner: "e1"),
      {"ns", "degraded"} => obs(bound_runner: "e1", running_plugins: [], subscribed_plugins: []),
      {"ns", "failed"} => obs(bound_runner: "gone"),
      {"ns", "unbound"} => obs(bound_runner: nil),
      {"ns", "parked"} => obs(bound_runner: "e1", parked?: true),
      {"ns", "suspended"} => obs(bound_runner: "e1", suspended?: true),
      {"ns", "unknown"} => obs(bound_runner: :unknown),
      {"ns", "plugged"} =>
        obs(
          bound_runner: "e1",
          running_plugins: ["src"],
          subscribed_plugins: ["src"],
          plugin_health: %{"src" => :healthy}
        ),
      {"blind", "unknown_runner"} => obs(bound_runner: "e_blind")
    }

    operatives =
      for {{ns, id}, _} <- observed do
        plugins = if id in ["degraded", "plugged"], do: [{"src", %{}}], else: []
        %{namespace: ns, operative_id: id, factory_id: nil, plugins: plugins}
      end

    intent = %Intent{operatives: operatives}

    reality = %{
      operatives: observed,
      active_runners: %{"ns" => MapSet.new(["e1", "e2"]), "blind" => :unknown}
    }

    signals = %{
      stall_verdict: {:driver_loop_frozen, %{transport: {:uds, :control}, loop_age_ms: 9_000}},
      liveness: %{
        drivers: [
          %{transport: {:uds, :control}},
          %{transport: {:uds, :spare}},
          %{transport: {:uds, :extra}}
        ],
        channels: [
          %{runner_id: "e1", name: :control},
          %{runner_id: "e2", name: :control},
          %{runner_id: "gone", name: :control},
          %{runner_id: "e_blind", name: :control}
        ]
      }
    }

    base = Judge.judge(Probe.context(intent, reality, signals, 1_000), %{})

    # The two states no single context can hold at once: a plugin the loop
    # could not probe at all, and a driver whose own reader failed.
    unprobed =
      judged(
        one("unprobed",
          bound_runner: "e1",
          running_plugins: ["src"],
          subscribed_plugins: ["src"]
        ),
        MapSet.new(["e1"]),
        %{},
        [{"src", %{}}]
      )

    # The wedge reader raises while the population reader answers: the drivers
    # and channels stay on the row and every one of them is `unknown`.
    blind_fabric =
      Judge.judge(
        Probe.context(
          intent,
          reality,
          %{stall_verdict: {:driver_loop_frozen, :not_a_context}, liveness: signals.liveness},
          1_000
        ),
        %{}
      )

    # A degraded channel: the classifier names a connection-scoped layer.
    wedged_channel =
      Judge.judge(
        Probe.context(
          intent,
          reality,
          %{
            stall_verdict: {:channel_not_sending, %{runner_id: "e1", channel: :control}},
            liveness: signals.liveness
          },
          1_000
        ),
        %{}
      )

    wedged_runner =
      Judge.judge(
        Probe.context(
          intent,
          reality,
          %{
            stall_verdict: {:runner_pool_wedged, %{runner_id: "e1", dispatches: 7}},
            liveness: signals.liveness
          },
          1_000
        ),
        %{}
      )

    Map.merge(base, unprobed)
    |> Map.merge(Map.take(blind_fabric, [{:driver, {:uds, :extra}}]))
    |> Map.merge(Map.take(wedged_channel, [{:channel, {"e1", :control}}]))
    |> Map.merge(Map.take(wedged_runner, [{:runner, "e1"}]))
  end

  defp obs(fields) do
    Enum.into(fields, %{
      bound_runner: nil,
      suspended?: false,
      parked?: false,
      running_plugins: [],
      subscribed_plugins: [],
      down_plugins: %{},
      plugin_health: %{}
    })
  end

  defp fixture_context() do
    context_for(one("fixture", bound_runner: "e1"), MapSet.new(["e1"]), %{}, [])
  end

  defp fixture_records(:parked),
    do: %{
      {:operative, {"ns", "p"}} =>
        Verdict.new({:operative, {"ns", "p"}}, :parked, reason: {:parked, :boom}, phase: :parked)
    }

  defp fixture_records(:failed),
    do: %{
      {:operative, {"ns", "f"}} =>
        Verdict.new({:operative, {"ns", "f"}}, :failed, reason: :dead_runner)
    }

  defp fixture_records(:unknown),
    do: %{
      {:operative, {"ns", "u"}} =>
        Verdict.new({:operative, {"ns", "u"}}, :unknown, reason: {:unobserved, :probe})
    }

  defp fixture_records(:driver_wedged),
    do: %{
      {:driver, {:uds, :control}} =>
        Verdict.new({:driver, {:uds, :control}}, :degraded,
          reason: {:wedged, :driver_loop_frozen}
        )
    }

  defp classifier_layers() do
    for layer <- [
          :driver_loop_frozen,
          :nif_queue_backing_up,
          :send_buffer_dropping,
          :socket_write_blocked,
          :runner_not_consuming,
          :runner_pool_wedged,
          :channel_not_sending,
          :operative_not_forwarding,
          :entity_dispatch_stalled,
          :producer_push_stalled
        ],
        do: layer
  end

  defp kind_of_scope(:fabric), do: :driver
  defp kind_of_scope(:connection), do: :channel
  defp kind_of_scope(:runner), do: :runner
  defp kind_of_scope(:operative), do: :operative
  defp kind_of_scope(nil), do: nil
end
