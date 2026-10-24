defmodule Flezha.Convergence.DiffTest do
  @moduledoc """
  The pure reconciler front half: `Backoff`'s ladder, the `Diff` between a
  declared intent and observed reality, and the `Planner` that turns a delta
  into ordered heal operations.
  """

  # async: false: writes Application env; Mimic stubs; a shared on-disk root.

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.ConvergenceFixtures, only: [diff: 2]
  use Mimic

  alias Flezha.Config.Backoff
  alias Flezha.Convergence.{Diff, Intent, Planner, Verdict}
  alias FlezhaTest.Helper.ConvergenceFixtures
  alias Flezha.Plugin

  defp plugin(id) do
    Plugin.new(
      id: id,
      provider: __MODULE__.NoopProvider,
      opts: %{},
      hooks: [{:on_output, []}],
      namespace: "default",
      relay_id: "r"
    )
  end

  defp plugin_intent(ids),
    do: %Intent{
      relays: [
        %{
          namespace: :ns,
          relay_id: "r",
          factory_id: nil,
          plugins: Enum.map(ids, &{&1, plugin(&1)})
        }
      ]
    }

  defp plugin_reality(fields),
    do: %{
      active_runners: %{ns: MapSet.new(["e"])},
      relays: %{
        {:ns, "r"} =>
          Map.merge(
            %{bound_runner: "e", pid: self(), running_plugins: [], subscribed_plugins: []},
            fields
          )
      },
      stalled_relays: MapSet.new()
    }

  describe "a declared plugin that is not running is a divergence (D1)" do
    test "a declared plugin stuck in :prepared is missing and yields a start_plugin op" do
      delta =
        diff(
          plugin_intent(["src"]),
          plugin_reality(%{running_plugins: [], subscribed_plugins: ["src"]})
        )

      assert [{{:ns, "r"}, "src", _plugin}] = delta.missing_plugins
      assert delta.extra_plugins == []
      refute Diff.empty?(delta)

      assert [%{primitive: :start_plugin, target: "r"}] =
               delta
               |> Planner.plan(Backoff.new(:heal))
               |> Enum.map(&Map.take(&1, [:primitive, :target]))
    end

    test "a binding recorded not started is planned after every repair" do
      ids = for i <- 1..(Planner.max_ops() + 5), do: "off_#{i}"

      delta =
        diff(
          plugin_intent(["src" | ids]),
          plugin_reality(%{
            running_plugins: [],
            subscribed_plugins: ["src", "stray" | ids],
            not_started_plugins: Map.new(ids, &{&1, {:plugin_not_found, &1}})
          })
        )

      assert [{{:ns, "r"}, "src", _plugin}] = delta.missing_plugins
      assert length(delta.not_started_plugins) == length(ids)

      # The repairs come first; the bindings that cannot start fill what is left.
      assert [
               {:start_plugin, "src"},
               {:stop_plugin, "stray"} | not_started
             ] =
               delta
               |> Planner.plan(Backoff.new(:heal))
               |> Enum.map(&{&1.primitive, &1.args.plugin_id})

      assert length(not_started) == Planner.max_ops() - 2
      assert Enum.all?(not_started, &match?({:start_plugin, "off_" <> _}, &1))
    end

    test "a running declared plugin yields no divergence and no op" do
      delta =
        diff(
          plugin_intent(["src"]),
          plugin_reality(%{running_plugins: ["src"], subscribed_plugins: ["src"]})
        )

      assert delta.missing_plugins == []
      assert Planner.plan(delta, Backoff.new(:heal)) == []
    end

    test "an undeclared plugin is extra in BOTH phases" do
      for observed <- [
            %{running_plugins: ["src", "stray"], subscribed_plugins: ["src", "stray"]},
            %{running_plugins: ["src"], subscribed_plugins: ["src", "stray"]}
          ] do
        delta = diff(plugin_intent(["src"]), plugin_reality(observed))

        assert delta.extra_plugins == [{{:ns, "r"}, "stray", self()}],
               "an undeclared plugin must be torn down whatever its phase, got " <>
                 inspect(delta.extra_plugins)
      end
    end

    test "a parked or a suspended relay yields neither missing nor extra" do
      for gate <- [%{phase: :halted}, %{phase: :suspended}] do
        observed =
          Map.merge(%{running_plugins: [], subscribed_plugins: ["src", "stray"]}, gate)

        delta = diff(plugin_intent(["src"]), plugin_reality(observed))

        assert delta.missing_plugins == []
        assert delta.extra_plugins == []
      end
    end

    test "a heal that keeps failing settles onto the backoff ladder, not every tick" do
      delta =
        diff(
          plugin_intent(["src"]),
          plugin_reality(%{running_plugins: [], subscribed_plugins: ["src"]})
        )

      [op] = Planner.plan(delta, Backoff.new(:heal))

      # `Plugins.subscribe/3` now answers `:error` for a plugin that did not
      # reach `:running`, so `Loop.ladder_outcome/2` records the heal as failed.
      # The divergence is recomputed from the Probe each tick and so is still
      # there — but the ladder holds the retry until its cooldown opens.
      backoff = Backoff.update(Backoff.new(:heal), Planner.key(op), :error, 0)

      refute Backoff.eligible?(backoff, Planner.key(op), 0)
      assert Backoff.eligible?(backoff, Planner.key(op), 5_000)
    end
  end

  describe "Backoff" do
    test "fresh keys are eligible; failures arm an exponential cooldown" do
      b = Backoff.new(:heal)
      key = {:rebind_relay, "btc"}
      now = 1_000

      assert Backoff.eligible?(b, key, now)

      b = Backoff.update(b, key, :error, now)
      refute Backoff.eligible?(b, key, now)
      # first step is 5s
      assert Backoff.eligible?(b, key, now + 5_000)

      # a success clears the entry
      b = Backoff.update(b, key, :ok, now)
      assert Backoff.eligible?(b, key, now)
    end

    test "filter drops ops still in cooldown, keeps the rest" do
      now = 0
      b = Backoff.update(Backoff.new(:heal), {:rebind_relay, "a"}, :error, now)

      ops = [
        %{primitive: :rebind_relay, target: "a"},
        %{primitive: :rebind_relay, target: "b"}
      ]

      kept = Backoff.filter(ops, b, &Planner.key/1, now)
      assert kept == [%{primitive: :rebind_relay, target: "b"}]
    end
  end

  describe "Diff.compute/2" do
    test "classifies unbound and misbound relays against active runners" do
      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "unbound", factory_id: nil, plugins: []},
          %{namespace: :ns, relay_id: "ok", factory_id: nil, plugins: []},
          %{namespace: :ns, relay_id: "stale", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        relays: %{
          {:ns, "unbound"} => %{bound_runner: nil},
          {:ns, "ok"} => %{bound_runner: "e_live"},
          {:ns, "stale"} => %{bound_runner: "e_dead"}
        },
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.unbound_relays == [{:ns, "unbound"}]
      assert delta.misbound_relays == [{{:ns, "stale"}, "e_dead", :probe}]
      refute Diff.empty?(delta)
    end

    test "empty delta when every relay is bound to a live runner" do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "ok", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        relays: %{{:ns, "ok"} => %{bound_runner: "e_live"}},
        stalled_relays: MapSet.new()
      }

      assert Diff.empty?(diff(intent, reality))
    end

    test "a declared relay with no observation is the reconcile's to start, not a heal's" do
      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "declared_only", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        relays: %{},
        stalled_relays: MapSet.new()
      }

      assert Diff.empty?(diff(intent, reality))
    end

    test "plans nothing when the factory is only served elsewhere: the hub is moving it" do
      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "movable", factory_id: "f", plugins: []},
          %{namespace: :ns, relay_id: "stale_movable", factory_id: "f", plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_other"])},
        relays: %{
          {:ns, "movable"} => %{bound_runner: nil, factory_id: "f"},
          {:ns, "stale_movable"} => %{bound_runner: "e_dead", factory_id: "f"}
        },
        placement: %{{:ns, "f"} => %{local: [], remote_nodes: [:peer@host]}},
        stalled_relays: MapSet.new()
      }

      assert Diff.empty?(diff(intent, reality))
    end

    test "an unknown local placement read falls back to an ordinary rebind" do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: "f", plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        relays: %{{:ns, "r"} => %{bound_runner: nil, factory_id: "f"}},
        placement: %{{:ns, "f"} => %{local: :unknown, remote_nodes: [:peer@host]}},
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.unbound_relays == [{:ns, "r"}]
    end

    test "no eligible fleet anywhere stays an ordinary rebind — degraded, no re-place churn" do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: "f", plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        relays: %{{:ns, "r"} => %{bound_runner: nil, factory_id: "f"}},
        placement: %{{:ns, "f"} => %{local: [], remote_nodes: []}},
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.unbound_relays == [{:ns, "r"}]
    end

    test "I2: classifies missing and extra plugins on a live relay" do
      p = plugin("declared")

      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        relays: %{
          {:ns, "r"} => %{
            bound_runner: "e",
            pid: self(),
            running_plugins: ["stray"],
            subscribed_plugins: ["stray"]
          }
        },
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.missing_plugins == [{{:ns, "r"}, "declared", p}]
      assert delta.extra_plugins == [{{:ns, "r"}, "stray", self()}]
    end

    test "I2: a suspended instance's released plugins are not missing" do
      p = plugin("declared")

      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      # Alive (the Overwatch stays), unbound, nothing subscribed: a parked
      # steps loop. The rehydrating bind replays its plugins.
      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        relays: %{
          {:ns, "r"} => %{bound_runner: nil, pid: self(), phase: :suspended, running_plugins: []}
        },
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
      assert delta.unhealthy_plugins == []
    end

    test "I2: a parked instance's unactivated plugins are not missing" do
      p = plugin("declared")

      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      # Alive with a failed init: its plugins are prepared, not running, and
      # the un-park activates them.
      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        relays: %{
          {:ns, "r"} => %{bound_runner: "e", pid: self(), phase: :halted, running_plugins: []}
        },
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    test "I2: no plugin divergence when running set matches declared" do
      p = plugin("p1")

      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"p1", p}]}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        relays: %{{:ns, "r"} => %{bound_runner: "e", pid: self(), running_plugins: ["p1"]}},
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    # fails when: a relay whose declared plugin set reads empty (cspec lag or
    # read failure) has its entire running plugin set torn down every tick,
    # causing a stop/re-subscribe storm
    test "I2: empty declared plugin set never marks running plugins as extra" do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        relays: %{
          {:ns, "r"} => %{
            bound_runner: "e",
            pid: self(),
            running_plugins: ["ws_in", "http_out"]
          }
        },
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    test "I3: a subscribed declared plugin self-reporting unhealthy diffs as unhealthy" do
      p = plugin("ws_in")

      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"ws_in", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        relays: %{
          {:ns, "r"} => %{
            bound_runner: "e",
            pid: self(),
            running_plugins: ["ws_in"],
            plugin_health: %{"ws_in" => {:unhealthy, :connection_idle}}
          }
        },
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.unhealthy_plugins == [{{:ns, "r"}, "ws_in", p, :connection_idle}]
      assert delta.missing_plugins == []
      refute Diff.empty?(delta)
    end

    test "I3: missing and unhealthy buckets are disjoint; uncertain health is healthy" do
      missing = plugin("gone")
      shaky = plugin("shaky")
      fine = plugin("fine")

      intent = %Intent{
        relays: [
          %{
            namespace: :ns,
            relay_id: "r",
            factory_id: nil,
            plugins: [{"gone", missing}, {"shaky", shaky}, {"fine", fine}]
          }
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        relays: %{
          {:ns, "r"} => %{
            bound_runner: "e",
            pid: self(),
            running_plugins: ["shaky", "fine"],
            # "gone" is absent from health (unsubscribed); "fine" has no
            # verdict at all (uncertain probe) — both must not be unhealthy.
            plugin_health: %{"shaky" => {:unhealthy, :produce_stalled}}
          }
        },
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.missing_plugins == [{{:ns, "r"}, "gone", missing}]
      assert delta.unhealthy_plugins == [{{:ns, "r"}, "shaky", shaky, :produce_stalled}]
    end

    test "I2: skips plugin diff for relays with no live pid" do
      p = plugin("declared")

      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        relays: %{{:ns, "r"} => %{bound_runner: nil, pid: nil, running_plugins: []}},
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.missing_plugins == []
    end

    test "skips misbound classification when the namespace's live-runner read is :unknown" do
      # C1 regression: a failed FleetManager read (Probe records `:unknown`)
      # must NOT make every bound relay misbound — that mass-migrates a whole
      # namespace on a transient read race.
      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "a", factory_id: nil, plugins: []},
          %{namespace: :ns, relay_id: "b", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: :unknown},
        relays: %{
          {:ns, "a"} => %{bound_runner: "e1"},
          {:ns, "b"} => %{bound_runner: "e2"}
        },
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.misbound_relays == []
      assert delta.unbound_relays == []
    end

    test "skips a relay whose own state read failed (bound_runner :unknown)" do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        relays: %{{:ns, "r"} => %{bound_runner: :unknown, pid: nil, running_plugins: []}},
        stalled_relays: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.misbound_relays == []
      assert delta.unbound_relays == []
      assert delta.missing_plugins == []
    end

    # Fails as: after a runner reconnects, a Cell's resync keeps going to the
    # session that ended (`no_control_channel`), its WAL never advances again,
    # and the relay stays degraded with `wal_frozen`.
    test "a Cell is resynced against the runner its relay is bound to now" do
      gap = {"r", "op", "k"}
      gave_up = {"unbound", "op", "k"}

      facts = %{
        {:seq_gap, gap} => %{at: 0, args: %{runner_id: "e_ended"}},
        {:compaction_gave_up, gave_up} => %{at: 0, args: %{runner_id: "e_named"}}
      }

      reality = %{relays: %{{:ns, "r"} => %{bound_runner: "e_live", pid: self()}}}

      delta = Diff.compute(%Intent{relays: []}, reality, %{}, facts)

      assert Enum.sort(delta.resync_cells) == [
               {gap, "e_live", :seq_gap},
               {gave_up, "e_named", :compaction_gave_up}
             ]
    end

    # fails when: a row that names no runner beside a bound relay is not a stale
    # tag, so a start's late registry write that erased the bind's tag is never
    # re-published. An unbound relay's empty row is not one.
    test "a bound relay whose row names no runner is a stale tag" do
      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "bound", factory_id: nil, plugins: []},
          %{namespace: :ns, relay_id: "unbound", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e1"])},
        relays: %{
          {:ns, "bound"} => %{bound_runner: "e1", registry_tag: nil, pid: self()},
          {:ns, "unbound"} => %{bound_runner: nil, registry_tag: nil, pid: self()}
        },
        stalled_relays: MapSet.new()
      }

      assert diff(intent, reality).stale_tags == [{{:ns, "bound"}, nil, "e1"}]
    end
  end

  describe "Planner.plan/3" do
    test "orders eviction before rebind for misbound relays" do
      delta = %Diff{misbound_relays: [{{:ns, "r"}, "e_dead", :probe}]}
      plan = Planner.plan(delta, Backoff.new(:heal))

      assert [
               %{primitive: :evict_runner, target: "e_dead"},
               %{primitive: :rebind_relay} = rebind
             ] =
               plan

      assert rebind.namespace == :ns
      assert rebind.target == "r"
    end

    test "caps ops per tick at max_ops" do
      delta = %Diff{unbound_relays: for(i <- 1..30, do: {:ns, "r#{i}"})}
      assert length(Planner.plan(delta, Backoff.new(:heal), max_ops: 5)) == 5
    end

    # fails when: ops whose precondition stays unmet fill every tick's cap,
    # and a repair behind them is never planned.
    test "ops unmet last tick go after the rest, so a capped plan reaches a repair" do
      p = plugin("lost")
      waiting = for i <- 1..5, do: {:ns, "w#{i}"}

      delta = %Diff{
        unbound_relays: waiting,
        unhealthy_plugins: [{{:ns, "r"}, "lost", p, {:down, :killed}}]
      }

      unmet_last = MapSet.new(waiting, fn {_ns, id} -> {:rebind_relay, id} end)

      assert Enum.map(Planner.plan(delta, Backoff.new(:heal), max_ops: 5), & &1.primitive) ==
               List.duplicate(:rebind_relay, 5)

      # Over the cap, a waiting op makes room; the plan keeps its order.
      assert Enum.map(
               Planner.plan(delta, Backoff.new(:heal), max_ops: 5, unmet_last: unmet_last),
               & &1.primitive
             ) == List.duplicate(:rebind_relay, 4) ++ [:restart_plugin]

      # Within the cap, nothing moves.
      assert Enum.map(
               Planner.plan(delta, Backoff.new(:heal), unmet_last: unmet_last),
               & &1.primitive
             ) == List.duplicate(:rebind_relay, 5) ++ [:restart_plugin]
    end

    test "unbound relays bind via rebind_relay (un-park), not a no-op" do
      delta = %Diff{unbound_relays: [{:ns, "parked"}]}

      assert [%{primitive: :rebind_relay, namespace: :ns, target: "parked"}] =
               Planner.plan(delta, Backoff.new(:heal))
    end

    test "missing plugins emit start_plugin carrying the full %Plugin{}" do
      p = plugin("p1")
      delta = %Diff{missing_plugins: [{{:ns, "r"}, "p1", p}]}

      assert [%{primitive: :start_plugin, namespace: :ns, target: "r", args: args}] =
               Planner.plan(delta, Backoff.new(:heal))

      assert args.plugin_id == "p1"
      assert args.plugin == p
    end

    test "unhealthy plugins emit restart_plugin carrying the %Plugin{} and the verdict" do
      p = plugin("p1")
      delta = %Diff{unhealthy_plugins: [{{:ns, "r"}, "p1", p, :produce_stalled}]}

      assert [%{primitive: :restart_plugin, namespace: :ns, target: "r", args: args} = op] =
               Planner.plan(delta, Backoff.new(:heal))

      assert args.plugin_id == "p1"
      assert args.plugin == p
      assert args.verdict == :produce_stalled
      # Keyed per plugin, so one crashing plugin never throttles its siblings.
      assert Planner.key(op) == {:restart_plugin, {"r", "p1"}}
    end

    test "a declared disconnect keys the rebind on the fact, a probed one on the primitive" do
      declared = %Diff{misbound_relays: [{{:ns, "r"}, "e_dead", :runner_disconnected}]}
      [_evict, rebind] = Planner.plan(declared, Backoff.new(:heal))
      assert Planner.key(rebind) == {:runner_disconnected, "r"}
      refute Planner.ladder?(rebind)

      probed = %Diff{misbound_relays: [{{:ns, "r"}, "e_dead", :probe}]}
      [_evict, rebind] = Planner.plan(probed, Backoff.new(:heal))
      assert Planner.key(rebind) == {:rebind_relay, "r"}
    end

    test "a parked relay rebinds on the parked ladder, cutting a dead transport first" do
      # The verdict rides the fact: `Lifecycle.halt/2` classifies its own
      # failure reason, so the Planner reads it rather than re-deriving it.
      dead =
        %Diff{
          halted_relays: [
            {{:ns, "r"}, %{reason: {:ping_failed, :closed}, transport: :dead}, "e1"}
          ]
        }

      assert [
               %{primitive: :reset_control_channel, target: "e1"},
               %{primitive: :rebind_relay, target: "r"} = rebind
             ] = Planner.plan(dead, Backoff.new(:heal))

      assert Planner.key(rebind) == {:halted, "r"}
      assert Planner.ladder?(rebind)

      # A timed-out ping is a busy runner: retried, and cut only once the
      # parked ladder has walked two rungs.
      busy =
        %Diff{
          halted_relays: [
            {{:ns, "r"}, %{reason: {:ping_failed, :timeout}, transport: :busy}, "e1"}
          ]
        }

      assert [%{primitive: :rebind_relay}] = Planner.plan(busy, Backoff.new(:heal))

      # Two rungs recorded long enough ago that the cooldown has opened, so
      # the tick plans both the cut and the retry it precedes. The clock is
      # monotonic and starts wherever the VM says, so the offset is taken from
      # it rather than from zero.
      long_ago = System.monotonic_time(:millisecond) - 1_000_000

      walked =
        Enum.reduce(1..2, Backoff.new(:heal), fn _, b ->
          Backoff.update(b, {:halted, "r"}, :error, long_ago)
        end)

      assert [%{primitive: :reset_control_channel, target: "e1"}, %{primitive: :rebind_relay}] =
               Planner.plan(busy, walked)
    end

    test "a Cell fact plans one resync keyed on the fact, and a stale tag one publish" do
      cell = {"class", "op0", "k1"}

      delta = %Diff{
        resync_cells: [{cell, "e1", :seq_gap}],
        stale_tags: [{{:ns, "r"}, "e_old", "e1"}]
      }

      assert [
               %{primitive: :resync_runner, target: ^cell, args: args} = resync,
               %{primitive: :publish_binding, namespace: :ns, target: "r"} = publish
             ] = Planner.plan(delta, Backoff.new(:heal))

      assert args.runner_id == "e1"
      assert Planner.key(resync) == {:seq_gap, cell}
      assert Planner.ladder?(resync)
      assert Planner.key(publish) == {:publish_binding, "r"}
      refute Planner.ladder?(publish)
    end
  end

  describe "who is waiting for a runner" do
    @waiting_ref {:ns, "r"}

    defp waiting_delta(state) do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: "f", plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        relays: %{@waiting_ref => %{bound_runner: "e_dead", pid: self(), factory_id: "f"}}
      }

      Diff.compute(
        intent,
        reality,
        ConvergenceFixtures.verdicts([{{:relay, @waiting_ref}, state}])
      )
    end

    defp waiting?(%Diff{} = delta) do
      Enum.any?(Diff.waiting_fields(), &(Map.fetch!(delta, &1) != []))
    end

    # fails when: `classify/6` answers a state with a binding that
    # `awaiting_binding/0` does not name, or names one it no longer answers.
    # The release path reads that list to find who a connecting runner is news
    # for, so the two drifting apart is the defect this change closed once.
    test "exactly the declared states classify into a field that needs a runner" do
      for state <- Verdict.states() do
        assert waiting?(waiting_delta(state)) == state in Diff.awaiting_binding(),
               "#{state}: `awaiting_binding/0` and the classification disagree"
      end
    end

    # fails when: the drain stops suppressing a field a binding is planned from.
    test "the drain suppresses every waiting field, and the row re-publish with them" do
      draining = %{{:node_draining, node()} => %{at: 0, args: %{}}}

      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: "f", plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        relays: %{@waiting_ref => %{bound_runner: "e_dead", pid: self(), factory_id: "f"}}
      }

      verdicts = ConvergenceFixtures.verdicts([{{:relay, @waiting_ref}, :failed}])

      assert waiting?(Diff.compute(intent, reality, verdicts))
      assert Diff.compute(intent, reality, verdicts, draining).draining
      refute waiting?(Diff.compute(intent, reality, verdicts, draining))
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
