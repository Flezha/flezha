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
  alias Flezha.Convergence.{Diff, Intent, Planner}
  alias Flezha.Plugin

  defp plugin(id) do
    Plugin.new(
      id: id,
      provider: __MODULE__.NoopProvider,
      opts: %{},
      subscriptions: [{:on_output, []}],
      namespace: "default",
      operative_id: "r"
    )
  end

  defp plugin_intent(ids),
    do: %Intent{
      operatives: [
        %{
          namespace: :ns,
          operative_id: "r",
          factory_id: nil,
          plugins: Enum.map(ids, &{&1, plugin(&1)})
        }
      ]
    }

  defp plugin_reality(fields),
    do: %{
      active_runners: %{ns: MapSet.new(["e"])},
      operatives: %{
        {:ns, "r"} =>
          Map.merge(
            %{bound_runner: "e", pid: self(), running_plugins: [], subscribed_plugins: []},
            fields
          )
      },
      stalled_operatives: MapSet.new()
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

    test "a parked or a suspended operative yields neither missing nor extra" do
      for gate <- [%{parked?: true}, %{suspended?: true}] do
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
      key = {:rebind_operative, "btc"}
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
      b = Backoff.update(Backoff.new(:heal), {:rebind_operative, "a"}, :error, now)

      ops = [
        %{primitive: :rebind_operative, target: "a"},
        %{primitive: :rebind_operative, target: "b"}
      ]

      kept = Backoff.filter(ops, b, &Planner.key/1, now)
      assert kept == [%{primitive: :rebind_operative, target: "b"}]
    end
  end

  describe "Diff.compute/2" do
    test "classifies unbound and misbound operatives against active runners" do
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "unbound", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "ok", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "stale", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{
          {:ns, "unbound"} => %{bound_runner: nil},
          {:ns, "ok"} => %{bound_runner: "e_live"},
          {:ns, "stale"} => %{bound_runner: "e_dead"}
        },
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.unbound_operatives == [{:ns, "unbound"}]
      assert delta.misbound_operatives == [{{:ns, "stale"}, "e_dead", :probe}]
      refute Diff.empty?(delta)
    end

    test "empty delta when every operative is bound to a live runner" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "ok", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{{:ns, "ok"} => %{bound_runner: "e_live"}},
        stalled_operatives: MapSet.new()
      }

      assert Diff.empty?(diff(intent, reality))
    end

    test "a declared operative with no observation is the reconcile's to start, not a heal's" do
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "declared_only", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{},
        stalled_operatives: MapSet.new()
      }

      assert Diff.empty?(diff(intent, reality))
    end

    test "re-places instead of rebinding when the factory is only served elsewhere" do
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "movable", factory_id: "f", plugins: []},
          %{namespace: :ns, operative_id: "stale_movable", factory_id: "f", plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_other"])},
        operatives: %{
          {:ns, "movable"} => %{bound_runner: nil, factory_id: "f"},
          {:ns, "stale_movable"} => %{bound_runner: "e_dead", factory_id: "f"}
        },
        placement: %{{:ns, "f"} => %{local: [], remote_nodes: [:peer@host]}},
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.unbound_operatives == []
      assert delta.misbound_operatives == []

      assert delta.replace_operatives == [
               {{:ns, "movable"}, nil},
               {{:ns, "stale_movable"}, "e_dead"}
             ]
    end

    test "an unknown local placement read falls back to an ordinary rebind" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: "f", plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        operatives: %{{:ns, "r"} => %{bound_runner: nil, factory_id: "f"}},
        placement: %{{:ns, "f"} => %{local: :unknown, remote_nodes: [:peer@host]}},
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.unbound_operatives == [{:ns, "r"}]
      assert delta.replace_operatives == []
    end

    test "no eligible fleet anywhere stays an ordinary rebind — degraded, no re-place churn" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: "f", plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        operatives: %{{:ns, "r"} => %{bound_runner: nil, factory_id: "f"}},
        placement: %{{:ns, "f"} => %{local: [], remote_nodes: []}},
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.unbound_operatives == [{:ns, "r"}]
      assert delta.replace_operatives == []
    end

    test "I2: classifies missing and extra plugins on a live operative" do
      p = plugin("declared")

      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_runner: "e",
            pid: self(),
            running_plugins: ["stray"],
            subscribed_plugins: ["stray"]
          }
        },
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.missing_plugins == [{{:ns, "r"}, "declared", p}]
      assert delta.extra_plugins == [{{:ns, "r"}, "stray", self()}]
    end

    test "I2: a suspended instance's released plugins are not missing" do
      p = plugin("declared")

      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      # Alive (the Overwatch stays), unbound, nothing subscribed: a parked
      # workflow loop. The rehydrating bind replays its plugins.
      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{bound_runner: nil, pid: self(), suspended?: true, running_plugins: []}
        },
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
      assert delta.unhealthy_plugins == []
    end

    test "I2: a parked instance's unactivated plugins are not missing" do
      p = plugin("declared")

      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      # Alive with a failed init: its plugins are prepared, not running, and
      # the un-park activates them.
      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{bound_runner: "e", pid: self(), parked?: true, running_plugins: []}
        },
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)

      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    test "I2: no plugin divergence when running set matches declared" do
      p = plugin("p1")

      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"p1", p}]}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{{:ns, "r"} => %{bound_runner: "e", pid: self(), running_plugins: ["p1"]}},
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    # fails when: a operative whose declared plugin set reads empty (cspec lag or
    # read failure) has its entire running plugin set torn down every tick,
    # causing a stop/re-subscribe storm
    test "I2: empty declared plugin set never marks running plugins as extra" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_runner: "e",
            pid: self(),
            running_plugins: ["ws_in", "kafka_out"]
          }
        },
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    test "I3: a subscribed declared plugin self-reporting unhealthy diffs as unhealthy" do
      p = plugin("ws_in")

      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"ws_in", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_runner: "e",
            pid: self(),
            running_plugins: ["ws_in"],
            plugin_health: %{"ws_in" => {:unhealthy, :connection_idle}}
          }
        },
        stalled_operatives: MapSet.new()
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
        operatives: [
          %{
            namespace: :ns,
            operative_id: "r",
            factory_id: nil,
            plugins: [{"gone", missing}, {"shaky", shaky}, {"fine", fine}]
          }
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_runner: "e",
            pid: self(),
            running_plugins: ["shaky", "fine"],
            # "gone" is absent from health (unsubscribed); "fine" has no
            # verdict at all (uncertain probe) — both must not be unhealthy.
            plugin_health: %{"shaky" => {:unhealthy, :produce_stalled}}
          }
        },
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.missing_plugins == [{{:ns, "r"}, "gone", missing}]
      assert delta.unhealthy_plugins == [{{:ns, "r"}, "shaky", shaky, :produce_stalled}]
    end

    test "I2: skips plugin diff for operatives with no live pid" do
      p = plugin("declared")

      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        operatives: %{{:ns, "r"} => %{bound_runner: nil, pid: nil, running_plugins: []}},
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.missing_plugins == []
    end

    test "skips misbound classification when the namespace's live-runner read is :unknown" do
      # C1 regression: a failed FleetManager read (Probe records `:unknown`)
      # must NOT make every bound operative misbound — that mass-migrates a whole
      # namespace on a transient read race.
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "a", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "b", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: :unknown},
        operatives: %{
          {:ns, "a"} => %{bound_runner: "e1"},
          {:ns, "b"} => %{bound_runner: "e2"}
        },
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.misbound_operatives == []
      assert delta.unbound_operatives == []
    end

    test "skips a operative whose own state read failed (bound_runner :unknown)" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{{:ns, "r"} => %{bound_runner: :unknown, pid: nil, running_plugins: []}},
        stalled_operatives: MapSet.new()
      }

      delta = diff(intent, reality)
      assert delta.misbound_operatives == []
      assert delta.unbound_operatives == []
      assert delta.missing_plugins == []
    end
  end

  describe "Planner.plan/3" do
    test "orders eviction before rebind for misbound operatives" do
      delta = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead", :probe}]}
      plan = Planner.plan(delta, Backoff.new(:heal))

      assert [
               %{primitive: :evict_runner, target: "e_dead"},
               %{primitive: :rebind_operative} = rebind
             ] =
               plan

      assert rebind.namespace == :ns
      assert rebind.target == "r"
    end

    test "caps ops per tick at max_ops" do
      delta = %Diff{unbound_operatives: for(i <- 1..30, do: {:ns, "r#{i}"})}
      assert length(Planner.plan(delta, Backoff.new(:heal), max_ops: 5)) == 5
    end

    test "unbound operatives bind via rebind_operative (un-park), not a no-op" do
      delta = %Diff{unbound_operatives: [{:ns, "parked"}]}

      assert [%{primitive: :rebind_operative, namespace: :ns, target: "parked"}] =
               Planner.plan(delta, Backoff.new(:heal))
    end

    test "a re-place evicts its dead runner first and keys its own backoff" do
      delta = %Diff{replace_operatives: [{{:ns, "r"}, "e_dead"}]}

      assert [
               %{primitive: :evict_runner, target: "e_dead"},
               %{primitive: :replace_operative, namespace: :ns, target: "r"} = replace
             ] = Planner.plan(delta, Backoff.new(:heal))

      assert Planner.key(replace) == {:replace_operative, "r"}
    end

    test "an unbound re-place plans no eviction" do
      delta = %Diff{replace_operatives: [{{:ns, "r"}, nil}]}

      assert [%{primitive: :replace_operative, namespace: :ns, target: "r"}] =
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
      declared = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead", :runner_disconnected}]}
      [_evict, rebind] = Planner.plan(declared, Backoff.new(:heal))
      assert Planner.key(rebind) == {:runner_disconnected, "r"}
      refute Planner.ladder?(rebind)

      probed = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead", :probe}]}
      [_evict, rebind] = Planner.plan(probed, Backoff.new(:heal))
      assert Planner.key(rebind) == {:rebind_operative, "r"}
    end

    test "a parked operative rebinds on the parked ladder, cutting a dead transport first" do
      # The verdict rides the fact: `Lifecycle.park/2` classifies its own
      # failure reason, so the Planner reads it rather than re-deriving it.
      dead =
        %Diff{
          parked_operatives: [
            {{:ns, "r"}, %{reason: {:ping_failed, :closed}, transport: :dead}, "e1"}
          ]
        }

      assert [
               %{primitive: :reset_control_channel, target: "e1"},
               %{primitive: :rebind_operative, target: "r"} = rebind
             ] = Planner.plan(dead, Backoff.new(:heal))

      assert Planner.key(rebind) == {:parked, "r"}
      assert Planner.ladder?(rebind)

      # A timed-out ping is a busy runner: retried, and cut only once the
      # parked ladder has walked two rungs.
      busy =
        %Diff{
          parked_operatives: [
            {{:ns, "r"}, %{reason: {:ping_failed, :timeout}, transport: :busy}, "e1"}
          ]
        }

      assert [%{primitive: :rebind_operative}] = Planner.plan(busy, Backoff.new(:heal))

      # Two rungs recorded long enough ago that the cooldown has opened, so
      # the tick plans both the cut and the retry it precedes. The clock is
      # monotonic and starts wherever the VM says, so the offset is taken from
      # it rather than from zero.
      long_ago = System.monotonic_time(:millisecond) - 1_000_000

      walked =
        Enum.reduce(1..2, Backoff.new(:heal), fn _, b ->
          Backoff.update(b, {:parked, "r"}, :error, long_ago)
        end)

      assert [%{primitive: :reset_control_channel, target: "e1"}, %{primitive: :rebind_operative}] =
               Planner.plan(busy, walked)
    end

    test "an Entity fact plans one resync keyed on the fact, and a stale tag one publish" do
      entity = {"class", "op0", "k1"}

      delta = %Diff{
        resync_entities: [{entity, "e1", :seq_gap}],
        stale_tags: [{{:ns, "r"}, "e_old", "e1"}]
      }

      assert [
               %{primitive: :resync_runner, target: ^entity, args: args} = resync,
               %{primitive: :publish_binding, namespace: :ns, target: "r"} = publish
             ] = Planner.plan(delta, Backoff.new(:heal))

      assert args.runner_id == "e1"
      assert Planner.key(resync) == {:seq_gap, entity}
      assert Planner.ladder?(resync)
      assert Planner.key(publish) == {:publish_binding, "r"}
      refute Planner.ladder?(publish)
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
