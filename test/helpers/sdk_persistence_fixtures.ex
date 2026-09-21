defmodule FlezhaTest.Helper.SdkPersistenceFixtures do
  @moduledoc """
  The tasklets and operatives the SDK's persistence tests drive: a
  changeful counter in Single, Static and Dynamic shapes, a `NoChange` echo, a
  class with persistence off, the two shapes the builder refuses, and the keyed
  sinks whose frames are stamped.
  """

  alias Flezha.SDK.Ctx

  defmodule CounterTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "counter"
    def init_state(_), do: %{value: 0}
    def apply_change(s, {:delta, n}), do: %{s | value: s.value + n}
    def state_from_json(%{"value" => v}), do: %{value: v}

    # `:kill` ends the process untrappably mid-dispatch, before any record ships.
    def run(:kill, _s, _ctx), do: Process.exit(self(), :kill)

    def run(input, s, ctx) do
      s = Ctx.apply(ctx, s, {:delta, input})
      Ctx.emit(ctx, Integer.to_string(s.value))
      {:ok, s}
    end
  end

  defmodule EchoTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "echo"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule ThriftyTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "thrifty"
    def init_state(_), do: %{value: 0}
    def compact_after_bytes, do: 1_048_576
    defdelegate apply_change(s, c), to: CounterTask
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule StaticCounterTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "scounter"
    def cell_keys, do: ["a", "b"]
    def cells_for(_input), do: ["a"]
    defdelegate init_state(key), to: CounterTask
    defdelegate apply_change(s, change), to: CounterTask
    defdelegate state_from_json(json), to: CounterTask
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule CounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "counter_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input_with("in", &parse/1) |> into(CounterTask) |> sink("c")
    end

    def parse("kill"), do: {:ok, :kill}
    def parse(bin), do: {:ok, String.to_integer(bin)}
  end

  defmodule OptedOutCounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "opted_out_counter"
    def persistence_enabled, do: false
    defdelegate channels(c), to: CounterOp
  end

  defmodule EchoOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "echo_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(EchoTask) |> sink("e")
    end
  end

  defmodule StaticCounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "static_counter"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> into(StaticCounterTask)
      |> sink("s")
    end
  end

  defmodule DynCounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "dyn_counter"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> key_by(fn n -> "k#{rem(n, 2)}" end)
      |> into(CounterTask)
      |> sink("c")
    end
  end

  defmodule TwoCountersOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "two_counters"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("a") |> into_named(CounterTask, "first") |> sink()
      c |> input("b") |> into_named(CounterTask, "second") |> sink()
    end
  end

  defmodule OptedOutOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "opted_out"
    def persistence_enabled, do: false

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ThriftyTask) |> into(CounterTask) |> sink()
    end
  end

  # Emits twice per input — once with user metadata — so the ordinal and the
  # stamps' position after the metadata are both observable on one dispatch.
  defmodule StampedTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "stamped"
    def init_state(_), do: %{value: 0}
    def apply_change(s, {:delta, n}), do: %{s | value: s.value + n}

    def run(input, s, ctx) do
      s = Ctx.apply(ctx, s, {:delta, input})
      :ok = Ctx.emit_with_meta(ctx, "a", %{"tag" => "x"})
      :ok = Ctx.emit(ctx, "b")
      {:ok, s}
    end
  end

  # Answers a refused emit as a non-retryable error, so the refusal is
  # readable off the park it produces.
  defmodule RefusingTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "refusing"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      case Ctx.emit(ctx, input) do
        :ok -> {:ok, s}
        {:error, reason} -> {:error, {:non_retryable, inspect(reason)}}
      end
    end
  end

  defmodule KeyedOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "keyed_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> key_by(fn n -> "k#{rem(n, 2)}" end)
      |> into(StampedTask)
      |> keyed()
      |> sink("c")
    end
  end

  # One node, two sinks: the keyed one is stamped, the plain one is not, and
  # the ordinal counts both.
  defmodule MixedSinksOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "mixed_sinks"

    def channels(c) do
      import Flezha.SDK.Channels

      node =
        c
        |> input_with("in", &CounterOp.parse/1)
        |> key_by(fn n -> "k#{rem(n, 2)}" end)
        |> into(StampedTask)
        |> named("c")

      node |> keyed() |> sink()
      node |> sink()
    end
  end

  # A Single node with a keyed sink and no key of its own: a plain emit has
  # nothing to fall back on.
  defmodule SingleKeyedOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "single_keyed"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> into(RefusingTask)
      |> keyed()
      |> sink("r")
    end
  end

  # Two live Cells at most: a third key's input has nowhere to go.
  defmodule CappedTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "capped"
    def max_cells, do: 2
    defdelegate init_state(key), to: CounterTask
    defdelegate apply_change(s, change), to: CounterTask
    defdelegate state_from_json(json), to: CounterTask
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule CappedOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "capped_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> key_by(&"k#{&1}")
      |> into(CappedTask)
      |> sink("c")
    end
  end

  # Emits what `Ctx.now/1` served: the seq the dispatch started at.
  defmodule LogicalClockTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "logical"
    def clock, do: :logical
    def init_state(_), do: %{value: 0}
    def apply_change(s, {:delta, n}), do: %{s | value: s.value + n}

    def run(input, s, ctx) do
      Ctx.emit(ctx, Integer.to_string(Ctx.now(ctx)))
      {:ok, Ctx.apply(ctx, s, {:delta, input})}
    end
  end

  # Emits what `Ctx.now/1` served: the input's own timestamp.
  defmodule EventTimeTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "event_time"
    def clock, do: :event_time
    def event_time_of(input), do: input
    def init_state(_), do: nil

    def run(_input, s, ctx) do
      Ctx.emit(ctx, Integer.to_string(Ctx.now(ctx)))
      {:ok, s}
    end
  end

  # Emits what `Ctx.now/1` served under the default clock: the epoch millisecond.
  defmodule NowTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "now"
    def init_state(_), do: nil

    def run(_input, s, ctx) do
      Ctx.emit(ctx, Integer.to_string(Ctx.now(ctx)))
      {:ok, s}
    end
  end

  # Each input journals its delta and registers a deadline `input` seconds out
  # (distinct per input, whatever the clock did between two dispatches); the
  # firing journals the timestamp it was handed and emits it.
  defmodule TimerTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "timer"
    def init_state(_), do: %{value: 0, fired: []}
    def apply_change(s, {:delta, n}), do: %{s | value: s.value + n}
    def apply_change(s, {:fired, at}), do: %{s | fired: s.fired ++ [at]}
    def state_from_json(%{"value" => v, "fired" => f}), do: %{value: v, fired: f}

    def run(input, s, ctx) do
      s = Ctx.apply(ctx, s, {:delta, input})
      {:ok, _at} = Ctx.timer_after(ctx, input * 1_000)
      Ctx.emit(ctx, Integer.to_string(s.value))
      {:ok, s}
    end

    # `now/1` inside a firing is the scheduled timestamp, whatever the clock.
    def on_timer(at, _kind, s, ctx) do
      s = Ctx.apply(ctx, s, {:fired, at})
      Ctx.emit(ctx, "fired@#{at}@#{Ctx.now(ctx)}")
      {:ok, s}
    end
  end

  # Fires on the input's own timeline: `arm:<at>` registers an event-time
  # timer at event time 0 (the high-water stays), `<t>` is a tick at `t`.
  defmodule EventTimerTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "event_timer"
    def clock, do: :event_time
    def event_time_of({:arm, _at}), do: 0
    def event_time_of({:tick, t}), do: t
    def init_state(_), do: nil

    def run({:arm, at}, s, ctx) do
      {:ok, ^at} = Ctx.timer_at_event_time(ctx, at)
      {:ok, s}
    end

    def run({:tick, t}, s, ctx) do
      Ctx.emit(ctx, "tick@#{t}")
      {:ok, s}
    end

    def on_timer(at, kind, s, ctx) do
      Ctx.emit(ctx, "fired@#{at}:#{kind}:#{Ctx.now(ctx)}")
      {:ok, s}
    end
  end

  # `on_timer/4` holds the Cell until told `:go`, telling the probe first,
  # so an input pushed meanwhile is dispatched after the firing.
  defmodule GatedTimerTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "gated_timer"
    def init_state(_), do: nil

    def run("arm", s, ctx) do
      {:ok, _at} = Ctx.timer_after(ctx, 0)
      Ctx.emit(ctx, "armed")
      {:ok, s}
    end

    def run(input, s, ctx) do
      Ctx.emit(ctx, input)
      {:ok, s}
    end

    def on_timer(_at, _kind, s, ctx) do
      send(Process.whereis(:timer_gate_probe), :firing)

      receive do
        :go -> :ok
      end

      Ctx.emit(ctx, "fired")
      {:ok, s}
    end
  end

  # A firing that fails non-retryably: the park must carry the removal.
  defmodule PoisonTimerTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "poison_timer"
    def init_state(_), do: %{armed: 0}
    def apply_change(s, {:armed, n}), do: %{s | armed: s.armed + n}

    def run("arm", s, ctx) do
      {:ok, _at} = Ctx.timer_after(ctx, 0)
      {:ok, Ctx.apply(ctx, s, {:armed, 1})}
    end

    def run(input, s, ctx) do
      Ctx.emit(ctx, input)
      {:ok, s}
    end

    def on_timer(_at, _kind, _s, _ctx), do: {:error, {:non_retryable, "poison firing"}}
  end

  # Registers and never handles.
  defmodule NoHandlerTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "no_handler"
    def init_state(_), do: nil

    def run("arm", s, ctx) do
      {:ok, _at} = Ctx.timer_after(ctx, 0)
      {:ok, s}
    end
  end

  # Remembers what it armed so a later input can cancel it.
  defmodule CancelTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "cancel"
    def init_state(_), do: %{armed: nil}
    def apply_change(s, {:armed, at}), do: %{s | armed: at}
    def state_from_json(%{"armed" => at}), do: %{armed: at}

    def run("arm", s, ctx) do
      {:ok, at} = Ctx.timer_after(ctx, 60_000)
      {:ok, Ctx.apply(ctx, s, {:armed, at})}
    end

    def run("cancel", s, ctx) do
      Ctx.emit(ctx, "cancelled:#{Ctx.cancel_processing_time_timer(ctx, s.armed)}")
      {:ok, s}
    end

    def run(input, s, ctx) do
      Ctx.emit(ctx, input)
      {:ok, s}
    end

    def on_timer(at, _kind, s, ctx) do
      Ctx.emit(ctx, "fired@#{at}")
      {:ok, s}
    end
  end

  # Arms 100 ms out, dies untrappably on demand: the replacement owes the timer.
  defmodule KillableTimerTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "killable_timer"
    def init_state(_), do: %{fired: []}
    def apply_change(s, {:fired, at}), do: %{s | fired: s.fired ++ [at]}
    def run("kill", _s, _ctx), do: Process.exit(self(), :kill)

    def run("arm", s, ctx) do
      {:ok, _at} = Ctx.timer_after(ctx, 100)
      {:ok, s}
    end

    def on_timer(at, _kind, s, ctx) do
      Ctx.emit(ctx, "fired@#{at}")
      {:ok, Ctx.apply(ctx, s, {:fired, at})}
    end
  end

  # A dynamic Cell idle-evicted after 100 ms, holding a timer a minute out.
  defmodule TtlTimerTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "ttl_timer"
    def cell_idle_ttl_ms, do: 100
    def init_state(_), do: %{armed: 0}
    def apply_change(s, {:armed, n}), do: %{s | armed: s.armed + n}

    def run(_input, s, ctx) do
      {:ok, _at} = Ctx.timer_after(ctx, 60_000)
      {:ok, Ctx.apply(ctx, s, {:armed, 1})}
    end

    def on_timer(at, _kind, s, ctx) do
      Ctx.emit(ctx, "fired@#{at}")
      {:ok, s}
    end
  end

  # One pending timer at most; emits what each registration answered.
  defmodule CappedTimersTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "capped_timers"
    def max_timers_per_cell, do: 1
    def init_state(_), do: nil

    def run(n, s, ctx) do
      Ctx.emit(ctx, inspect(Ctx.timer_after(ctx, n * 1_000)))
      {:ok, s}
    end

    def on_timer(_at, _kind, s, _ctx), do: {:ok, s}
  end

  defmodule EventTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "event_timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input_with("in", &parse/1) |> into(EventTimerTask) |> sink("e")
    end

    def parse("arm:" <> at), do: {:ok, {:arm, String.to_integer(at)}}
    def parse(t), do: {:ok, {:tick, String.to_integer(t)}}
  end

  defmodule GatedTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "gated_timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(GatedTimerTask) |> sink("g")
    end
  end

  defmodule PoisonTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "poison_timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(PoisonTimerTask) |> sink("p")
    end
  end

  defmodule NoHandlerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "no_handler_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(NoHandlerTask) |> sink("n")
    end
  end

  defmodule CancelOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "cancel_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(CancelTask) |> sink("c")
    end
  end

  defmodule KillableTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "killable_timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(KillableTimerTask) |> sink("k")
    end
  end

  defmodule TtlTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "ttl_timer_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input("in")
      |> key_by(&hd(String.split(&1, ":")))
      |> into(TtlTimerTask)
      |> sink("t")
    end
  end

  defmodule CappedTimersOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "capped_timers_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> into(CappedTimersTask)
      |> sink("c")
    end
  end

  defmodule LogicalClockOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "logical_clock_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> into(LogicalClockTask)
      |> sink("l")
    end
  end

  defmodule EventTimeOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "event_time_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> into(EventTimeTask)
      |> sink("t")
    end
  end

  defmodule NowOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "now_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(NowTask) |> sink("n")
    end
  end

  defmodule TimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input_with("in", &CounterOp.parse/1) |> into(TimerTask) |> sink("t")
    end
  end

  # Idle-evicted after 100 ms, on a sweep every 25 ms.
  defmodule TtlTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "ttl"
    def cell_idle_ttl_ms, do: 100
    defdelegate init_state(key), to: CounterTask
    defdelegate apply_change(s, change), to: CounterTask
    defdelegate state_from_json(json), to: CounterTask
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule TtlOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "ttl_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> key_by(&"k#{&1}")
      |> into(TtlTask)
      |> sink("c")
    end
  end

  # Blocks inside `run/3` until told `:go`, so a sweep meets it busy; a
  # NoChange tasklet, so its eviction ships no delete frame.
  defmodule GatedTtlTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "gated_ttl"
    def cell_idle_ttl_ms, do: 1
    def init_state(_), do: nil

    def run(input, s, ctx) do
      receive do
        :go -> :ok
      end

      Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule GatedTtlOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "gated_ttl_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> key_by(& &1) |> into(GatedTtlTask) |> sink("g")
    end
  end

  # The counter one schema version on: `unit` joined the state, a note joined
  # the change, and both read back from version 1 through the migrations.
  defmodule CounterV2Task do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet

    alias Flezha.SDK.Ctx

    def name, do: "counter"
    def schema_version, do: 2
    def min_readable_schema_version, do: 1
    def init_state(_), do: %{value: 0, unit: "n"}
    def apply_change(s, {:delta, n, _note}), do: %{s | value: s.value + n}
    def migrate_state(1, %{value: v}), do: {:ok, %{value: v, unit: "n"}}
    def migrate_state(1, _other), do: {:error, "bad shape"}
    def migrate_change(1, {:delta, n}), do: {:ok, {:delta, n, "migrated"}}
    def state_from_json(%{"value" => v, "unit" => u}), do: %{value: v, unit: u}

    def run(input, s, ctx) do
      s = Ctx.apply(ctx, s, {:delta, input, "live"})
      Ctx.emit(ctx, Integer.to_string(s.value))
      {:ok, s}
    end
  end

  # Claims to read version 1 and declares no migration.
  defmodule FlooredTask do
    @moduledoc false
    @behaviour Flezha.SDK.Tasklet
    def name, do: "floored"
    def schema_version, do: 2
    def min_readable_schema_version, do: 1
    defdelegate init_state(key), to: CounterTask
    defdelegate apply_change(s, change), to: CounterTask
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule CounterV2Op do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "counter_v2_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> into(CounterV2Task)
      |> sink("c")
    end
  end
end
