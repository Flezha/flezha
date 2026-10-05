defmodule FlezhaTest.Helper.SdkPersistenceFixtures do
  @moduledoc """
  The tasklets and relays the SDK's persistence tests drive: a
  changeful counter in Single, Static and Dynamic shapes, a `NoChange` echo, a
  class with persistence off, the two shapes the builder refuses, and the keyed
  sinks whose frames are stamped.
  """

  # The counter's one change, for every tasklet here that counts as it does.
  defmodule Delta do
    @moduledoc false
    defmacro __using__(_opts) do
      quote do
        require Flezha.SDK.State

        Flezha.SDK.State.defchange delta(s, n) do
          %{s | value: s.value + n}
        end
      end
    end
  end

  defmodule CounterTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "counter"
    @impl true
    def init_state(_), do: %{value: 0}

    use Delta

    @impl true
    def state_from_json(%{"value" => v}), do: %{value: v}

    # `:kill` ends the process untrappably mid-dispatch, before any record ships.
    @impl true
    def run(:kill, _s, _ctx), do: Process.exit(self(), :kill)

    def run(input, s, ctx) do
      s = delta(s, ctx, input)
      emit(ctx, Integer.to_string(s.value))
      {:ok, s}
    end
  end

  defmodule EchoTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "echo"

    @impl true
    def run(input, s, ctx) do
      emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule ThriftyTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "thrifty", compact_after_bytes: 1_048_576
    @impl true
    def init_state(_), do: %{value: 0}

    use Delta

    @impl true
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule StaticCounterTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "scounter", cell_keys: ["a", "b"]
    @impl true
    def cells_for(_input), do: ["a"]
    @impl true
    defdelegate init_state(key), to: CounterTask

    use Delta

    @impl true
    defdelegate state_from_json(json), to: CounterTask
    @impl true
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule CounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "opted_out_counter"
    def durable, do: false
    defdelegate channels(c), to: CounterOp
  end

  defmodule EchoOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "echo_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(EchoTask) |> sink("e")
    end
  end

  defmodule StaticCounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "two_counters"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("a") |> into_named(CounterTask, "first") |> sink()
      c |> input("b") |> into_named(CounterTask, "second") |> sink()
    end
  end

  defmodule OptedOutOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "opted_out"
    def durable, do: false

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ThriftyTask) |> into(CounterTask) |> sink()
    end
  end

  # Emits twice per input — once with user metadata — so the ordinal and the
  # stamps' position after the metadata are both observable on one dispatch.
  defmodule StampedTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "stamped"
    @impl true
    def init_state(_), do: %{value: 0}

    use Delta

    @impl true
    def run(input, s, ctx) do
      s = delta(s, ctx, input)
      :ok = emit_with_meta(ctx, "a", %{"tag" => "x"})
      :ok = emit(ctx, "b")
      {:ok, s}
    end
  end

  # Answers a refused emit as a non-retryable error, so the refusal is
  # readable off the park it produces.
  defmodule RefusingTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "refusing"

    @impl true
    def run(input, s, ctx) do
      case emit(ctx, input) do
        :ok -> {:ok, s}
        {:error, reason} -> {:error, {:non_retryable, inspect(reason)}}
      end
    end
  end

  defmodule KeyedOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
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
    use Flezha.SDK.Tasklet, name: "capped", max_cells: 2
    @impl true
    defdelegate init_state(key), to: CounterTask

    use Delta

    @impl true
    defdelegate state_from_json(json), to: CounterTask
    @impl true
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule CappedOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
    use Flezha.SDK.Tasklet, name: "logical", logical_clock: true
    @impl true
    def init_state(_), do: %{value: 0}

    use Delta

    @impl true
    def run(input, s, ctx) do
      emit(ctx, Integer.to_string(now(ctx)))
      {:ok, delta(s, ctx, input)}
    end
  end

  # Emits what `Ctx.now/1` served: the input's own timestamp.
  defmodule EventTimeTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "event_time"
    @impl true
    def event_time(input), do: input

    @impl true
    def run(_input, s, ctx) do
      emit(ctx, Integer.to_string(now(ctx)))
      {:ok, s}
    end
  end

  # Emits what `Ctx.now/1` served under the default clock: the epoch millisecond.
  defmodule NowTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "now"

    @impl true
    def run(_input, s, ctx) do
      emit(ctx, Integer.to_string(now(ctx)))
      {:ok, s}
    end
  end

  # Each input journals its delta and registers a deadline `input` seconds out
  # (distinct per input, whatever the clock did between two dispatches); the
  # firing journals the timestamp it was handed and emits it.
  defmodule TimerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "timer"
    @impl true
    def init_state(_), do: %{value: 0, fired: []}

    use Delta

    defchange fired(s, at) do
      %{s | fired: s.fired ++ [at]}
    end

    @impl true
    def state_from_json(%{"value" => v, "fired" => f}), do: %{value: v, fired: f}

    @impl true
    def run(input, s, ctx) do
      s = delta(s, ctx, input)
      {:ok, _at} = timer_after(ctx, input * 1_000)
      emit(ctx, Integer.to_string(s.value))
      {:ok, s}
    end

    # `now/1` inside a firing is the scheduled timestamp, whatever the clock.
    @impl true
    def on_timer(at, _kind, s, ctx) do
      s = fired(s, ctx, at)
      emit(ctx, "fired@#{at}@#{now(ctx)}")
      {:ok, s}
    end
  end

  # Fires on the input's own timeline: `arm:<at>` registers an event-time
  # timer at event time 0 (the high-water stays), `<t>` is a tick at `t`.
  defmodule EventTimerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "event_timer"
    @impl true
    def event_time({:arm, _at}), do: 0
    def event_time({:tick, t}), do: t

    @impl true
    def run({:arm, at}, s, ctx) do
      {:ok, ^at} = timer_at_event_time(ctx, at)
      {:ok, s}
    end

    def run({:tick, t}, s, ctx) do
      emit(ctx, "tick@#{t}")
      {:ok, s}
    end

    @impl true
    def on_timer(at, kind, s, ctx) do
      emit(ctx, "fired@#{at}:#{kind}:#{now(ctx)}")
      {:ok, s}
    end
  end

  # `on_timer/4` holds the Cell until told `:go`, telling the probe first,
  # so an input pushed meanwhile is dispatched after the firing.
  defmodule GatedTimerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "gated_timer"

    @impl true
    def run("arm", s, ctx) do
      {:ok, _at} = timer_after(ctx, 0)
      emit(ctx, "armed")
      {:ok, s}
    end

    def run(input, s, ctx) do
      emit(ctx, input)
      {:ok, s}
    end

    @impl true
    def on_timer(_at, _kind, s, ctx) do
      send(Process.whereis(:timer_gate_probe), :firing)

      receive do
        :go -> :ok
      end

      emit(ctx, "fired")
      {:ok, s}
    end
  end

  # A firing that fails non-retryably: the park must carry the removal.
  defmodule PoisonTimerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "poison_timer"
    @impl true
    def init_state(_), do: %{armed: 0}

    defchange armed(s, n) do
      %{s | armed: s.armed + n}
    end

    @impl true
    def run("arm", s, ctx) do
      {:ok, _at} = timer_after(ctx, 0)
      {:ok, armed(s, ctx, 1)}
    end

    def run(input, s, ctx) do
      emit(ctx, input)
      {:ok, s}
    end

    @impl true
    def on_timer(_at, _kind, _s, _ctx), do: {:error, {:non_retryable, "poison firing"}}
  end

  # Registers and never handles.
  defmodule NoHandlerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "no_handler"

    @impl true
    def run("arm", s, ctx) do
      {:ok, _at} = timer_after(ctx, 0)
      {:ok, s}
    end
  end

  # Remembers what it armed so a later input can cancel it.
  defmodule CancelTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "cancel"
    @impl true
    def init_state(_), do: %{armed: nil}

    defchange armed(s, at) do
      %{s | armed: at}
    end

    @impl true
    def state_from_json(%{"armed" => at}), do: %{armed: at}

    @impl true
    def run("arm", s, ctx) do
      {:ok, at} = timer_after(ctx, 60_000)
      {:ok, armed(s, ctx, at)}
    end

    def run("cancel", s, ctx) do
      emit(ctx, "cancelled:#{cancel_processing_time_timer(ctx, s.armed)}")
      {:ok, s}
    end

    def run(input, s, ctx) do
      emit(ctx, input)
      {:ok, s}
    end

    @impl true
    def on_timer(at, _kind, s, ctx) do
      emit(ctx, "fired@#{at}")
      {:ok, s}
    end
  end

  # Arms 100 ms out, dies untrappably on demand: the replacement owes the timer.
  defmodule KillableTimerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "killable_timer"
    @impl true
    def init_state(_), do: %{fired: []}

    defchange fired(s, at) do
      %{s | fired: s.fired ++ [at]}
    end

    @impl true
    def run("kill", _s, _ctx), do: Process.exit(self(), :kill)

    def run("arm", s, ctx) do
      {:ok, _at} = timer_after(ctx, 100)
      {:ok, s}
    end

    @impl true
    def on_timer(at, _kind, s, ctx) do
      emit(ctx, "fired@#{at}")
      {:ok, fired(s, ctx, at)}
    end
  end

  # A dynamic Cell idle-evicted after one second, holding a timer a minute out.
  defmodule TtlTimerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "ttl_timer", idle_ttl_seconds: 1
    @impl true
    def init_state(_), do: %{armed: 0}

    defchange armed(s, n) do
      %{s | armed: s.armed + n}
    end

    @impl true
    def run(_input, s, ctx) do
      {:ok, _at} = timer_after(ctx, 60_000)
      {:ok, armed(s, ctx, 1)}
    end

    @impl true
    def on_timer(at, _kind, s, ctx) do
      emit(ctx, "fired@#{at}")
      {:ok, s}
    end
  end

  # One pending timer at most; emits what each registration answered.
  defmodule CappedTimersTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "capped_timers", max_timers_per_cell: 1

    @impl true
    def run(n, s, ctx) do
      emit(ctx, inspect(timer_after(ctx, n * 1_000)))
      {:ok, s}
    end

    @impl true
    def on_timer(_at, _kind, s, _ctx), do: {:ok, s}
  end

  defmodule EventTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "gated_timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(GatedTimerTask) |> sink("g")
    end
  end

  defmodule PoisonTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "poison_timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(PoisonTimerTask) |> sink("p")
    end
  end

  defmodule NoHandlerOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "no_handler_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(NoHandlerTask) |> sink("n")
    end
  end

  defmodule CancelOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "cancel_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(CancelTask) |> sink("c")
    end
  end

  defmodule KillableTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "killable_timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(KillableTimerTask) |> sink("k")
    end
  end

  defmodule TtlTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
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
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "now_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(NowTask) |> sink("n")
    end
  end

  defmodule TimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "timer_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input_with("in", &CounterOp.parse/1) |> into(TimerTask) |> sink("t")
    end
  end

  # Idle-evicted after one second.
  defmodule TtlTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "ttl", idle_ttl_seconds: 1
    @impl true
    defdelegate init_state(key), to: CounterTask

    use Delta

    @impl true
    defdelegate state_from_json(json), to: CounterTask
    @impl true
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule TtlOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
    use Flezha.SDK.Tasklet, name: "gated_ttl", idle_ttl_seconds: 1

    @impl true
    def run(input, s, ctx) do
      receive do
        :go -> :ok
      end

      emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule GatedTtlOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
    use Flezha.SDK.Tasklet, name: "counter", schema_version: 2, min_readable_schema_version: 1

    @impl true
    def init_state(_), do: %{value: 0, unit: "n"}

    defchange delta(s, n, _note) do
      %{s | value: s.value + n}
    end

    @impl true
    def migrate_state(1, %{value: v}), do: {:ok, %{value: v, unit: "n"}}
    def migrate_state(1, _other), do: {:error, "bad shape"}
    @impl true
    def migrate_change(1, {:delta, n}), do: {:ok, {:delta, n, "migrated"}}
    @impl true
    def state_from_json(%{"value" => v, "unit" => u}), do: %{value: v, unit: u}

    @impl true
    def run(input, s, ctx) do
      s = delta(s, ctx, input, "live")
      emit(ctx, Integer.to_string(s.value))
      {:ok, s}
    end
  end

  # Claims to read version 1 and declares no migration.
  defmodule FlooredTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "floored", schema_version: 2, min_readable_schema_version: 1
    @impl true
    defdelegate init_state(key), to: CounterTask

    use Delta

    @impl true
    defdelegate run(input, s, ctx), to: CounterTask
  end

  defmodule CounterV2Op do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
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
