defmodule FlezhaTest.Helper.SdkPersistenceFixtures do
  @moduledoc """
  The procedures and operatives the SDK's persistence tests drive: a
  changeful counter in Single, Static and Dynamic shapes, a `NoChange` echo, a
  class with persistence off, the two shapes the builder refuses, and the keyed
  sinks whose frames are stamped.
  """

  alias Flezha.SDK.Ctx

  defmodule CounterProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
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

  defmodule EchoProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "echo"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule ThriftyProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "thrifty"
    def init_state(_), do: %{value: 0}
    def compact_after_bytes, do: 1_048_576
    defdelegate apply_change(s, c), to: CounterProc
    defdelegate run(input, s, ctx), to: CounterProc
  end

  defmodule StaticCounterProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "scounter"
    def entity_keys, do: ["a", "b"]
    def entities_for(_input), do: ["a"]
    defdelegate init_state(key), to: CounterProc
    defdelegate apply_change(s, change), to: CounterProc
    defdelegate state_from_json(json), to: CounterProc
    defdelegate run(input, s, ctx), to: CounterProc
  end

  defmodule CounterOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "counter_op"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input_with("in", &parse/1) |> into(CounterProc) |> named("c") |> sink("out")
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
      c |> input("in") |> into(EchoProc) |> named("e") |> sink("out")
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
      |> into(StaticCounterProc)
      |> named("s")
      |> sink("out")
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
      |> into(CounterProc)
      |> named("c")
      |> sink("out")
    end
  end

  defmodule TwoCountersOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "two_counters"

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("a") |> into_named(CounterProc, "first") |> sink("first_out")
      c |> input("b") |> into_named(CounterProc, "second") |> sink("second_out")
    end
  end

  defmodule OptedOutOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "opted_out"
    def persistence_enabled, do: false

    def channels(c) do
      import Flezha.SDK.Channels
      c |> input("in") |> into(ThriftyProc) |> into(CounterProc) |> sink("out")
    end
  end

  # Emits twice per input — once with user metadata — so the ordinal and the
  # stamps' position after the metadata are both observable on one dispatch.
  defmodule StampedProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
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
  defmodule RefusingProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
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
      |> into(StampedProc)
      |> named("c")
      |> keyed()
      |> sink("out")
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
        |> into(StampedProc)
        |> named("c")

      node |> keyed() |> sink("out")
      node |> sink("plain")
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
      |> into(RefusingProc)
      |> named("r")
      |> keyed()
      |> sink("out")
    end
  end

  # Two live Entities at most: a third key's input has nowhere to go.
  defmodule CappedProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "capped"
    def max_entities, do: 2
    defdelegate init_state(key), to: CounterProc
    defdelegate apply_change(s, change), to: CounterProc
    defdelegate state_from_json(json), to: CounterProc
    defdelegate run(input, s, ctx), to: CounterProc
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
      |> into(CappedProc)
      |> named("c")
      |> sink("out")
    end
  end

  # Idle-evicted after 100 ms, on a sweep every 25 ms.
  defmodule TtlProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "ttl"
    def entity_idle_ttl_ms, do: 100
    defdelegate init_state(key), to: CounterProc
    defdelegate apply_change(s, change), to: CounterProc
    defdelegate state_from_json(json), to: CounterProc
    defdelegate run(input, s, ctx), to: CounterProc
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
      |> into(TtlProc)
      |> named("c")
      |> sink("out")
    end
  end

  # Blocks inside `run/3` until told `:go`, so a sweep meets it busy; a
  # NoChange procedure, so its eviction ships no delete frame.
  defmodule GatedTtlProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "gated_ttl"
    def entity_idle_ttl_ms, do: 1
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
      c |> input("in") |> key_by(& &1) |> into(GatedTtlProc) |> named("g") |> sink("out")
    end
  end

  # The counter one schema version on: `unit` joined the state, a note joined
  # the change, and both read back from version 1 through the migrations.
  defmodule CounterV2Proc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure

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
  defmodule FlooredProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "floored"
    def schema_version, do: 2
    def min_readable_schema_version, do: 1
    defdelegate init_state(key), to: CounterProc
    defdelegate apply_change(s, change), to: CounterProc
    defdelegate run(input, s, ctx), to: CounterProc
  end

  defmodule CounterV2Op do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "counter_v2_op"

    def channels(c) do
      import Flezha.SDK.Channels

      c
      |> input_with("in", &CounterOp.parse/1)
      |> into(CounterV2Proc)
      |> named("c")
      |> sink("out")
    end
  end
end
