# The Elixir bench factories, mirroring test/apps/rust_runner/src/main.rs
# under the same factory ids and topologies so `mix bench --runtime elixir`
# measures the same shapes:
#   do_nothing     — regression: input → echo → sink
#   entity_stress  — entity:     A(8 keys) → B → C → D → sink (fan-out then fan-in)
#   dynamic_entity — dynamic:    filter + key_by(prefix) → keyed Entity → sink
# plus the `mix bench.roundtrip` twins:
#   bench_echo            — do_nothing with a declared Block on the entry mailbox
#   bench_keyed_nopersist — "key:value" upsert, emits "key=value" (Single Entity)
# Lives beside the bench task (not under test/) because the in-process
# `--transport native` bench needs it in the controller's own build; the
# standalone test/apps/elixir_runner registers the same modules from the dep.

defmodule Flezha.Bench.EchoProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "do_nothing_procedure"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
end

defmodule Flezha.Bench.DoNothing do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "do_nothing"

  def channels(c) do
    import Flezha.SDK.Channels
    c |> input("input") |> into(Flezha.Bench.EchoProc) |> named("echo") |> sink("do_nothing_echo")
  end
end

defmodule Flezha.Bench.StressA do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "stress_a"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
  # The shared 8-key static set (Rust's `STRESS_KEYS`); B/C/D declare the same
  # keyset and narrow the routing to `p0` through `entities_for/1`.
  def entity_keys, do: ~w(p0 p1 p2 p3 p4 p5 p6 p7)
end

defmodule Flezha.Bench.StressB do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "stress_b"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
  defdelegate entity_keys, to: Flezha.Bench.StressA
  def entities_for(_), do: ["p0"]
end

defmodule Flezha.Bench.StressC do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "stress_c"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
  defdelegate entity_keys, to: Flezha.Bench.StressA
  def entities_for(_), do: ["p0"]
end

defmodule Flezha.Bench.StressD do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "stress_d"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
  defdelegate entity_keys, to: Flezha.Bench.StressA
  def entities_for(_), do: ["p0"]
end

defmodule Flezha.Bench.EntityStress do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "entity_stress"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> into(Flezha.Bench.StressA)
    |> named("a_to_b")
    |> into(Flezha.Bench.StressB)
    |> named("b_to_c")
    |> into(Flezha.Bench.StressC)
    |> named("c_to_d")
    |> into(Flezha.Bench.StressD)
    |> named("echo")
    |> sink("entity_stress_echo")
  end
end

defmodule Flezha.Bench.DynamicProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "dynamic_entity_procedure"
  def init_state(_), do: %{seen: []}

  # Rust journals the push as a Change and evicts on `DYN_TEST_TTL_MS`; v1 has
  # neither, so the state is a plain term and the Entity lives for the
  # operative's lifetime.
  def run(input, s, ctx) do
    Flezha.SDK.Ctx.emit(ctx, input)
    {:ok, %{s | seen: [input | s.seen]}}
  end
end

defmodule Flezha.Bench.DynamicEntity do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "dynamic_entity"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:block)
    |> filter(fn s -> s != "drop" end)
    |> key_by(fn s -> s |> to_string() |> String.split(":") |> hd() end)
    |> into(Flezha.Bench.DynamicProc)
    |> named("kafka")
    |> sink("test_kafka")
  end
end

defmodule Flezha.Bench.BenchEcho do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bench_echo"

  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:block)
    |> into(Flezha.Bench.EchoProc)
    |> named("echo")
    |> sink("bench_echo_sink")
  end
end

defmodule Flezha.Bench.TierStoreProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "tier_store_procedure"
  def init_state(_), do: %{}

  # Rust emits `key=value` keyed on `key`; v1 has no `emit_keyed`, same bytes unkeyed.
  def run(input, values, ctx) do
    {key, value} =
      case String.split(input, ":", parts: 2) do
        [key, value] -> {key, value}
        [key] -> {key, ""}
      end

    if value == "poison" do
      {:error, "poison value"}
    else
      Flezha.SDK.Ctx.emit(ctx, "#{key}=#{value}")
      {:ok, Map.put(values, key, value)}
    end
  end
end

defmodule Flezha.Bench.KeyedNoPersist do
  @moduledoc false
  @behaviour Flezha.SDK.StreamOperative
  def factory_id, do: "bench_keyed_nopersist"

  # Rust switches the class's per-Entity persistence off; v1 has no persistence
  # at all, so this is the heap-only posture by construction.
  def channels(c) do
    import Flezha.SDK.Channels

    c
    |> input("input")
    |> on_full(:block)
    |> into(Flezha.Bench.TierStoreProc)
    |> named("sink")
    |> sink("tier_sink")
  end
end

defmodule Mix.Tasks.Bench.ElixirFactories do
  @moduledoc "Registry of the Elixir bench factory operatives (see this file's header)."

  @doc "Every bench factory operative module."
  def all do
    [
      Flezha.Bench.DoNothing,
      Flezha.Bench.EntityStress,
      Flezha.Bench.DynamicEntity,
      Flezha.Bench.BenchEcho,
      Flezha.Bench.KeyedNoPersist
    ]
  end
end
