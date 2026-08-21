# The Elixir bench factory trio, mirroring test/apps/rust_runner/src/main.rs
# under the same factory ids and topologies so `mix bench --runtime elixir`
# measures the same shapes:
#   do_nothing     — regression: input → echo → sink
#   entity_stress  — entity:     A(8 keys) → B → C → D → sink (fan-out then fan-in)
#   dynamic_entity — dynamic:    filter + key_by(prefix) → keyed Entity → sink
# Lives beside the bench task (not under test/) because the in-process
# `--transport native` bench needs it in the controller's own build; the
# standalone test/apps/elixir_runner registers the same modules from the dep.

defmodule Flezha.Bench.EchoProc do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "echo"
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
  def entity_keys, do: for(n <- 0..7, do: "p#{n}")
end

defmodule Flezha.Bench.StressB do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "stress_b"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
  def entity_keys, do: ["p0"]
end

defmodule Flezha.Bench.StressC do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "stress_c"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
  def entity_keys, do: ["p0"]
end

defmodule Flezha.Bench.StressD do
  @moduledoc false
  @behaviour Flezha.SDK.Procedure
  def name, do: "stress_d"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
  def entity_keys, do: ["p0"]
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
  def name, do: "dynamic"
  def init_state(_), do: nil
  def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
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

defmodule Mix.Tasks.Bench.ElixirFactories do
  @moduledoc "Registry of the Elixir bench factory operatives (see this file's header)."

  @doc "Every bench factory operative module."
  def all, do: [Flezha.Bench.DoNothing, Flezha.Bench.EntityStress, Flezha.Bench.DynamicEntity]
end
