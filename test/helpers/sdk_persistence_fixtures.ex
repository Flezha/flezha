defmodule FlezhaTest.Helper.SdkPersistenceFixtures do
  @moduledoc """
  The procedures and operatives the SDK's persistence tests drive: a
  changeful counter in Single, Static and Dynamic shapes, a `NoChange` echo, a
  class with persistence off, and the two shapes the builder refuses.
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
end
