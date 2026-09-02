defmodule FlezhaTest.Helper.SdkTierFixtures do
  @moduledoc """
  The classes the effectively-once tier's tests drive: the twin of the
  reference's `tier_store` (a keyed upsert over a keyed plugin sink under
  `effectively_once/1`), its checkpoint postures, its poison and timer
  variants, a persistence-off variant and a two-node shape with an
  inter-procedure edge under the tier.
  """

  alias Flezha.SDK.{Channels, Ctx}

  defmodule TierStoreProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "tier_store_procedure"
    def init_state(_), do: %{}
    def apply_change(s, {k, v}), do: Map.put(s, k, v)
    def state_from_json(json), do: json

    # `commit:` closes the interval and records nothing, as the reference's
    # `checkpoint_store_procedure` does; `poison` exhausts the ladder.
    def run("poison" <> _, _s, _ctx), do: {:error, {:non_retryable, "poison value"}}

    # Ends the process untrappably mid-dispatch, before any record ships.
    def run("kill", _s, _ctx), do: Process.exit(self(), :kill)

    def run("commit:" <> _, s, ctx) do
      Ctx.checkpoint(ctx)
      {:ok, s}
    end

    def run(input, s, ctx) do
      [k, v] = String.split(input, "=", parts: 2)
      s = Ctx.apply(ctx, s, {k, v})
      Ctx.emit_keyed(ctx, k, input)
      {:ok, s}
    end
  end

  defmodule TierStoreOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "tier_store"

    def channels(c),
      do:
        c
        |> Channels.effectively_once()
        |> Channels.input("input")
        |> Channels.on_full(:drop_nothing)
        |> Channels.retry_backoff_ms(300)
        |> Channels.into(TierStoreProc)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink("tier_sink")
  end

  defmodule CheckpointBatchedOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "checkpoint_batched_store"

    def channels(c) do
      c = c |> Channels.effectively_once() |> Channels.checkpoint_ack()

      c
      |> Channels.outputs_at_checkpoint(1_500)
      |> Channels.input("input")
      |> Channels.into(TierStoreProc)
      |> Channels.named("sink")
      |> Channels.keyed()
      |> Channels.sink("tier_sink")
    end
  end

  defmodule CheckpointStoreOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "checkpoint_store"

    def channels(c),
      do:
        c
        |> Channels.effectively_once()
        |> Channels.checkpoint_ack()
        |> Channels.input("input")
        |> Channels.into(TierStoreProc)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink("tier_sink")
  end

  defmodule CheckpointEveryOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "checkpoint_every"

    def channels(c),
      do:
        c
        |> Channels.effectively_once()
        |> Channels.checkpoint_every(3)
        |> Channels.input("input")
        |> Channels.into(TierStoreProc)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink("tier_sink")
  end

  defmodule CheckpointHeldOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "checkpoint_held"

    def channels(c),
      do:
        c
        |> Channels.effectively_once()
        |> Channels.outputs_at_checkpoint(20)
        |> Channels.input("input")
        |> Channels.into(TierStoreProc)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink("tier_sink")
  end

  defmodule TierTimerProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "tier_timer"
    def init_state(_), do: %{}
    def apply_change(s, {k, v}), do: Map.put(s, k, v)

    # `arm:<at>` registers a processing-time timer that is already overdue, so
    # the firing needs no wall-clock wait.
    def run("arm:" <> at, s, ctx) do
      {:ok, _at} = Ctx.timer_after(ctx, String.to_integer(at))
      {:ok, s}
    end

    def run("poison" <> _, _s, _ctx), do: {:error, {:non_retryable, "poison value"}}

    def on_timer(at, _kind, s, ctx) do
      s = Ctx.apply(ctx, s, {"fired", at})
      Ctx.emit_keyed(ctx, "fired", "fired@#{at}")
      {:ok, s}
    end
  end

  defmodule TierTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "tier_timer_op"

    def channels(c),
      do:
        c
        |> Channels.effectively_once()
        |> Channels.input("input")
        |> Channels.into(TierTimerProc)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink("tier_sink")
  end

  defmodule HeldConsumerProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "held_consumer"
    def init_state(_), do: nil
    def mailbox_capacity, do: 1

    # Only a `hold` value blocks, and it blocks on its own mailbox until the
    # test releases it — no registered name and no `:persistent_term`, so the
    # module stays `async: true`. The timeout is the fixture's own safety
    # net: a missed release leaves no process wedged past the suite.
    def run("hold" <> _, s, _ctx) do
      receive do
        :release -> :ok
      after
        5_000 -> :ok
      end

      {:ok, s}
    end

    def run(_input, s, _ctx), do: {:ok, s}
  end

  defmodule EdgeProducerProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "edge_producer"
    def init_state(_), do: %{}
    def apply_change(s, {k, v}), do: Map.put(s, k, v)

    def run(input, s, ctx) do
      [k, v] = String.split(input, "=", parts: 2)
      s = Ctx.apply(ctx, s, {k, v})
      Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule TierEdgeOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "tier_edge"

    def channels(c) do
      c
      |> Channels.effectively_once()
      |> Channels.input("input")
      |> Channels.into(EdgeProducerProc)
      |> Channels.named("producer")
      |> Channels.into(HeldConsumerProc)
      |> Channels.named("consumer")
      |> Channels.sealed()
    end
  end

  defmodule StatelessTierProc do
    @moduledoc false
    @behaviour Flezha.SDK.Procedure
    def name, do: "stateless_tier"
    def init_state(_), do: nil

    # No `apply_change/2`: the class holds nothing, and under the tier its
    # output and its offset still have to reach the controller.
    def run(input, s, ctx) do
      [k, _v] = String.split(input, "=", parts: 2)
      Ctx.emit_keyed(ctx, k, input)
      {:ok, s}
    end
  end

  defmodule StatelessTierOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "stateless_tier"

    def channels(c),
      do:
        c
        |> Channels.effectively_once()
        |> Channels.input("input")
        |> Channels.into(StatelessTierProc)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink("tier_sink")
  end

  defmodule TierStoreNoPersistOp do
    @moduledoc false
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "tier_store_nopersist"
    def persistence_enabled, do: false
    defdelegate channels(c), to: TierStoreOp
  end
end
