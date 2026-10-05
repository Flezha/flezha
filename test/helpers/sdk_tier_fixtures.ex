defmodule FlezhaTest.Helper.SdkTierFixtures do
  @moduledoc """
  The classes the effectively-once tier's tests drive: the twin of the
  reference's `tier_store` (a keyed upsert over a keyed plugin sink under
  `effectively_once/1`), its checkpoint postures, its poison and timer
  variants, a persistence-off variant and a two-node shape with an
  inter-tasklet edge under the tier, and the correction probes — an
  event-time class recording nothing but the ORDER its inputs were folded
  in, which is the only thing a correction changes.
  """

  alias Flezha.SDK.Channels

  defmodule TierStoreTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tier_store_tasklet"
    @impl true
    def init_state(_), do: %{}

    defchange set(s, k, v) do
      Map.put(s, k, v)
    end

    @impl true
    def state_from_json(json), do: json

    # `commit:` closes the interval and records nothing, as the reference's
    # `checkpoint_store_tasklet` does; `poison` exhausts the ladder.
    @impl true
    def run("poison" <> _, _s, _ctx), do: {:error, {:non_retryable, "poison value"}}

    # Ends the process untrappably mid-dispatch, before any record ships.
    def run("kill", _s, _ctx), do: Process.exit(self(), :kill)

    def run("commit:" <> _, s, ctx) do
      checkpoint(ctx)
      {:ok, s}
    end

    def run(input, s, ctx) do
      [k, v] = String.split(input, "=", parts: 2)
      s = set(s, ctx, k, v)
      emit_keyed(ctx, k, input)
      {:ok, s}
    end
  end

  defmodule TierStoreOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "tier_store"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.on_full(:drop_nothing)
        |> Channels.retry_backoff_ms(300)
        |> Channels.into(TierStoreTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end

  defmodule CheckpointBatchedOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "checkpoint_batched_store"

    def run,
      do:
        {:flow,
         exactly:
           {:effectively_once, ack: {:checkpoint, outputs: :at_checkpoint, max_delay_ms: 1_500}}}

    def channels(c) do
      c
      |> Channels.input("input")
      |> Channels.into(TierStoreTask)
      |> Channels.named("sink")
      |> Channels.keyed()
      |> Channels.sink()
    end
  end

  defmodule CheckpointStoreOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "checkpoint_store"
    def run, do: {:flow, exactly: {:effectively_once, ack: :checkpoint}}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.into(TierStoreTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end

  defmodule CheckpointEveryOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "checkpoint_every"
    def run, do: {:flow, exactly: {:effectively_once, ack: {:checkpoint, every: 3}}}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.into(TierStoreTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end

  defmodule CheckpointHeldOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "checkpoint_held"

    def run,
      do:
        {:flow,
         exactly:
           {:effectively_once, ack: {:checkpoint, outputs: :at_checkpoint, max_delay_ms: 20}}}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.into(TierStoreTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end

  defmodule TierTimerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "tier_timer"
    @impl true
    def init_state(_), do: %{}

    defchange set(s, k, v) do
      Map.put(s, k, v)
    end

    # `arm:<at>` registers a processing-time timer that is already overdue, so
    # the firing needs no wall-clock wait.
    @impl true
    def run("arm:" <> at, s, ctx) do
      {:ok, _at} = timer_after(ctx, String.to_integer(at))
      {:ok, s}
    end

    def run("poison" <> _, _s, _ctx), do: {:error, {:non_retryable, "poison value"}}

    @impl true
    def on_timer(at, _kind, s, ctx) do
      s = set(s, ctx, "fired", at)
      emit_keyed(ctx, "fired", "fired@#{at}")
      {:ok, s}
    end
  end

  defmodule TierTimerOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "tier_timer_op"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.into(TierTimerTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end

  defmodule HeldConsumerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "held_consumer", mailbox_capacity: 1

    # Only a `hold` value blocks, and it blocks on its own mailbox until the
    # test releases it — no registered name and no `:persistent_term`, so the
    # module stays `async: true`. The timeout is the fixture's own safety
    # net: a missed release leaves no process wedged past the suite.
    @impl true
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

  defmodule EdgeProducerTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "edge_producer"
    @impl true
    def init_state(_), do: %{}

    defchange set(s, k, v) do
      Map.put(s, k, v)
    end

    @impl true
    def run(input, s, ctx) do
      [k, v] = String.split(input, "=", parts: 2)
      s = set(s, ctx, k, v)
      emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule TierEdgeOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "tier_edge"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c) do
      c
      |> Channels.input("input")
      |> Channels.into(EdgeProducerTask)
      |> Channels.named("producer")
      |> Channels.into(HeldConsumerTask)
      |> Channels.named("consumer")
    end
  end

  defmodule StatelessTierTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "stateless_tier"

    # No `apply_change/2`: the class holds nothing, and under the tier its
    # output and its offset still have to reach the controller.
    @impl true
    def run(input, s, ctx) do
      [k, _v] = String.split(input, "=", parts: 2)
      emit_keyed(ctx, k, input)
      {:ok, s}
    end
  end

  defmodule StatelessTierOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "stateless_tier"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.into(StatelessTierTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end

  # The correction probes. Deliberately NOT the reference's `candle`: no bucket
  # arithmetic and no emit format, because the only claim a unit test makes here
  # is about fold ORDER. Input is `<event_time>:<label>`.
  defmodule CorrectionTask do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "correction_tasklet"
    @impl true
    def init_state(_), do: %{order: []}

    # Newest first, and the change carries only the event time: nothing here
    # needs the app fixture's bucketing or its JSON shape.
    defchange fold(s, at) do
      %{s | order: [at | s.order]}
    end

    @impl true
    def event_time(input) do
      {at, _rest} = Integer.parse(input)
      at
    end

    @impl true
    def run(input, s, ctx) do
      at = now(ctx)
      emit_keyed(ctx, "fold", input)
      {:ok, fold(s, ctx, at)}
    end
  end

  defmodule CorrectionOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "correction_probe"
    def run, do: {:flow, exactly: {:effectively_once, correction_ms: 600_000}}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.into(CorrectionTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end

  defmodule CorrectionDropOp do
    @moduledoc false
    @behaviour Flezha.SDK.Relay
    def factory_id, do: "correction_probe_drop"

    def run,
      do: {:flow, exactly: {:effectively_once, correction_ms: 600_000, beyond_window: :drop}}

    def channels(c),
      do:
        c
        |> Channels.input("input")
        |> Channels.into(CorrectionTask)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
  end
end
