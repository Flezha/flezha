defmodule FlezhaTest.Helper.SdkSpawnFixtures do
  @moduledoc """
  The unbounded class the tasklet-requested spawn tests drive: one spawn per
  input, named by the input, whose answer is read inside the invocation and
  emitted on `out`. An `unread:` input spawns and reads nothing, so a failure
  nobody reads has something to be logged for.
  """

  alias Flezha.SDK.{Channels, Spawn}

  defmodule Spawner do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "spawner"

    @impl true
    def run("unread:" <> instance_id, state, ctx) do
      spawn_bounded(ctx, "worker", "payload", id: instance_id)
      emit(ctx, "sent")
      {:ok, state}
    end

    def run(instance_id, state, ctx) do
      handle = spawn_bounded(ctx, "worker", "payload", id: instance_id)
      emit(ctx, inspect(Spawn.await(handle, 5_000)))
      {:ok, state}
    end
  end

  defmodule SpawnerOp do
    @moduledoc false
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "spawner"

    def channels(c),
      do: c |> Channels.input("in") |> Channels.into(Spawner) |> Channels.sink("out")
  end
end
