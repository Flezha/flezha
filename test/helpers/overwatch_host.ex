defmodule FlezhaTest.Helper.OverwatchHost do
  @moduledoc """
  A bare process around an `Flezha.Operator.Overwatch` struct, for the cases
  whose unit is the Overwatch's loop: a durable log answers its mailbox, and a
  reply it owes a caller is a real call that has not returned. Started with
  `{:absorb, state, handover}` it absorbs a handover itself, so the deadlines
  the handover re-arms are its own timers.
  """
  use GenServer

  alias Flezha.Operator.{Builder, Handover, Overwatch}
  alias Flezha.Plugin.Extension.Table

  def start_link(arg), do: GenServer.start_link(__MODULE__, arg)

  @impl true
  def init({:absorb, state, handover}), do: {:ok, Handover.absorb(state, handover)}
  def init(%Overwatch{} = state), do: {:ok, state}

  @impl true
  defdelegate handle_call(msg, from, state), to: Overwatch
  @impl true
  defdelegate handle_cast(msg, state), to: Overwatch
  @impl true
  defdelegate handle_info(msg, state), to: Overwatch

  @doc "A flow Overwatch of the `chat` factory for `inst`, with `overrides`."
  def state(inst, overrides \\ []) do
    struct!(
      %Overwatch{
        id: inst,
        namespace: "default",
        runner_id: "ex_1",
        factory_id: "chat",
        builder: %Builder{id: inst, factory_id: "chat", run: :flow, opts: []},
        extensions: Table.new(Overwatch.__extensions_catalog__())
      },
      overrides
    )
  end
end
