defmodule Flecha.Engine.StreamSupervisor do
  alias Flecha.Service.ConcurrencyMapper
  alias Flecha.Engine.StreamChannel
  alias Flecha.Utility.Identifier

  use Supervisor

  def start_link(engine_id) do
    Supervisor.start_link(__MODULE__, engine_id, name: Identifier.stream_sup(engine_id))
  end

  @impl true
  def init(engine_id) do
    stream_listener = Identifier.stream_listener(engine_id)
    lsock = GenServer.call(stream_listener, :get_socket)

    children =
      for _x <- 1..ConcurrencyMapper.concurrent_entity_count() do
        {StreamChannel, lsock}
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
