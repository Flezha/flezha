defmodule Aflecha.Subscriber.SubscribersSupervisor do
  alias Aflecha.Subscriber.SubscriberSupervisor

  use DynamicSupervisor

  def start_link(_) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def start_subscriber(subscriber_id, sock) do
    DynamicSupervisor.start_child(__MODULE__, %{
      id: subscriber_id,
      start: {SubscriberSupervisor, :start_link, [{subscriber_id, sock}]}
    })
  end

  @impl true
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
