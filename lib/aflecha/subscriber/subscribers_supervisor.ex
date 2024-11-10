defmodule Aflecha.Subscriber.SubscribersSupervisor do
  alias Aflecha.Subscriber.SubscriberSupervisor

  use Supervisor

  def start_link(_) do
    Supervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    Supervisor.init([], strategy: :one_for_one)
  end

  def start_subscriber(group_id, sock) do
    Supervisor.start_child(__MODULE__, %{
      id: group_id,
      start: {SubscriberSupervisor, :start_link, [{group_id, sock}]}
    })
  end

  def default_group() do
    child =
      DynamicSupervisor.which_children(__MODULE__)
      |> Enum.at(0)

    case child do
      {group_id, _, _, _} -> group_id
      _ -> raise "No subscribers available"
    end
  end
end
