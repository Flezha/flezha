defmodule Aflecha.Service.EventManager do
  alias :blockade, as: Blockade

  @event_manager :aflecha_event_manager

  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {Blockade, :start_link, [%{name: @event_manager}]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def start_link(opts) do
    Blockade.start_link(%{name: opts[:name]})
  end

  def add_handler(event_name) do
    Blockade.add_handler(@event_manager, event_name)
  end

  def dispatch(event_name, event_data, opts \\ %{}) do
    Blockade.dispatch(@event_manager, event_name, event_data, opts)
  end
end
