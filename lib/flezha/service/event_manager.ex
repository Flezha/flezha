defmodule Flezha.Service.EventManager do
  alias Flezha.Process.Cspec
  alias :blockade, as: Blockade

  @event_manager :flezha_event_manager

  def child_spec(_opts) do
    Cspec.build(__MODULE__, {Blockade, :start_link, [%{name: @event_manager}]}, :worker_permanent,
      shutdown: 500
    )
  end

  def start_link(opts) do
    Blockade.start_link(%{name: opts[:name]})
  end

  def add_handler(event_name) do
    Blockade.add_handler(@event_manager, event_name)
  end

  def rem_handler(event_name) do
    Blockade.remove_handler(@event_manager, event_name)
  end

  def dispatch(event_name, event_data, opts \\ %{}) do
    Blockade.dispatch(@event_manager, event_name, event_data, opts)
  end
end
