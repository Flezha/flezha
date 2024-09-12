defmodule Flecha.Service.ConcurrencyMapper do
  def concurrent_entity_count() do
    System.schedulers_online() * 2
  end
end
