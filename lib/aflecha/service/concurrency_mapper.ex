defmodule Aflecha.Service.ConcurrencyMapper do
  def concurrent_entity_count() do
    2
    # TODO: Should be configurable also.
    # System.schedulers_online() * 2
  end
end
