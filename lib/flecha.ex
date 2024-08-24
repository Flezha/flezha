defmodule Flecha do
  @moduledoc """
  Documentation for `FlechaCore`.
  """

  def add_computation(computation) do
    GenServer.call(Flecha.Controller, {:add_computation, computation})
  end
end
