defmodule Flecha.ChannelSupervisor do
  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(_args) do
    children = children()

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp children() do
    # TODO: maybe 10x threads per scheduler? or configurable.
    # Get the number of available cores and double it.
    core_count = System.schedulers_online() * 2

    # Get all processors.
    processors = Application.get_env(:flecha_core, :computation_engines)

    Enum.map(processors, fn {engine_name, _} ->
      Enum.map(1..core_count, fn x ->
        channel_name = (Integer.to_string(x) <> "_" <> engine_name) |> String.to_atom()

        %{
          id: channel_name,
          start: {Flecha.Channel, :start_link, [channel_name]}
        }
      end)
    end)
    |> List.flatten()
  end
end
