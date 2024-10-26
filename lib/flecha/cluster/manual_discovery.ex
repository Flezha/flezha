defmodule Flecha.Cluster.ManualDiscovery do
  alias Flecha.Cluster.DiscoveryProtocol

  @type t() :: %__MODULE__{
          initial_nodes: [node()]
        }
  defstruct [:initial_nodes]

  defimpl DiscoveryProtocol, for: Flecha.Cluster.ManualDiscovery do
    @impl true
    def get_nodes(struct) do
      nodes =
        if length(struct.initial_nodes) > 0 do
          (struct.initial_nodes ++ Node.list()) |> Enum.uniq()
        else
          Node.list()
        end

      {:ok, Map.put(struct, :initial_nodes, []), nodes}
    end

    @impl true
    def validate_settings(_struct) do
      static_nodes = Application.fetch_env!(:flecha_core, :static_nodes)

      cond do
        is_list(static_nodes) ->
          {:ok, __MODULE__}

        true ->
          {:error,
           "Invalid static nodes configuration. Expected a empty list or a list of nodes."}
      end
    end
  end
end
