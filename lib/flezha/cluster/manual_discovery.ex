defmodule Flezha.Cluster.ManualDiscovery do
  @moduledoc """
  Discovery from a statically configured node list, plus whatever is already
  connected. The fallback strategy when no cluster API is available.
  """

  alias Flezha.Cluster.DiscoveryProtocol
  alias Flezha.Config

  @type t() :: %__MODULE__{
          initial_nodes: [node()]
        }
  defstruct [:initial_nodes]

  defimpl DiscoveryProtocol, for: Flezha.Cluster.ManualDiscovery do
    @impl true
    def get_nodes(struct) do
      nodes =
        if struct.initial_nodes != [] do
          (struct.initial_nodes ++ Node.list()) |> Enum.uniq()
        else
          Node.list()
        end

      {:ok, Map.put(struct, :initial_nodes, []), nodes}
    end

    @impl true
    def validate_settings(_struct) do
      static_nodes = Config.get(:static_nodes)

      if is_list(static_nodes) do
        {:ok, __MODULE__}
      else
        {:error, {:invalid_static_nodes, static_nodes}}
      end
    end
  end
end
