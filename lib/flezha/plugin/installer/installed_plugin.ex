defmodule Flezha.Plugin.Installer.InstalledPlugin do
  @moduledoc """
  Return shape of `Flezha.Plugin.Installer.list/0` and `info/1`. Projects
  the DETS catalog row plus the subset of manifest fields runners want
  to see on the CLI / UI.
  """

  @type status :: :enabled | :disabled | :load_failed | :pending_upgrade

  @type t :: %__MODULE__{
          id: binary(),
          version: binary(),
          status: status(),
          description: binary() | nil,
          author: binary() | nil,
          provides: [map()],
          modules: [atom()],
          installed_at: integer() | nil
        }

  @enforce_keys [:id, :version, :status]
  defstruct [
    :id,
    :version,
    :status,
    :description,
    :author,
    :installed_at,
    provides: [],
    modules: []
  ]

  @doc "Projects a `{plugin_id, row}` tuple from the Store to a struct."
  @spec from_row({binary(), map()}) :: t()
  def from_row({id, row}) do
    %__MODULE__{
      id: id,
      version: row[:version],
      status: row[:status],
      description: row[:description],
      author: row[:author],
      installed_at: row[:installed_at],
      provides: Map.get(row, :provides, []),
      modules: Map.get(row, :modules, [])
    }
  end
end
