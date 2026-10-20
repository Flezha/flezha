defmodule Flezha.State.Ref.Cspec do
  @moduledoc "`{:cspec, {hub_id, writer}}`: a hub's registry cspec, one replica per controller."

  use Flezha.State.Ref

  alias Flezha.Layout.Table, as: Layout
  alias Flezha.State.Ref

  def payload_files({:cspec, {hub_id, _writer}}),
    do: Ref.present(Layout.file(:process_hub, hub_id), [Layout.hub_registry_file()])
end
