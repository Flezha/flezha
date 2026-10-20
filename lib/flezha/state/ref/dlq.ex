defmodule Flezha.State.Ref.Dlq do
  @moduledoc "`{:dlq, {relay_class, tasklet_id}}`: a tasklet's parked failures, packed whole."

  use Flezha.State.Ref

  alias Flezha.Dlq
  alias Flezha.State.Ref

  def payload_files({:dlq, {relay_class, tasklet_id}}) do
    root = Dlq.Store.task_dir(relay_class, tasklet_id)

    case File.ls(root) do
      {:ok, names} -> Ref.present(root, names)
      {:error, _} -> {root, []}
    end
  end
end
