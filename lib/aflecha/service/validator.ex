defmodule Aflecha.Service.Validator do
  alias Aflecha.Service.Identificator

  use Aflecha.Constant

  def byte_limit_exceed(value, max_length \\ @worker_id_max_bytes) do
    case byte_size(value) > max_length do
      true -> {:error, "worker id exceeds #{@worker_id_max_bytes} bytes"}
      false -> :ok
    end
  end

  def worker_unique(group_id, worker_id) do
    result =
      ProcessHub.child_lookup(
        Identificator.hub(group_id),
        worker_id
      )

    case result do
      nil -> :ok
      _ -> {:error, "worker id already in use"}
    end
  end
end
