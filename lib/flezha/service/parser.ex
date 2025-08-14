defmodule Flezha.Service.Parser do
  def string_to_pid(pname) when is_binary(pname) do
    try do
      pname = String.to_existing_atom(pname)

      case Process.whereis(pname) do
        nil -> {:error, "Process #{pname} not found"}
        pid -> pid
      end
    catch
      _ -> {:error, "Invalid PID format"}
    end
  end
end
