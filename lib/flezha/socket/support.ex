defmodule Flezha.Socket.Support do
  def prepare_socket_dir() do
    base_path = sockets_base_path()

    File.rm_rf(base_path)
    File.mkdir_p(base_path)
  end

  @spec set_active(port() | {:"$inet", atom(), any()}) :: :ok | {:error, atom()}
  def set_active(socket) do
    :inet.setopts(socket, [{:active, true}])
  end

  @spec pass_ownership(port() | {:"$inet", atom(), any()}, pid()) :: :ok | {:error, atom()}
  def pass_ownership(socket, pid) do
    :gen_tcp.controlling_process(socket, pid)
  end

  def control_socket_path() do
    sockets_base_path() <> "/executor_control_channel.sock"
  end

  def stream_socket_path(executor_id, type) when is_atom(executor_id) do
    stream_socket_path(Atom.to_string(executor_id), type)
  end

  def stream_socket_path(executor_id, type) when is_binary(executor_id) do
    sockets_base_path() <> "/#{Atom.to_string(type)}_stream_#{executor_id}.sock"
  end

  defp sockets_base_path() do
    Application.fetch_env!(:flezha, :socket_files_path)
    |> String.replace_trailing("/", "")
  end
end
