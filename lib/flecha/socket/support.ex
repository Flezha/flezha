defmodule Flecha.Socket.Support do
  def prepare_socket_dir() do
    File.rm_rf("app_data/sockets")
    File.mkdir_p("app_data/sockets")
  end

  def set_active(socket) do
    :inet.setopts(socket, [{:active, true}])
  end

  def pass_ownership(socket, pid) do
    :gen_tcp.controlling_process(socket, pid)
  end
end
