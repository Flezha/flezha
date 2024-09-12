defmodule Flecha.Socket.Support do
  def prepare_socket_dir() do
    File.rm_rf("app_data/sockets")
    File.mkdir_p("app_data/sockets")
  end

  def take_ownership(socket) do
    :gen_tcp.controlling_process(socket, self())
    :inet.setopts(socket, [{:active, true}])
  end
end
