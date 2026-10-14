defmodule FlezhaTest.Helper.StreamPair do
  @moduledoc """
  A controller-side NIF stream with a peer the test reads: a UDS server of its
  own, dialled once. A frame the controller writes on the stream reaches the
  calling process as `{:flz_data, 0, payload_id, message, metadata}`, the shape
  a runner's own stream delivers.
  """

  alias Flezha.Transport.Native
  alias FlezhaTest.Helper.ControlFrameTestHelper

  @doc "The controller's side of a fresh pair, and the function that closes the pair."
  @spec open() :: {reference(), (-> :ok)}
  def open do
    # Short and private: a socket path has a length limit and a fixed name.
    dir = Path.join(System.tmp_dir!(), "flzpair#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "p.sock")
    {:ok, server} = Native.start_uds_server(path, self())
    socket = peer(path)

    stream =
      receive do
        {:flz_connection, stream} -> stream
      after
        5_000 -> raise "the pair's server never accepted its own dial"
      end

    close = fn ->
      :gen_tcp.close(socket)
      Native.stop_server(server)
      File.rm_rf!(dir)
      :ok
    end

    {stream, close}
  end

  @doc """
  A peer dialling the UDS socket at `path`. Its frames reach the calling
  process as `{:flz_data, 0, payload_id, message, metadata}`, and it closes
  when the calling process exits.
  """
  @spec peer(Path.t()) :: port()
  def peer(path) do
    {:ok, socket} = :gen_tcp.connect({:local, path}, 0, [:binary, active: false])
    owner = self()

    reader =
      spawn_link(fn ->
        receive do
          :go -> :ok = :inet.setopts(socket, active: true)
        end

        read(socket, owner, <<>>)
      end)

    :ok = :gen_tcp.controlling_process(socket, reader)
    send(reader, :go)
    socket
  end

  defp read(socket, owner, buffered) do
    receive do
      {:tcp, ^socket, bytes} ->
        {frames, rest} = ControlFrameTestHelper.decode_frames(buffered <> bytes)

        for {id, message, metadata} <- frames,
            do: send(owner, {:flz_data, 0, id, message, metadata})

        read(socket, owner, rest)

      {:tcp_closed, ^socket} ->
        :ok
    end
  end
end
