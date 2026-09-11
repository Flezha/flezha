defmodule Flezha.Socket.SupportTest do
  @moduledoc """
  The controller's end of the socket names: a per-runner data socket must fit
  `sun_path` whatever the runner id, and must be the same string the runner's
  own side composes from a directory it resolved separately.
  """
  # async: false: repoints the controller's socket directory in Application env.
  use ExUnit.Case, async: false

  alias Flezha.Socket.Support
  alias Flezha.State.TestHelpers
  alias Flezha.Transport.Native

  # A socket directory of 48 bytes, the most `Environment.per_run/2` mints under
  # `/tmp` (a 7-digit token, a 20-digit peer index), and a runner id at the width
  # a late-scheduled module draws (40 characters).
  @peer_dir "/tmp/flezha_sockets/native_reject_peer@127.0.0.1"
  @long_id "schema_evolution_e2e_419362_660179359383"

  setup do
    File.mkdir_p!(@peer_dir)
    on_exit(TestHelpers.override_env(:socket_files_path, @peer_dir))
    :ok
  end

  test "a data socket for a long runner id fits a peer node's socket directory" do
    path = Support.stream_socket_path(@long_id, :secondary)

    assert byte_size(path) <= 107,
           "#{path} is #{byte_size(path)} bytes; sun_path holds 107 and a NUL"

    File.rm(path)
    assert {:ok, server_ref} = Native.start_uds_server(path, self())
    Native.stop_server(server_ref)
    File.rm(path)
  end

  test "the id field is the id's own prefix and the digest of the whole id" do
    # The digest is SHA-256 over the runner id's bytes, so it is reproducible
    # outside this repository: `printf %s <id> | sha256sum`.
    assert Support.stream_socket_path(@long_id, :secondary) ==
             "#{@peer_dir}/secondary_stream_schema_evol~d33017db4b61.sock"
  end

  test "an id within the ceiling stays readable" do
    # The longest id `Flezha.Identificator.unique_id/1` mints in the default
    # namespace: four numbers of four digits each.
    id = "default_1000100010001000"

    assert Support.stream_socket_path(id, :primary) ==
             "#{@peer_dir}/primary_stream_#{id}.sock"
  end

  test "the controller and the runner's own side compose one name" do
    # `Flezha.SDK.Transport.connect_sockets/5` dials exactly this expression,
    # with the directory its own ladder walk reached.
    for type <- ["primary", "secondary"] do
      assert Support.stream_socket_path(@long_id, type) ==
               elem(Native.stream_socket_path(Support.socket_dir(), @long_id, type), 1)
    end
  end

  test "a socket directory that leaves no room for a name is refused by name and length" do
    dir = "/tmp/#{String.duplicate("d", 80)}"
    on_exit(TestHelpers.override_env(:socket_files_path, dir))

    error =
      assert_raise(RuntimeError, fn ->
        Support.stream_socket_path("default_1_2_3_4", :secondary)
      end)

    assert error.message =~ "secondary_stream_default_1_2_3_4.sock"
    assert error.message =~ "123 bytes"
    assert error.message =~ "107"
  end
end
