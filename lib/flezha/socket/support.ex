defmodule Flezha.Socket.Support do
  @moduledoc """
  The controller's end of the UDS socket ladder: where the control and data
  sockets live, and who may reach them.

  The directory itself is resolved by `flz_transport`'s ladder through the NIF,
  the same implementation every SDK's runner resolves with, so the two sides
  cannot drift. Traversal of that directory is the UDS authentication boundary:
  owner-only by default, widened to a single named group only when
  `FLEZHA_SOCKET_DIR_GROUP` says so.
  """

  require Logger

  alias Flezha.Transport.Native

  @group_var "FLEZHA_SOCKET_DIR_GROUP"
  @dir_var "FLEZHA_SOCKET_FILES_PATH"

  @doc """
  Establish this controller's socket directory, clearing whatever a previous
  controller left in it.

  Refuses when a live peer is already listening there: the directory is one
  instance's, and clearing it would take that instance's control socket and
  every attached runner's stream socket with it. A socket nobody answers is a
  controller that died hard and is cleared as normal, so a restart needs no
  manual cleanup.
  """
  def prepare_socket_dir() do
    base_path = socket_dir()

    refuse_live_peer!(base_path)

    File.rm_rf(base_path)
    result = File.mkdir_p(base_path)
    :ok = apply_access_boundary(base_path)
    result
  end

  @doc """
  The absolute socket directory this controller established. Resolved once
  through the shared ladder and held in `:socket_files_path`; a value already
  configured there (an operator's, or a test's) is used as it stands.

  Always absolute: a relative socket path is resolved against each process's own
  working directory, so a controller and a runner started from two directories
  compute two paths and never meet.
  """
  @spec socket_dir() :: String.t()
  def socket_dir() do
    case Application.get_env(:flezha, :socket_files_path) do
      nil -> resolve_socket_dir()
      path -> path |> String.replace_trailing("/", "") |> Path.expand()
    end
  end

  @spec set_active(port() | {:"$inet", atom(), any()}) :: :ok | {:error, atom()}
  def set_active(socket) do
    :inet.setopts(socket, [{:active, true}])
  end

  def control_socket_path() do
    socket_dir() <> "/runner_control_channel.sock"
  end

  def stream_socket_path(runner_id, type) when is_atom(runner_id) do
    stream_socket_path(Atom.to_string(runner_id), type)
  end

  def stream_socket_path(runner_id, type) when is_binary(runner_id) do
    socket_dir() <> "/#{Atom.to_string(type)}_stream_#{runner_id}.sock"
  end

  # Liveness is `flz_transport`'s probe, the one every runner's diagnosis asks:
  # a second implementation here would be a second answer to drift from.
  defp refuse_live_peer!(path) do
    if Native.socket_dir_live(path) do
      raise "#{path} is another controller's socket directory: #{control_socket_path()} " <>
              "answered a connect, so a controller is live there. Set #{@dir_var} to a " <>
              "directory of this controller's own — one directory tree per controller — " <>
              "or stop the controller listening there. Nothing was removed."
    end
  end

  defp resolve_socket_dir() do
    case Native.resolve_connect("controller") do
      {:ok, dir, _quic_addresses} ->
        Application.put_env(:flezha, :socket_files_path, dir)
        dir

      {:error, reason} ->
        raise "cannot establish a socket directory: #{reason}"
    end
  end

  # Owner-only unless the operator has widened the boundary to one named group,
  # which is warned on every boot for as long as it is set.
  defp apply_access_boundary(path) do
    case System.get_env(@group_var) do
      empty when empty in [nil, ""] ->
        File.chmod(path, 0o700)

      group ->
        gid = group_gid!(group)

        Logger.warning(
          "[flezha] #{@group_var}=#{group}: the UDS access boundary is now that group, " <>
            "not this user alone — #{path} is group-readable (0750). Unset it to return " <>
            "to owner-only."
        )

        :ok = :file.change_group(String.to_charlist(path), gid)
        File.chmod(path, 0o750)
    end
  end

  # A group the controller's user does not belong to cannot own a directory the
  # controller must write, so an unusable group refuses the boot rather than
  # producing a socket nobody can reach. `id` reports names and gids in the same
  # order, so membership and the gid come from one answer.
  defp group_gid!(group) do
    names = run_id!("-nG")
    gids = run_id!("-G")

    case Enum.zip(names, gids) |> List.keyfind(group, 0) do
      {^group, gid} ->
        String.to_integer(gid)

      nil ->
        raise "#{@group_var}=#{group}: no such group, or this controller's user is not a " <>
                "member of it (member of: #{Enum.join(names, ", ")})"
    end
  end

  defp run_id!(flag) do
    case System.cmd("id", [flag], stderr_to_stdout: true) do
      {output, 0} ->
        output |> String.trim() |> String.split(" ", trim: true)

      {output, status} ->
        raise "#{@group_var} is set but `id #{flag}` failed (#{status}): #{output}"
    end
  end
end
