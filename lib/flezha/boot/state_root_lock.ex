defmodule Flezha.Boot.StateRootLock do
  @moduledoc """
  This controller's exclusive claim on its state root.

  Several controllers may share a host, each owning its own directory tree
  (`docs/topics/deploying/deploying_a_controller.md`). Two of them sharing one state root are two
  unsynchronised writers over the same cell WAL trees, and until this claim
  nothing but configuration kept them apart — a shape reached by copying a unit
  file and forgetting one variable.

  The claim is an OS advisory lock on a file in the state root, taken before
  any state path is opened and held for the process lifetime. The kernel
  releases it when the holder exits, so a controller that died hard leaves
  nothing to clear, and the refusal names the process still holding it.
  """

  alias Flezha.State.Paths
  alias Flezha.Transport.Native

  @var Flezha.Config.env_name(:state_root)

  @doc """
  Claim `root` for this controller, or refuse naming its holder. Returns the
  lock file taken; claiming a root this process already holds is a no-op, which
  is what a controller restarting inside one OS process does.

  `root` is already absolute: `Flezha.State.Paths.state_root/0` resolves it,
  and it is the same string the WAL, snapshot and outbox paths are built from,
  so the claim, the refusal and `held/0` name one directory.
  """
  @spec claim!(binary()) :: String.t()
  def claim!(root \\ Paths.state_root()) do
    case Native.lock_state_root(root) do
      {:ok, lock_file} ->
        lock_file

      {:error, holder} ->
        raise "state root #{root} is already claimed (#{holder}): two controllers " <>
                "writing one cell WAL tree corrupt it, so this one will not start. " <>
                "Set #{@var} to a state root of this controller's own — one directory " <>
                "tree per controller — or stop the holder."
    end
  end

  @doc "The lock file this controller holds; `nil` before it has claimed one."
  @spec held() :: String.t() | nil
  def held() do
    case Native.held_state_root() do
      "" -> nil
      lock_file -> lock_file
    end
  end
end
