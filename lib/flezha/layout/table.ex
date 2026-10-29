defmodule Flezha.Layout.Table do
  @moduledoc """
  Where this product puts its files: one row per subtree it owns.

  A row names the subtree, the `Flezha.Config.Keys` knob that may move it,
  whether a directory claim is owed before the first write, and how
  `flz reset` treats it. Nothing here restates a default — the root of every
  subtree is a `:path` row of the knob table, and this table declares only
  what that table does not: the subtree's identity, its claim and its reset
  mode.

  `dir/1` resolves a root, `file/2` names a file inside one, and `claim/1` is
  the one `File.mkdir_p` a subtree gets. A caller that spells a root by hand
  is a caller the two runtimes can disagree with: `flz reset` reads the same
  rows through `crates/cli/src/reset/plan.rs`, so a subtree that is cleared and a
  subtree that is kept are decided once.

  ## The socket directory is not a subtree of the data root

  The `:sockets` row declares `dir: :ladder`. The socket directory is resolved
  by the ladder `flz_transport` owns — an explicit variable, `XDG_RUNTIME_DIR`,
  `/run/flezha`, then `storage/app/sockets` — is frequently not under the data
  root at all, and `--storage-root` must not move it. `dir/1` and `claim/1`
  therefore refuse that row and name its owner, `Flezha.Socket.Support`, which
  claims the directory with the live-peer refusal a bare `mkdir_p` does not
  carry. This table owns the names inside that directory, never the directory.
  """

  alias Flezha.Config

  @type subtree ::
          :state
          | :auth
          | :audit
          | :relay_history
          | :dlq
          | :plugins
          | :process_hub
          | :schedules
          | :certs
          | :logs
          | :sockets

  @typedoc """
  How `flz reset` treats a subtree: cleared by default, cleared on an explicit
  flag, or never cleared.
  """
  @type reset_mode :: :default | :flag | :never

  @typedoc """
  How the directory comes into existence: a plain `mkdir_p`, a `mkdir_p`
  narrowed to the owner (0700, for a subtree holding secrets), or `:ladder`,
  meaning the ladder's owner claims it and this table does not.
  """
  @type claim_mode :: :plain | :owner | :ladder

  # {subtree, knob, claim, reset}. `knob` is a `Flezha.Config.Keys` row name,
  # or `{:dirname, name}` where the knob names a file inside the subtree
  # rather than the subtree itself.
  #
  # Adding a subtree is adding a row here and the matching row in
  # `crates/cli/src/reset/plan.rs`; `test/flezha/layout/table_test.exs` asserts no
  # module outside this one spells a root, so a private root cannot be added
  # without the gate saying so.
  @subtrees [
    {:state, :state_root, :plain, :default},
    {:process_hub, :process_hub_registry_root, :plain, :default},
    {:certs, {:dirname, :ca_cert_path}, :plain, :flag},
    {:dlq, :dlq_dir, :plain, :flag},
    {:schedules, :schedule_dir, :plain, :flag},
    {:auth, :authz_auth_dir, :owner, :flag},
    {:audit, :audit_log_dir, :plain, :flag},
    {:relay_history, :relay_history_dir, :plain, :default},
    {:plugins, :plugin_root, :plain, :flag},
    {:sockets, :socket_files_path, :ladder, :flag},
    {:logs, {:dirname, :log_path}, :plain, :never}
  ]

  @names Enum.map(@subtrees, &elem(&1, 0))
  @by_name Map.new(@subtrees, fn {name, knob, claim, reset} ->
             {name, %{name: name, knob: knob, claim: claim, reset: reset}}
           end)

  @doc "Every declared subtree, in reset order: what is cleared first comes first."
  @spec subtrees() :: [subtree()]
  def subtrees(), do: @names

  @doc "The row for `subtree`. Raises for a name no row declares."
  @spec row(subtree()) :: map()
  def row(subtree) when subtree in @names, do: Map.fetch!(@by_name, subtree)

  @doc """
  The absolute-or-configured root of `subtree`.

  Raises for `:sockets`: that directory is the ladder's, and asking this table
  for it would be a second answer to drift from.
  """
  @spec dir(subtree()) :: String.t()
  def dir(:sockets), do: raise(ArgumentError, ladder_refusal("dir/1"))

  def dir(subtree) when subtree in @names do
    case row(subtree).knob do
      {:dirname, knob} -> Path.dirname(Config.get(knob))
      knob -> Config.get(knob)
    end
  end

  @doc """
  The path of `name` inside `subtree`, where `name` is a filename or the list
  of segments below the root (a hub's `registry.dets` is two).
  """
  @spec file(subtree(), String.t() | [String.t()]) :: String.t()
  def file(subtree, name) when subtree in @names,
    do: Path.join([dir(subtree) | List.wrap(name)])

  @doc """
  Create `subtree`'s directory, idempotently, with the access the row declares.

  Raises for `:sockets`: `Flezha.Socket.Support.prepare_socket_dir/0` claims
  that one, because establishing it refuses a live peer first and applies an
  access boundary a `mkdir_p` cannot express.
  """
  @spec claim(subtree()) :: :ok
  def claim(:sockets), do: raise(ArgumentError, ladder_refusal("claim/1"))
  def claim(subtree) when subtree in @names, do: claim(subtree, dir(subtree))

  @doc """
  Claim `path` with the access `subtree`'s row declares, for a store whose
  directory is its own rather than the subtree's root. One row still decides
  who may read it.
  """
  @spec claim(subtree(), String.t()) :: :ok
  def claim(subtree, path) when subtree in @names and is_binary(path) do
    case File.mkdir_p(path) do
      :ok ->
        if row(subtree).claim == :owner, do: _ = File.chmod(path, 0o700)
        :ok

      {:error, reason} ->
        raise File.Error,
          reason: reason,
          action: "claim the #{subtree} directory",
          path: path
    end
  end

  @doc """
  A hub's durable registry file name, inside `<process_hub root>/<hub id>/`.

  The one leaf this table names, because three layers reach it and no rank
  they share holds it otherwise: `service/` writes it, `state/` ships it in a
  durability-tier artifact, and `flz reset` clears the tree it lives in.
  """
  @spec hub_registry_file() :: String.t()
  def hub_registry_file(), do: "registry.dets"

  @doc "How `flz reset` treats `subtree`."
  @spec reset_mode(subtree()) :: reset_mode()
  def reset_mode(subtree) when subtree in @names, do: row(subtree).reset

  # The refusal names the ladder, never the module that resolves it: this
  # table is rank 0 in `scripts/check-layer-direction.sh` and names nothing
  # above itself, a message string included.
  defp ladder_refusal(fun) do
    "the socket directory is resolved by the transport's ladder, not by the " <>
      "layout table: ask the controller's socket support for it rather than " <>
      "calling #{inspect(__MODULE__)}.#{fun} with :sockets"
  end
end
