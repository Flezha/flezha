defmodule Flezha.Boot.Loader do
  @moduledoc """
  The controller's boot sequence, in one process: pre-hooks, the prerequisites
  each enabled transport needs (the UDS socket directory, the QUIC certificates),
  post-hooks, then the boot report: where the durable state lives, whether that
  directory is empty, and how a runner attaches.

  Readiness is not answered here. `Flezha.Boot.Readiness` folds it from a
  precondition table evaluated on every read, so a probe is never queued behind
  this process's mailbox and a hub that recovers late needs no restart. What
  the loader owns is the *sequence*, and `await_sequence/0` is the one call
  that says it has run.
  """

  require Logger

  use GenServer

  alias Flezha.Config
  alias Flezha.Plugin.Provider.File.Storage
  alias Flezha.Socket.Support
  alias Flezha.State.Paths
  alias Flezha.Transport.CertGenerator

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    run_pre_hooks()

    transports = Config.get(:transports)

    if :uds in transports do
      Support.prepare_socket_dir()
    end

    if :quic in transports do
      CertGenerator.ensure_certs()
    end

    {:ok, %{}, {:continue, :boot}}
  end

  @doc """
  Block until the boot continuation has run. The call queues behind that
  continuation in this process's mailbox, and that ordering is the whole
  guarantee: it is the only fact that says the sequence finished, and the
  30 s allowance is what turns a wedged hook into a named exit rather than a
  hang.
  """
  @spec await_sequence() :: :ok
  def await_sequence(), do: GenServer.call(__MODULE__, :await_sequence, 30_000)

  @impl true
  def handle_continue(:boot, state) do
    run_post_hooks()
    Enum.each(boot_report(), &Logger.info/1)

    {:noreply, state}
  end

  @impl true
  def handle_call(:await_sequence, _from, state) do
    {:reply, :ok, state}
  end

  @doc """
  What this boot chose that nothing outside the node can see: the state root it
  opened, whether that directory holds anything, the folder file plugins may use,
  and how a runner attaches.

  Every line on every boot, not only the boot that establishes it. The user
  who ran the first boot already knows; the population these lines are for is
  whoever inherits the node afterwards, and a once-only line is invisible to
  them. `flz controller status` answers the same questions later, so neither
  depends on having watched a boot.
  """
  @spec boot_report() :: [String.t()]
  def boot_report() do
    Enum.reject(
      [state_root_line(), cold_start_line(), file_root_line(), attach_line()],
      &is_nil/1
    )
  end

  defp state_root_line() do
    "[flezha] durable state under #{Paths.state_root()} " <>
      "(set with #{Config.env_name(:state_root)})"
  end

  defp file_root_line() do
    "[flezha] file plugins read and write under #{Storage.Config.shown_root()} " <>
      "(set with #{Config.env_name(:file_plugin_root)})"
  end

  # A relocated working directory under the default relative root opens an
  # empty tree, which is correct behaviour that reads as data loss. The data is
  # in the directory the previous run was started from, and this line plus the
  # one above it is what says so.
  defp cold_start_line() do
    if Paths.relay_classes() == [] do
      "[flezha] that directory holds no durable state: this controller is starting cold"
    end
  end

  @doc """
  How a runner reaches this controller: the absolute socket directory and the
  assignment that attaches to it, plus the QUIC listen address when QUIC is
  enabled. Logged once the sequence completes so a user never has to
  find the documentation. `nil` when this node listens on no runner transport.
  """
  @spec attach_line() :: String.t() | nil
  def attach_line() do
    transports = Config.get(:transports)

    parts =
      Enum.concat([
        if(:uds in transports,
          do: [
            "runners attach with #{Config.env_name(:socket_files_path)}=#{Support.socket_dir()}"
          ],
          else: []
        ),
        if(:quic in transports,
          do: ["QUIC listening on #{Config.get(:quic_listen_address)}"],
          else: []
        )
      ])

    if parts != [], do: "[flezha] " <> Enum.join(parts, "; ")
  end

  @impl true
  def handle_info({:runner_connected_event, _}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.error("Unexpected message at boot validator: #{inspect(msg)} on node #{node()}")

    {:noreply, state}
  end

  defp run_pre_hooks() do
    Config.get(:pre_hooks)
    |> Enum.each(& &1.())
  end

  defp run_post_hooks() do
    Config.get(:post_hooks)
    |> Enum.each(& &1.())
  end
end
