defmodule Flezha.Boot.Loader do
  @moduledoc """
  The controller's boot sequence, in one process: pre-hooks, the prerequisites
  each enabled transport needs (the UDS socket directory, the QUIC certificates),
  post-hooks, then the warnings about an unprotected distribution link and the
  boot report: where the durable state lives, whether that directory is empty,
  and how a runner attaches.

  Readiness is not answered here. `Flezha.Boot.Readiness` folds it from a
  precondition table evaluated on every read, so a probe is never queued behind
  this process's mailbox and a hub that recovers late needs no restart. What
  the loader owns is the *sequence*, and `await_sequence/0` is the one call
  that says it has run.
  """

  require Logger

  use GenServer

  alias Flezha.Config
  alias Flezha.Config.Durations
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

    # Every controller listens on its UDS control socket.
    Support.prepare_socket_dir()

    if :quic in Flezha.Transport.Server.transports() do
      CertGenerator.ensure_certs()
    end

    {:ok, %{}, {:continue, :boot}}
  end

  @doc """
  Block until the boot continuation has run. The call queues behind that
  continuation in this process's mailbox, and that ordering is the whole
  guarantee: it is the only fact that says the sequence finished, and the
  allowance (`:boot_sequence_await_ms`) is what turns a wedged hook into a
  named exit rather than a hang.
  """
  @spec await_sequence() :: :ok
  def await_sequence(),
    do: GenServer.call(__MODULE__, :await_sequence, Durations.get(:boot_sequence_await_ms))

  @impl true
  def handle_continue(:boot, state) do
    run_post_hooks()
    _ = warn_unprotected_distribution()
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

  @doc false
  # A connected node is trusted with everything, so a distribution link another
  # machine can reach in plain TCP, or over TLS that does not check its peer,
  # is warned on every boot for as long as it stays that way. Nothing is
  # refused. Answers the warnings it logged.
  @spec warn_unprotected_distribution(map()) :: [String.t()]
  def warn_unprotected_distribution(facts \\ distribution_facts()) do
    warnings = distribution_warnings(facts)
    Enum.each(warnings, &Logger.warning("[flezha] " <> &1))
    warnings
  end

  defp distribution_facts() do
    %{
      node: node(),
      tls?:
        match?(
          {:ok, [[proto | _] | _]} when proto in [~c"inet_tls", ~c"inet6_tls"],
          :init.get_argument(:proto_dist)
        ),
      optfile:
        case :init.get_argument(:ssl_dist_optfile) do
          {:ok, [[path | _] | _]} -> List.to_string(path)
          _ -> nil
        end,
      runner_anchors: [Config.get(:quic_client_ca_path), Config.get(:ca_cert_path)]
    }
  end

  defp distribution_warnings(%{node: :nonode@nohost}), do: []

  defp distribution_warnings(%{tls?: false, node: node}) do
    if loopback?(node),
      do: [],
      else: [
        "distribution on #{node} is plain TCP: traffic between controllers is neither " <>
          "encrypted nor authenticated beyond the cookie. Set FLEZHA_DIST_TLS_OPTFILE to " <>
          "run it over mutual TLS (deploy/dist-tls/)."
      ]
  end

  defp distribution_warnings(%{optfile: path} = facts) do
    case :file.consult(path) do
      {:ok, [sections]} when is_list(sections) -> tls_warnings(path, sections, facts)
      _unreadable -> ["the distribution TLS options file #{path} cannot be read"]
    end
  end

  defp tls_warnings(path, sections, %{runner_anchors: anchors}) do
    server = section(sections, :server)
    client = section(sections, :client)

    [
      if(server[:verify] != :verify_peer,
        do:
          "#{path} does not set {verify, verify_peer} for the server: peers are admitted " <>
            "without a certificate check"
      ),
      if(server[:versions] == nil and client[:versions] == nil,
        do: "#{path} sets no versions: only TLS 1.2 is offered; set {versions, ['tlsv1.3']}"
      ),
      if(Enum.any?([server[:cacertfile], client[:cacertfile]], &same_file?(&1, anchors)),
        do:
          "#{path} trusts the runner authority (FLEZHA_QUIC_CLIENT_CA or FLEZHA_CA_CERT_PATH): " <>
            "a runner's certificate passes the distribution TLS check"
      )
    ]
    |> Enum.reject(&is_nil/1)
  end

  defp section(sections, side) do
    case List.keyfind(sections, side, 0) do
      {^side, opts} when is_list(opts) -> opts
      _ -> []
    end
  end

  defp same_file?(nil, _anchors), do: false

  defp same_file?(file, anchors) do
    file = to_string(file)
    Enum.any?(anchors, &(is_binary(&1) and identity(&1) == identity(file)))
  end

  # A file by its inode, so two spellings of one path agree; by its path when it
  # cannot be read.
  defp identity(path) do
    case File.stat(path) do
      {:ok, %File.Stat{major_device: device, inode: inode}} -> {device, inode}
      {:error, _} -> Path.expand(path)
    end
  end

  defp loopback?(node) do
    host = node |> Atom.to_string() |> String.split("@") |> List.last()

    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, {127, _, _, _}} -> true
      {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} -> true
      _ -> host == "localhost"
    end
  end

  @doc """
  How a runner reaches this controller: the absolute socket directory and the
  assignment that attaches to it, plus the QUIC listen address when QUIC is
  enabled. Logged once the sequence completes so a user never has to
  find the documentation.
  """
  @spec attach_line() :: String.t()
  def attach_line() do
    socket = "runners attach with #{Config.env_name(:socket_files_path)}=#{Support.socket_dir()}"

    quic =
      if :quic in Flezha.Transport.Server.transports(),
        do: ["QUIC listening on #{Config.get(:quic_listen_address)}"],
        else: []

    "[flezha] " <> Enum.join([socket | quic], "; ")
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
