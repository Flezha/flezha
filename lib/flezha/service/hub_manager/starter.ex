defmodule Flezha.Service.HubManager.Starter do
  @moduledoc """
  The owner that gives every declared namespace a hub at boot.

  Without it a hub exists only where a runner has connected, so a restarted
  controller replays nothing until one does: its durable registry sits on disk
  with no hub to read it, and every surface that answers a question about hubs
  answers it over an empty set. The declared set is
  `HubManager.declared_namespaces/0`, and every hub still starts through
  `HubManager.start_hub/1`, so one created here is indistinguishable from one
  created on connect.

  Off the boot path on purpose, for the reason `Flezha.Schedule.Hub` gives:
  the cluster gate blocks for up to the formation timeout, and
  `Flezha.Application.start_controller/0` waits on the boot sequence, so a
  wait placed inside it would hold application start for that whole window.

  Never crashes. This owner sits under the application's `one_for_one`
  supervisor, where a restart loop would take the controller down; a namespace
  whose hub will not start is retried on the `:hub_retry` ladder and holds
  readiness in the meantime.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:retry]

  alias Flezha.Config.Backoff
  alias Flezha.Service.HubManager

  @pending {__MODULE__, :pending}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  The declared namespaces this node has not given a hub yet, or `:unstarted`
  before the first pass has finished. `Flezha.Boot.Readiness` reads it.

  Read from `:persistent_term` rather than from this process, for the reason
  the readiness table gives: a probe must be answerable while the tree is
  coming up, and a call here would queue behind a mailbox parked on the
  cluster gate for the whole formation window.

  What it answers is whether boot-time creation has finished, not whether
  every hub is still up. A hub this node later reclaims because it serves
  none of that namespace's relays is a deliberate teardown, and holding
  the node out of rotation for it would strand a controller that is healthy.

  A node with the auto-restore kill-switch off owes nothing: it creates no
  hub at boot and starts no owner to do it. The switch is read here so the
  readiness row reads one fact rather than two.
  """
  @spec pending() :: [binary()] | :unstarted
  def pending() do
    if HubManager.durable_starts?(), do: :persistent_term.get(@pending, :unstarted), else: []
  end

  @doc false
  @spec reset() :: :ok
  def reset() do
    _ = :persistent_term.erase(@pending)
    :ok
  end

  @impl true
  def init(_opts) do
    # A restarted owner owes its batch again, and says so until it has run it.
    reset()
    {:ok, %{}, {:continue, :bootstrap}}
  end

  @impl true
  def handle_continue(:bootstrap, state) do
    # Once for the whole batch: cluster formation is a property of the node,
    # not of a namespace.
    _ = HubManager.await_cluster_gate()
    {:noreply, start_declared(state)}
  end

  @impl true
  def handle_info(:retry, state), do: {:noreply, start_declared(state)}

  # The declared set is re-derived on every pass, so a namespace created while
  # an earlier one was still failing is picked up by the retry.
  defp start_declared(state) do
    pending =
      HubManager.declared_namespaces()
      |> Enum.reject(&HubManager.hub_alive?/1)
      |> Enum.reject(fn namespace ->
        HubManager.attempt_start(namespace, fn -> HubManager.start_hub(namespace) end) == :ok
      end)

    publish(pending)

    if pending == [] do
      state
    else
      arm_timer(state, :retry, Backoff.arm(:hub_retry, 1, :declared_hubs), :retry)
    end
  end

  # Only on a change: a `:persistent_term` write scans every process, and a
  # namespace that keeps failing would otherwise pay for one every retry.
  defp publish(pending) do
    unless pending() == pending, do: :persistent_term.put(@pending, pending)
    :ok
  end
end
