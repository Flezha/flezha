defmodule Flezha.Boot.Readiness do
  @moduledoc """
  Is this node ready to serve? One verdict, folded from a declared table of
  preconditions.

  This is not the health verdict and cannot be derived from it. Health answers
  "should the kubelet restart me?" and stays `200` throughout a planned
  recovery, so the pod is not killed mid-replay. Readiness answers "should
  traffic be routed to me?" and must refuse for exactly that window.

  A precondition is one row: a key, the verdict it holds the node at while
  unmet, and the predicate that decides it. Adding one is adding a row.

  Two properties the rows depend on:

    * **No owner process.** The flag lives in `:persistent_term`, so the
      verdict is answerable before any Flezha process exists, while the
      supervision tree is coming up, and after a crash. A probe issued during
      the boot sequence is answered rather than queued behind the boot's own
      mailbox — which is what a readiness probe timing out looks like.
    * **Live, not latched.** Only `boot_sequence_complete` is recorded; the
      other rows are evaluated on every read, so a node whose bounded boot
      recovery wait elapsed becomes ready when the hub finally reaches
      `:normal`, with no restart.

  The verdict fails closed: a fact that has not been established, or cannot be
  read, counts as unmet, and `:ready` needs every row affirmatively met.
  """

  alias Flezha.Convergence.RecoveryGate
  alias Flezha.Service.HubManager

  @flag {__MODULE__, :boot_sequence_complete}

  @preconditions [
    %{key: :boot_sequence_complete, blocks: :booting, check: &__MODULE__.sequence_complete?/0},
    %{key: :hubs_recovered, blocks: :recovering, check: &__MODULE__.hubs_recovered?/0},
    %{key: :durable_registry_loaded, blocks: :recovering, check: &__MODULE__.registry_loaded?/0}
  ]

  @type row :: %{key: atom(), met: boolean()}

  @doc """
  The verdict and the rows that produced it. The first unmet row's `blocks`
  value is the verdict; every row met is `:ready`.
  """
  @spec verdict() :: {:booting | :recovering | :ready, [row()]}
  def verdict() do
    rows =
      Enum.map(@preconditions, fn %{key: key, blocks: blocks, check: check} ->
        %{key: key, blocks: blocks, met: met?(check)}
      end)

    verdict =
      case Enum.find(rows, &(not &1.met)) do
        nil -> :ready
        row -> row.blocks
      end

    {verdict, Enum.map(rows, &Map.take(&1, [:key, :met]))}
  end

  @doc """
  Record that the boot sequence completed. Called by `Flezha.Application`
  once `Supervisor.start_link/2` has returned *and* the boot continuation has
  finished — the only point at which both facts hold.
  """
  @spec publish_sequence_complete() :: :ok
  def publish_sequence_complete(), do: :persistent_term.put(@flag, true)

  @doc false
  @spec reset() :: :ok
  def reset() do
    _ = :persistent_term.erase(@flag)
    :ok
  end

  @doc false
  @spec sequence_complete?() :: boolean()
  def sequence_complete?(), do: :persistent_term.get(@flag, false) == true

  @doc false
  @spec hubs_recovered?() :: boolean()
  def hubs_recovered?(), do: not RecoveryGate.any_recovering?()

  @doc false
  @spec registry_loaded?() :: boolean()
  def registry_loaded?(), do: not HubManager.durable_registry_pending?()

  # Anything but an affirmative `true` — a raise, an exit from a manager that
  # is not up, a nil — is unmet. There is no path on which missing information
  # produces a ready answer.
  defp met?(check) do
    check.() == true
  rescue
    _ -> false
  catch
    _, _ -> false
  end
end
