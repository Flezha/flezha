defmodule Flezha.AuthZ.AuthState do
  @moduledoc """
  The durable bootstrap claim of the authn/authz subsystem.

  Lives in `<auth_dir>/state.dets` as one row, `:enabled`, holding
  `%{enabled, principal, generation, at}`. The bootstrap claim writes it
  enabled, naming the admin; the break-glass reset writes it disabled. Each
  write is one generation above the last, which is how a cluster ranks two
  claims (`prefer?/2`). A row written before claims were ranked is a bare
  boolean and still reads as one.

  The runtime `enabled?/0` accessor is the source of truth consumed
  by `Flezha.AuthZ.enabled?/0`. Before the GenServer has been
  started (very early boot) it returns the conservatively-safe
  `false`.
  """

  use GenServer
  use Flezha.State.Store

  @behaviour Flezha.AuthZ.Replica

  alias Flezha.AuthZ.{Bootstrap, Config, Replica}
  alias Flezha.DetsStore

  @table :flz_authz_state
  @filename "state.dets"
  @key :enabled

  @type claim :: %{
          enabled: boolean(),
          principal: binary() | nil,
          generation: non_neg_integer(),
          at: integer()
        }

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "True when the durable flag is set."
  @spec enabled?() :: boolean()
  def enabled?() do
    case DetsStore.cache_lookup(@table, @key) do
      {:ok, %{enabled: enabled}} -> enabled
      {:ok, value} when is_boolean(value) -> value
      _ -> false
    end
  end

  @doc """
  Record the claim of `principal` as admin. The fsync completes, and every
  connected controller holds the claim, before the call returns.
  """
  @spec set_enabled(binary() | nil) :: :ok
  def set_enabled(principal \\ nil), do: write(true, principal)

  @doc """
  Force the flag back to `false`. Used by the break-glass rebootstrap
  recovery (`Flezha.AuthZ.Bootstrap.consume_recovery_marker/0`) and by tests.
  """
  @spec reset() :: :ok
  def reset(), do: write(false, nil)

  ## ----- replication -----

  @doc """
  A higher generation wins — a recovery claim or a reset beats the claim it
  replaced — and of two equal generations, two first claims made apart, the
  earlier one.
  """
  @impl Flezha.AuthZ.Replica
  @spec prefer?({term(), Replica.stamp()}, {term(), Replica.stamp()}) :: boolean()
  def prefer?({local, _}, {remote, _}), do: rank(remote) > rank(local)

  @doc """
  A claim made on another controller is this one's too: its own
  `bootstrap.token` is removed, and an admin it claimed that lost is
  invalidated.
  """
  @impl Flezha.AuthZ.Replica
  @spec adopted(atom(), claim(), term()) :: :ok
  def adopted(@key, %{enabled: true, principal: winner}, previous) do
    _ = File.rm(Config.bootstrap_token_path())

    case previous do
      %{principal: loser} when is_binary(loser) and loser != winner ->
        Bootstrap.invalidate_admin(loser, "claim-merge")

      _ ->
        :ok
    end
  end

  def adopted(_key, _claim, _previous), do: :ok

  defp rank(%{generation: generation, at: at}), do: {generation, -at}
  defp rank(:deleted), do: {-1, 0}
  defp rank(_legacy_boolean), do: {0, 0}

  ## ----- store declaration -----

  @impl Flezha.State.Store
  def subtree(), do: :auth

  @impl Flezha.State.Store
  def filename(), do: @filename

  @impl Flezha.State.Store
  def table(), do: @table

  # The ETS cache shares the DETS table's name (separate namespaces).
  @impl Flezha.State.Store
  def cache(), do: @table

  ## ----- GenServer -----

  @impl true
  def handle_call({:write, enabled, principal}, _from, state) do
    generation =
      case DetsStore.fetch(@table, @key) do
        {:ok, %{generation: generation}} -> generation
        _ -> 0
      end

    claim = %{
      enabled: enabled,
      principal: principal,
      generation: generation + 1,
      at: System.os_time(:microsecond)
    }

    {:reply, Replica.put(__MODULE__, @key, claim), state}
  end

  def handle_call({:replica_accept, rows}, _from, state),
    do: {:reply, Replica.merge(__MODULE__, rows), state}

  defp write(enabled, principal),
    do:
      Replica.published(
        GenServer.call(__MODULE__, {:write, enabled, principal}),
        __MODULE__,
        @key
      )
end
