defmodule Flezha.AuthZ.Replica do
  @moduledoc """
  What makes the authorization stores one state per cluster.

  A store writes a row through `put/3` or `delete/2`, inside its own GenServer.
  The row's stamp — `{system_time_us, node}` of its last write — is kept here,
  in `replica.dets` under `{store, key}`, beside the row rather than in it, so
  no row shape changes. A stamp whose row is gone is the delete's tombstone.

  The store's mutating client function then ends in `published/3`, whose
  `publish/2` hands the row to every connected controller's `accept/2`, and
  `accept/2` merges it
  through that controller's own store GenServer, so a remote row is serialized
  with the local writes. Publishing from the caller rather than from inside the
  store means two controllers writing at once never wait on each other's store.

  When two controllers connect, each merges the other's snapshot and hands over
  its own. The later stamp wins, unless the store implements `c:prefer?/2`; a
  row with no stamp, written before this module, is older than any stamped one,
  and a row the receiving side has never seen is adopted whatever its stamp. A
  store implementing `c:adopted/3` is told each row it adopted, outside its
  GenServer.
  """

  use GenServer
  use Flezha.State.Store

  require Logger

  alias Flezha.AuthZ.{AuthState, NamespaceStore, RoleStore, TokenStore}
  alias Flezha.Config.Durations
  alias Flezha.DetsStore

  @table :flz_authz_replica
  @filename "replica.dets"
  @stores [AuthState, RoleStore, NamespaceStore, TokenStore]

  @type stamp :: {integer(), node()} | nil
  @type row :: {term(), term() | :deleted, stamp()}

  @doc "Whether the remote `{value, stamp}` wins over the local one."
  @callback prefer?(local :: {term(), stamp()}, remote :: {term(), stamp()}) :: boolean()

  @doc "A row this controller adopted from another, with the value it replaced."
  @callback adopted(key :: term(), value :: term(), previous :: term()) :: :ok

  @optional_callbacks prefer?: 2, adopted: 3

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Write `value` under `key` in `store`'s table and stamp it. Called inside the store's GenServer."
  @spec put(module(), term(), term()) :: :ok
  def put(store, key, value), do: write(store, key, value, now())

  @doc "Delete `key` from `store`'s table and keep the tombstone. Called inside the store's GenServer."
  @spec delete(module(), term()) :: :ok
  def delete(store, key), do: write(store, key, :deleted, now())

  @doc """
  Hand `store`'s current row for `key` to every connected controller. A
  controller that does not answer is named in a warning and reconciled when it
  next connects.
  """
  @spec publish(module(), term()) :: :ok
  def publish(store, key) do
    case Node.list() do
      [] ->
        :ok

      peers ->
        {value, stamp} = local(store, key)

        results =
          :erpc.multicall(peers, __MODULE__, :accept, [store, [{key, value, stamp}]], call_ms())

        for {peer, result} <- Enum.zip(peers, results), not match?({:ok, _}, result) do
          Logger.warning(
            "[authz.replica] #{inspect(store)} write not applied on #{inspect(peer)} " <>
              "(#{inspect(result)}); it is merged when that controller reconnects"
          )
        end

        :ok
    end
  end

  @doc """
  `result` of a mutating call on `store`, after publishing `key` unless the
  call was refused. How every store's client function ends a write.
  """
  @spec published(result, module(), term()) :: result when result: term()
  def published({:error, _} = refused, _store, _key), do: refused

  def published(result, store, key) do
    :ok = publish(store, key)
    result
  end

  @doc """
  Merge `rows` into this controller's `store` through its GenServer, then tell
  the store what it adopted. `:not_running` on a node without the store.
  """
  @spec accept(module(), [row()]) :: :ok | :not_running
  def accept(store, rows) do
    if Process.whereis(store) do
      # Unbounded: the local store finishes these writes whatever the caller's
      # deadline, and what it adopted must reach `adopted/3` either way. A
      # remote caller is bounded by its own `:erpc` timeout.
      adopted = GenServer.call(store, {:replica_accept, rows}, :infinity)

      if function_exported?(store, :adopted, 3),
        do:
          Enum.each(adopted, fn {key, value, previous} -> store.adopted(key, value, previous) end)

      :ok
    else
      :not_running
    end
  end

  @doc """
  The rows of `rows` that win over this controller's, written with their
  stamps, as `{key, value, previous}`. Called inside the store's GenServer.
  """
  @spec merge(module(), [row()]) :: [{term(), term(), term()}]
  def merge(store, rows) do
    Enum.flat_map(rows, fn {key, value, stamp} ->
      {previous, local_stamp} = local = local(store, key)

      cond do
        value != previous and remote_wins?(store, local, {value, stamp}) ->
          :ok = write(store, key, value, stamp)
          [{key, value, previous}]

        # The same value with a newer stamp: a delete of a row never held here
        # must still outrank an older copy that arrives later.
        value == previous and stamp > local_stamp ->
          :ok = write_stamp(store, key, stamp)
          []

        true ->
          []
      end
    end)
  end

  @doc """
  Merge with every controller already connected, and return once done.

  `nodeup` covers a controller that connects later. One connected before the
  subtree was up — a restart under its supervisor, or a node that joined the
  cluster before its application started — is merged here, which
  `Flezha.AuthZ.Bootstrap.OnBoot` does before deciding whether a bootstrap
  token is owed.
  """
  @spec sync() :: :ok
  def sync(), do: GenServer.call(__MODULE__, :sync, :infinity)

  @doc "Every row of `store`, live and deleted, with its stamp."
  @spec snapshot(module()) :: [row()]
  def snapshot(store) do
    live = DetsStore.all(store.table())
    live_keys = MapSet.new(live, &elem(&1, 0))

    tombstones =
      for {{^store, key}, stamp} <- DetsStore.all(@table),
          not MapSet.member?(live_keys, key),
          do: {key, :deleted, stamp}

    Enum.map(live, fn {key, value} -> {key, value, stamp(store, key)} end) ++ tombstones
  end

  ## ----- store declaration -----

  @impl Flezha.State.Store
  def subtree(), do: :auth

  @impl Flezha.State.Store
  def filename(), do: @filename

  @impl Flezha.State.Store
  def table(), do: @table

  @impl Flezha.State.Store
  def after_open(_state) do
    _ = :net_kernel.monitor_nodes(true)
    :ok
  end

  ## ----- GenServer -----

  @impl true
  def handle_info({:nodeup, peer}, state) do
    Enum.each(@stores, &exchange(&1, peer))
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # Every exchange is bounded by its own calls, so the reply always comes.
  @impl true
  def handle_call(:sync, _from, state) do
    for peer <- Node.list(), store <- @stores, do: exchange(store, peer)
    {:reply, :ok, state}
  end

  ## ----- private -----

  # Both directions in one pass, so either side's `nodeup` alone converges the
  # pair. A peer whose subtree is not up yet answers nothing here and makes the
  # same exchange from its own side, through `sync/0`, once it is.
  defp exchange(store, peer) do
    with {:ok, rows} <- remote(peer, :snapshot, [store]),
         :ok <- accept(store, rows) do
      _ = remote(peer, :accept, [store, snapshot(store)])
      :ok
    end
  catch
    :exit, reason ->
      Logger.debug(
        "[authz.replica] merge of #{inspect(store)} with #{inspect(peer)}: #{inspect(reason)}"
      )
  end

  defp remote(peer, fun, args) do
    {:ok, :erpc.call(peer, __MODULE__, fun, args, call_ms())}
  rescue
    e -> {:error, e}
  end

  defp remote_wins?(store, local, remote) do
    if function_exported?(store, :prefer?, 2),
      do: store.prefer?(local, remote),
      else: later?(local, remote)
  end

  @doc """
  The default rule: a row never seen here is adopted, otherwise the later
  stamp wins. Exported so a store's own `prefer?/2` can fall back to it.
  """
  @spec later?({term(), stamp()}, {term(), stamp()}) :: boolean()
  def later?({:deleted, nil}, _remote), do: true
  def later?({_value, local_stamp}, {_remote_value, remote_stamp}), do: remote_stamp > local_stamp

  defp local(store, key) do
    value =
      case DetsStore.fetch(store.table(), key) do
        {:ok, value} -> value
        :error -> :deleted
      end

    {value, stamp(store, key)}
  end

  defp stamp(store, key) do
    case DetsStore.fetch(@table, {store, key}) do
      {:ok, stamp} -> stamp
      :error -> nil
    end
  end

  defp write(store, key, value, stamp) do
    :ok = write_row(store, key, value, cache(store))
    write_stamp(store, key, stamp)
  end

  # A store started on its own, without this process (an isolated store test),
  # keeps no stamps.
  defp write_stamp(store, key, stamp) do
    if :dets.info(@table, :size) != :undefined and stamp != nil,
      do: DetsStore.put(@table, {store, key}, stamp),
      else: :ok
  end

  defp write_row(store, key, :deleted, nil), do: DetsStore.delete(store.table(), key)
  defp write_row(store, key, :deleted, cache), do: DetsStore.delete(store.table(), cache, key)
  defp write_row(store, key, value, nil), do: DetsStore.put(store.table(), key, value)
  defp write_row(store, key, value, cache), do: DetsStore.put(store.table(), cache, key, value)

  defp cache(store), do: if(function_exported?(store, :cache, 0), do: store.cache())

  defp now(), do: {System.os_time(:microsecond), node()}

  defp call_ms(), do: Durations.get(:authz_replica_call_ms)
end
