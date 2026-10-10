defmodule Flezha.Runner.DropMarks do
  @moduledoc """
  The runner processes an operator dropped, which every controller refuses
  until a different process of the same runner connects.

  A mark is keyed `{namespace, stable_id}` and names the SDK `incarnation` it
  refuses, which is minted once per runner process. The OS pid is known only
  on a socket and is never trusted, so it rides along for display only.

  Marks are written through `Flezha.AuthZ.Replica`, with the stamps and the
  cluster-wide publication revoked tokens use, so a mark survives a restart
  and reaches a controller that joins later. Every controller that holds or
  adopts a mark closes the matching session
  (`Flezha.Runner.FleetManager.drop_matching/3`), and its connect path refuses
  that process again.

  Placing and lifting a mark emit `[:flezha, :runner, :dropped | :readmitted]`
  telemetry with the mark's fields, once, on the controller that acted.
  """

  use GenServer
  use Flezha.State.Store

  @behaviour Flezha.AuthZ.Replica

  alias Flezha.AuthZ.Replica
  alias Flezha.DetsStore
  alias Flezha.Runner.FleetManager

  @table :flz_runner_drop_marks
  @filename "runner_drops.dets"

  @type mark :: %{
          required(:namespace) => String.t(),
          required(:stable_id) => String.t(),
          required(:incarnation) => String.t(),
          required(:pid) => non_neg_integer() | nil,
          required(:node) => node(),
          required(:dropped_at) => integer(),
          required(:dropped_by) => String.t() | nil
        }

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Place `mark`, publish it to every connected controller, and close the
  session it names on this one.
  """
  @spec put(mark()) :: {:ok, mark()}
  def put(%{namespace: namespace, stable_id: stable_id, incarnation: incarnation} = mark) do
    key = {namespace, stable_id}
    :ok = __MODULE__ |> GenServer.call({:put, key, mark}) |> Replica.published(__MODULE__, key)
    :telemetry.execute([:flezha, :runner, :dropped], %{count: 1}, signalled(mark))
    _closed = FleetManager.drop_matching(namespace, stable_id, incarnation)
    {:ok, mark}
  end

  @doc "Lift `namespace`'s mark on `stable_id` on every controller, answering it."
  @spec clear(String.t(), String.t()) ::
          {:ok, mark()} | {:error, {:runner_not_found, String.t(), String.t()}}
  def clear(namespace, stable_id) do
    key = {namespace, stable_id}

    with {:ok, mark} <-
           __MODULE__ |> GenServer.call({:clear, key}) |> Replica.published(__MODULE__, key) do
      :telemetry.execute([:flezha, :runner, :readmitted], %{count: 1}, signalled(mark))
      {:ok, mark}
    end
  end

  @doc """
  Lift `namespace`'s mark on `stable_id` only while it still refuses
  `incarnation`, once a different process of that runner has been admitted.

  Compared and lifted in one step, so a mark placed since on the newer process
  — by an operator, or arriving late from another controller — is kept.
  """
  @spec lift_superseded(String.t(), String.t(), String.t()) :: :ok
  def lift_superseded(namespace, stable_id, incarnation) do
    key = {namespace, stable_id}

    case GenServer.call(__MODULE__, {:clear_if, key, incarnation}) do
      {:ok, mark} ->
        :ok = Replica.published(:ok, __MODULE__, key)
        :telemetry.execute([:flezha, :runner, :readmitted], %{count: 1}, signalled(mark))

      :kept ->
        :ok
    end

    :ok
  end

  @doc "`namespace`'s standing marks, oldest first."
  @spec list(String.t()) :: [mark()]
  def list(namespace) do
    for({{^namespace, _stable_id}, mark} <- DetsStore.all(@table), do: mark)
    |> Enum.sort_by(& &1.dropped_at)
  rescue
    _ -> []
  end

  @doc "`namespace`'s mark on `stable_id`, or `nil`."
  @spec standing(String.t(), String.t()) :: mark() | nil
  def standing(namespace, stable_id) do
    case DetsStore.fetch(@table, {namespace, stable_id}) do
      {:ok, mark} -> mark
      :error -> nil
    end
  end

  @doc "Whether `namespace`'s mark on `stable_id` refuses the process `incarnation` names."
  @spec match?(String.t(), String.t(), String.t()) :: boolean()
  def match?(namespace, stable_id, incarnation),
    do: Kernel.match?(%{incarnation: ^incarnation}, standing(namespace, stable_id))

  ## ----- store declaration -----

  @impl Flezha.State.Store
  def subtree(), do: :auth

  @impl Flezha.State.Store
  def filename(), do: @filename

  @impl Flezha.State.Store
  def table(), do: @table

  ## ----- replication -----

  @doc "A mark placed on another controller closes the matching session here too."
  @impl Flezha.AuthZ.Replica
  @spec adopted(term(), mark() | :deleted, term()) :: :ok
  def adopted(_key, :deleted, _previous), do: :ok

  def adopted({namespace, stable_id}, %{incarnation: incarnation}, _previous) do
    _closed = FleetManager.drop_matching(namespace, stable_id, incarnation)
    :ok
  end

  ## ----- GenServer -----

  @impl true
  def handle_call({:put, key, mark}, _from, state) do
    :ok = Replica.put(__MODULE__, key, mark)
    {:reply, :ok, state}
  end

  def handle_call({:clear, {namespace, stable_id} = key}, _from, state) do
    reply =
      case DetsStore.fetch(@table, key) do
        {:ok, mark} ->
          :ok = Replica.delete(__MODULE__, key)
          {:ok, mark}

        :error ->
          {:error, {:runner_not_found, namespace, stable_id}}
      end

    {:reply, reply, state}
  end

  def handle_call({:clear_if, key, incarnation}, _from, state) do
    reply =
      case DetsStore.fetch(@table, key) do
        {:ok, %{incarnation: ^incarnation} = mark} ->
          :ok = Replica.delete(__MODULE__, key)
          {:ok, mark}

        _newer_or_gone ->
          :kept
      end

    {:reply, reply, state}
  end

  def handle_call({:replica_accept, rows}, _from, state),
    do: {:reply, Replica.merge(__MODULE__, rows), state}

  defp signalled(mark), do: Map.take(mark, [:namespace, :stable_id, :incarnation, :pid, :node])
end
