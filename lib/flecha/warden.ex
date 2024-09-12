defmodule Flecha.Warden do
  use GenServer

  @table_name :flecha_warden

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def get(key) do
    case :ets.lookup(@table_name, key) do
      [{_key, value} | _] -> value
      _ -> nil
    end
  end

  def put(key, value) do
    GenServer.call(__MODULE__, {:write, key, value})
  end

  def update(key, func) do
    GenServer.call(__MODULE__, {:update, key, func})
  end

  @impl true
  def init([]) do
    table = :ets.new(@table_name, [:set, :protected, :named_table])
    :ets.insert(table, {:engines, %{}})

    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:write, key, value}, _from, state) do
    :ets.insert(state.table, {key, value})

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:update, key, func}, _from, state) do
    existing_val = get(key)

    :ets.insert(state.table, {key, func.(existing_val)})

    {:reply, :ok, state}
  end
end
