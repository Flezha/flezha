defmodule Flezha.State.Store do
  @moduledoc """
  What a durable control-plane store is, and how it opens and closes.

  Nine modules keep a DETS file: the six auth stores, the runner drop marks,
  the schedule registry and the plugin catalog. Each one declared the same
  three facts — which subtree it lives in, what its file is called, and what
  its table is named — and then repeated the same `init/1` and the same
  `terminate/2` around them.
  Opening was shared through `Flezha.State.DetsStore`; closing was not, so a
  store's shutdown was written once per owner and its contract nowhere.

  A store declares the three facts and gets the rest:

      use GenServer
      use Flezha.State.Store

      @impl Flezha.State.Store
      def subtree(), do: :auth
      @impl Flezha.State.Store
      def filename(), do: "tokens.dets"
      @impl Flezha.State.Store
      def table(), do: :flz_authz_tokens

  `use Flezha.State.Store` defines `path/0`, `init/1` and `terminate/2`, so no
  store writes a GenServer body for its table at all.

  The optional callbacks are what a store may declare on top: `cache/0` an ETS
  read cache warmed from disk on open, `dir/0` a directory of its own where the
  subtree's root is not it, `open_opts/0` the auto-save interval and the clock
  that names a rotated file, and `after_open/1` work owed once the table is
  open — seeding a row, announcing what was recovered.

  A sweep is not part of this contract. `Flezha.Process.Timer` is the one tick
  seam in this tree, and a store that sweeps arms it directly.
  """

  alias Flezha.Layout.Table, as: Layout
  alias Flezha.DetsStore

  @doc "The layout subtree the store's file lives in."
  @callback subtree() :: Layout.subtree()

  @doc "The store's file name inside its subtree."
  @callback filename() :: String.t()

  @doc "The DETS table name, which is also the registered table reference."
  @callback table() :: atom()

  @doc """
  The named ETS read cache, warmed from disk on open. A store without one
  reads DETS directly.
  """
  @callback cache() :: atom()

  @doc """
  The directory holding the store's file, when it is not the subtree's root.
  """
  @callback dir() :: String.t()

  @doc """
  How this store's file is opened, where it differs from the default
  `[auto_save: 0, corrupt_suffix: :second]` — see `Flezha.State.DetsStore.open/3`.
  """
  @callback open_opts() :: keyword()

  @doc """
  Work the store owes once its table is open and warm — seeding a row that
  must exist, announcing what was recovered. Runs inside `init/1`, so a
  refusal here refuses the boot.
  """
  @callback after_open(state :: map()) :: :ok | {:error, term()}

  @optional_callbacks cache: 0, dir: 0, open_opts: 0, after_open: 1

  @doc "Absolute path of `module`'s backing file."
  @spec path(module()) :: String.t()
  def path(module), do: Path.join(dir(module), module.filename())

  @doc """
  Claim the directory and open the table, rotating a corrupt file first.

  The GenServer `init/1` body every declaring store shares: returns
  `{:ok, %{path: path}}` or `{:stop, {:dets_open_failed, path, reason}}`.
  """
  @spec open(module()) :: {:ok, %{path: String.t()}} | {:stop, term()}
  def open(module) do
    file_path = path(module)
    :ok = Layout.claim(module.subtree(), dir(module))
    opts = optional(module, :open_opts, [])

    opened =
      case optional(module, :cache, nil) do
        nil -> DetsStore.init_table(module.table(), file_path, opts)
        cache -> DetsStore.init_cached(module.table(), cache, file_path, opts)
      end

    with {:ok, state} <- opened,
         :ok <- after_open(module, state) do
      {:ok, state}
    else
      {:stop, _reason} = stop -> stop
      {:error, reason} -> {:stop, {:store_open_failed, file_path, reason}}
    end
  end

  @doc "Close `module`'s table. The GenServer `terminate/2` body every store shares."
  @spec close(module()) :: :ok
  def close(module) do
    _ = :dets.close(module.table())
    :ok
  end

  @doc """
  Every module in this application that declares this behaviour, sorted.

  Read by the conformance test, so an implementer cannot be added without being
  proved, and by the durability tier, so a new store ships without being listed
  a second time. Which modules declare a compile-time behaviour cannot change
  while the node runs, so the scan — half a second, because it loads every
  module — is done once and held.
  """
  @spec implementers() :: [module()]
  def implementers() do
    case :persistent_term.get(__MODULE__, nil) do
      nil ->
        found = scan_implementers()
        :persistent_term.put(__MODULE__, found)
        found

      found ->
        found
    end
  end

  @doc """
  Every implementer whose files live in `subtree`, by file name.

  What a durability-tier artifact of that subtree carries: the names are each
  store's own `filename/0`, so a store added to a subtree ships with it.
  """
  @spec filenames_in(Layout.subtree()) :: [String.t()]
  def filenames_in(subtree) do
    implementers()
    |> Enum.filter(&(&1.subtree() == subtree))
    |> Enum.map(& &1.filename())
    |> Enum.sort()
  end

  defmacro __using__(_opts) do
    quote do
      @behaviour Flezha.State.Store

      @doc "Absolute path of the backing DETS file."
      @spec path() :: String.t()
      def path(), do: Flezha.State.Store.path(__MODULE__)

      @impl GenServer
      def init(_opts), do: Flezha.State.Store.open(__MODULE__)

      @impl GenServer
      def terminate(_reason, _state), do: Flezha.State.Store.close(__MODULE__)

      defoverridable path: 0, terminate: 2
    end
  end

  # The directory is the subtree's root unless the store declares one, and the
  # access is the subtree row's either way: `:auth` is owner-only because the
  # files there hold secrets, and a store with a directory of its own does not
  # get a weaker claim by having one.
  defp dir(module), do: optional(module, :dir, nil) || Layout.dir(module.subtree())

  defp scan_implementers() do
    {:ok, modules} = :application.get_key(:flezha, :modules)

    modules
    |> Enum.filter(fn module ->
      # `module_info(:attributes)` repeats the `:behaviour` key once per
      # declaration, so a keyword lookup answers only the first — for a store
      # that is `GenServer`, and every implementer would be invisible.
      Code.ensure_loaded?(module) and
        module.module_info(:attributes)
        |> Keyword.get_values(:behaviour)
        |> List.flatten()
        |> Enum.member?(__MODULE__)
    end)
    |> Enum.sort()
  end

  defp after_open(module, state) do
    if function_exported?(module, :after_open, 1), do: module.after_open(state), else: :ok
  end

  defp optional(module, fun, default) do
    if function_exported?(module, fun, 0), do: apply(module, fun, []), else: default
  end
end
