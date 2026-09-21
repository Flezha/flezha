defmodule Flezha.Schedule.Store do
  @moduledoc """
  Durable cron-schedule registry backed by `<schedule_dir>/schedules.dets`.

  Each row stores one schedule entry keyed by its unique name. An ETS read
  cache fronts the DETS table (the `Flezha.DetsStore` cached skeleton).
  Like every sibling control-plane registry the store is node-local: it is the
  definition of record, and `Flezha.Schedule.Hub` asserts what it holds into
  the cluster-wide `:flz_schedules` hub, which decides who runs it.
  """

  use GenServer
  use Flezha.State.Store

  alias Flezha.DetsStore
  alias Flezha.Identificator
  alias Flezha.Operative.Builder
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Schedule.Cron

  # Distinct from the `:flz_schedules` hub id, which names an ETS table.
  @table :flz_schedule_defs
  @cache :flz_schedule_defs_cache
  @filename "schedules.dets"

  # A UTC slot renders as a fixed-width suffix; the composed cron instance
  # id `<name>@<slot-iso>` must fit the operative id byte limit.

  @type entry :: %{
          required(:name) => binary(),
          required(:cron) => binary(),
          required(:timezone) => binary(),
          required(:factory_id) => binary(),
          required(:namespace) => binary(),
          required(:init_values) => map(),
          required(:text) => binary() | nil,
          required(:channel) => binary() | nil,
          required(:plugins) => binary(),
          required(:created_at) => binary()
        }

  ## ----- Public API -----

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Validate and durably add a schedule. `fields` must carry `:name`,
  `:cron`, `:timezone`, `:factory_id`, `:namespace` and `:init_values`;
  `:plugins` (the raw `--plugins` JSON array), `:text` (the first message
  every fired instance receives, stored verbatim) and `:channel` (the entry
  channel that message names, refused without `:text`) are optional.
  """
  @spec add(map()) :: {:ok, entry()} | {:error, term()}
  def add(fields) when is_map(fields) do
    with :ok <- validate_name(fields[:name]),
         {:ok, _} <- validate_cron(fields[:cron]),
         :ok <- validate_timezone(fields[:timezone]),
         :ok <- validate_factory_id(fields[:factory_id]),
         :ok <- validate_init_values(fields[:init_values]),
         :ok <- validate_first_message(fields[:text], fields[:channel]),
         {:ok, plugins} <- validate_plugins(fields[:plugins]) do
      entry = %{
        name: fields[:name],
        cron: fields[:cron],
        timezone: fields[:timezone],
        factory_id: fields[:factory_id],
        namespace: fields[:namespace],
        init_values: fields[:init_values],
        text: fields[:text],
        channel: fields[:channel],
        plugins: plugins,
        created_at: DateTime.to_iso8601(DateTime.utc_now())
      }

      GenServer.call(__MODULE__, {:add, entry})
    end
  end

  @spec remove(binary()) :: :ok | {:error, :not_found}
  def remove(name) when is_binary(name) do
    GenServer.call(__MODULE__, {:remove, name})
  end

  @spec get(binary()) :: {:ok, entry()} | :error
  def get(name) when is_binary(name), do: DetsStore.cache_lookup(@cache, name)

  @spec list() :: [entry()]
  def list(), do: DetsStore.cache_values(@cache) |> Enum.sort_by(& &1.name)

  ## ----- store declaration -----

  @impl Flezha.State.Store
  def subtree(), do: :schedules

  @impl Flezha.State.Store
  def filename(), do: @filename

  @impl Flezha.State.Store
  def table(), do: @table

  @impl Flezha.State.Store
  def cache(), do: @cache

  ## ----- GenServer -----

  @impl true
  def handle_call({:add, entry}, _from, state) do
    case DetsStore.put_new(@table, @cache, entry.name, entry) do
      :ok -> {:reply, {:ok, entry}, state}
      {:error, :already_exists} = err -> {:reply, err, state}
    end
  end

  def handle_call({:remove, name}, _from, state) do
    case DetsStore.fetch(@table, name) do
      :error ->
        {:reply, {:error, :not_found}, state}

      {:ok, _} ->
        :ok = DetsStore.delete(@table, @cache, name)
        {:reply, :ok, state}
    end
  end

  ## ----- private -----

  # The name is embedded in instance ids as `<name>@<slot>`, so it must be
  # unambiguous there (no `@`, no whitespace, filesystem-safe) and short
  # enough that every fire's composed id passes the operative id limit.
  defp validate_name(name) when is_binary(name) do
    case Identificator.validate(:schedule_name, name) do
      :ok ->
        :ok

      {:error, _refusal} ->
        {:error,
         {:invalid_name,
          "schedule name must match [A-Za-z0-9][A-Za-z0-9_-]* and be at most " <>
            "#{Identificator.limit(:schedule_name)} bytes (`<name>@<slot>` must fit the " <>
            "#{Identificator.limit(:operative)}-byte operative id limit)"}}
    end
  end

  defp validate_name(_), do: {:error, {:invalid_name, "schedule name is required"}}

  defp validate_cron(expr) when is_binary(expr) do
    case Cron.parse(expr) do
      {:ok, _} = ok -> ok
      {:error, msg} -> {:error, {:invalid_cron, msg}}
    end
  end

  defp validate_cron(_), do: {:error, {:invalid_cron, "cron expression is required"}}

  defp validate_timezone(tz) when is_binary(tz) do
    if Cron.valid_timezone?(tz) do
      :ok
    else
      {:error, {:invalid_timezone, tz}}
    end
  end

  defp validate_timezone(_), do: {:error, {:invalid_timezone, "timezone is required"}}

  defp validate_factory_id(factory_id) when is_binary(factory_id) and factory_id != "", do: :ok
  defp validate_factory_id(_), do: {:error, {:invalid_factory, "factory id is required"}}

  defp validate_init_values(values) when is_map(values), do: :ok

  defp validate_init_values(_),
    do: {:error, {:invalid_init_values, "init_values must be a JSON object"}}

  # The first message is stored as the text the operator wrote, validated as
  # the seed a firing makes of it.
  defp validate_first_message(text, channel) do
    case Builder.seed_input(text, channel) do
      {:ok, _seed} -> :ok
      {:error, reason} -> {:error, {:invalid_first_message, reason}}
    end
  end

  # Structural only: provider ids are resolved at every firing against the
  # controller that is actually running, so a decoded plugin must never be
  # frozen into a DETS row. `CliManager.schedule_add/2` does the full
  # provider-resolving check before calling `add/1`.
  defp validate_plugins(json) when json in [nil, ""], do: {:ok, "[]"}

  defp validate_plugins(json) when is_binary(json) do
    if SpecDecoder.valid_json_array?(json) do
      {:ok, json}
    else
      {:error, {:invalid_plugins, "plugins must be a JSON array of plugin specs"}}
    end
  end

  defp validate_plugins(_),
    do: {:error, {:invalid_plugins, "plugins must be a JSON array of plugin specs"}}
end
