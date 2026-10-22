defmodule Flezha.Service.Schedule do
  @moduledoc """
  Published service-API surface for scheduled relay starts: the stored
  entries and when each fires next.

  Every published function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.

  Schedule management is a `_system` privilege. The store is one durable table
  for the whole controller and the hub is a cluster-wide singleton: a listing
  crosses every namespace, and an entry's own `namespace` field says where its
  firings land rather than partitioning the registry. So every `schedule.*`
  action is judged in the system namespace — an `admin@_system` token, or a
  principal bound to `_system` with the role the action needs. Reading the
  registry is a viewer's; changing it is a user's.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ.Namespace
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Runner.FleetManager
  alias Flezha.Schedule.Cron
  alias Flezha.Schedule.Hub
  alias Flezha.Schedule.Job
  alias Flezha.Schedule.Store
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @schedule_name [type: :string, doc: "The schedule's name."]

  @doc """
  Every stored schedule, by name, with what the hub holds for it. That is the
  node that fires it, its last fire and that fire's result, when it fires next,
  in UTC, and `dormant?`. The next fire is the slot the job is armed for, `nil`
  while it is dormant; only an entry with no job answering has it computed from
  the cron.
  """
  @spec list(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list(caller_context), verb: :schedule_list do
    statuses = Hub.statuses()

    {:ok,
     Enum.map(Store.list(), fn entry ->
       status = Map.get(statuses, entry.name, %{})

       Map.merge(entry, %{
         node: Map.get(status, :node),
         last_fire: Map.get(status, :last_fire),
         next_fire: Map.get_lazy(status, :next_fire, fn -> next_fire_of(entry) end),
         dormant?: Map.get(status, :dormant?, false)
       })
     end)}
  end

  @doc """
  Store a schedule and assert it on the hub. `fields` holds `name`, `cron`,
  `timezone`, `factory_id`, `namespace`, `init_values`, `text`, `channel` and
  `plugins`. Once asserted, the schedule starts firing. A factory a connected
  runner announces as a stream is refused, and the plugin specs are checked now
  rather than at the first fire. Answers the entry, when it fires next, and
  whether its factory has a live runner yet: firings fail until one connects.
  """
  @spec add(keyword() | map(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb add(fields, caller_context),
    verb: :schedule_add,
    target: &schedule_target/1,
    args: [fields: [type: :keyword, doc: "The schedule's fields."]] do
    fields = Map.new(fields)
    namespace = Namespace.or_default(Map.get(fields, :namespace) || "")

    fields =
      Map.merge(fields, %{
        namespace: namespace,
        init_values: Map.get(fields, :init_values) || %{},
        plugins: Map.get(fields, :plugins) || ""
      })

    with :ok <- Hub.quorum(),
         :ok <- steps_only(namespace, fields[:factory_id]),
         :ok <- plugins_decode(fields.plugins, namespace, fields[:name]),
         {:ok, entry} <- Store.add(fields) do
      _ = Hub.assert(entry)

      {:ok,
       %{
         entry: entry,
         next_fire: next_fire_of(entry),
         runner_missing: runner_missing?(namespace, entry.factory_id)
       }}
    end
  end

  @doc "Forget a schedule and retract it from the hub. It stops firing."
  @spec remove(String.t(), caller_context()) :: :ok | {:error, term()}
  verb remove(name, caller_context), verb: :schedule_remove, args: [name: @schedule_name] do
    # Store first: retracting first would let the periodic re-assert sweep
    # resurrect the job before the row is gone.
    with :ok <- Hub.quorum(),
         :ok <- Store.remove(name) do
      _ = Hub.retract(name)
      :ok
    end
  end

  @doc """
  Fire a schedule once, now. It is judged as a start where its instance starts.
  Hub-free, so a manual fire works on any node whether or not the schedule hub
  is up.
  """
  @spec run(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb run(name, caller_context),
    verb: :schedule_run,
    row: &stored/1,
    as: located,
    args: [name: @schedule_name] do
    entry = located.entry

    case Job.spawn_instance(entry, "#{entry.name}@manual-#{System.system_time(:second)}") do
      {:ok, %{relay_id: id}} -> {:ok, %{relay_id: id, outcome: :spawned}}
      :duplicate -> {:ok, %{relay_id: nil, outcome: :already_running}}
      {:error, _reason} = refused -> refused
    end
  end

  # `add/2` takes its fields as a map or a keyword list, so the audit reads the
  # name only when there is one to read.
  defp schedule_target([%{name: name} | _]), do: %{schedule: name}
  defp schedule_target([[_ | _] = fields | _]), do: %{schedule: fields[:name]}
  defp schedule_target(_args), do: %{}

  defp stored([name | _]) do
    case Store.get(name) do
      {:ok, entry} -> {:ok, %{namespace: entry.namespace, entry: entry}}
      :error -> {:error, {:schedule_not_found, name}}
    end
  end

  defp steps_only(namespace, factory_id) do
    case FleetManager.factory_run(namespace, factory_id) do
      {:ok, :flow} -> {:error, {:unschedulable_run, factory_id}}
      _steps_or_unannounced -> :ok
    end
  catch
    :exit, _ -> :ok
  end

  # A slot instance id only exists at fire time, so the check runs against a
  # sentinel that could never be one.
  defp plugins_decode(plugins, namespace, name) do
    with {:ok, _} <- SpecDecoder.decode(plugins, namespace, "#{name}@spec-check"), do: :ok
  end

  # The factory's runner may simply not have connected yet.
  defp runner_missing?(namespace, factory_id) do
    FleetManager.runners_for_factory(namespace, factory_id) == []
  catch
    :exit, _ -> false
  end

  defp next_fire_of(entry) do
    with {:ok, cron} <- Cron.parse(entry.cron),
         {:ok, next} <- Cron.next_fire(cron, entry.timezone, DateTime.utc_now()),
         do: next,
         else: (_ -> nil)
  end
end
