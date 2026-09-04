defmodule Flezha.Service.Schedule do
  @moduledoc """
  Published service-API surface for scheduled operative starts: the stored
  entries, the hub's view of them, and the cron arithmetic a caller needs to
  show an operator when a schedule fires next.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument and answers data.

  Schedule management is a `_system` privilege. The store is one durable table
  for the whole controller and the hub is a cluster-wide singleton: a listing
  crosses every namespace, and an entry's own `namespace` field says where its
  firings land rather than partitioning the registry. So every `schedule.*`
  action is judged in the system namespace — an `admin@_system` token, or a
  principal bound to `_system` with the role the action needs. Reading the
  registry is a viewer's; changing it is an operator's.

  The cron arithmetic carries no action. `parse_cron/2` and `next_fire/4` read
  nothing and reach no store; they are the same arithmetic a caller could do
  itself, published so it does not have to.
  """

  alias Flezha.Schedule.Cron
  alias Flezha.Schedule.Hub
  alias Flezha.Schedule.Store
  alias Flezha.Service.CallerContext

  @audit_source :schedule_service

  @type caller_context :: CallerContext.t()

  @doc "Every stored schedule, by name."
  @spec list(caller_context()) :: {:ok, [map()]} | {:error, term()}
  def list(caller_context) do
    schedule_read(caller_context, fn -> {:ok, Store.list()} end)
  end

  @doc "Store a schedule from `fields`."
  @spec add(map(), caller_context()) :: term() | {:error, term()}
  def add(fields, caller_context) when is_map(fields) do
    gated(caller_context, :"schedule.add", fields[:name], fn -> Store.add(fields) end)
  end

  @doc "Forget the schedule `name`."
  @spec remove(String.t(), caller_context()) :: term() | {:error, term()}
  def remove(name, caller_context) when is_binary(name) do
    gated(caller_context, :"schedule.remove", name, fn -> Store.remove(name) end)
  end

  @doc "What the hub currently holds for each schedule."
  @spec statuses(caller_context()) :: {:ok, term()} | {:error, term()}
  def statuses(caller_context) do
    schedule_read(caller_context, fn -> {:ok, Hub.statuses()} end)
  end

  @doc "Assert `entry` on the hub, so it starts firing."
  @spec assert_entry(term(), caller_context()) :: term() | {:error, term()}
  def assert_entry(entry, caller_context) do
    gated(caller_context, :"schedule.assert", schedule_name(entry), fn -> Hub.assert(entry) end)
  end

  @doc "Retract the schedule `name` from the hub, so it stops firing."
  @spec retract(String.t(), caller_context()) :: term() | {:error, term()}
  def retract(name, caller_context) when is_binary(name) do
    gated(caller_context, :"schedule.retract", name, fn -> Hub.retract(name) end)
  end

  @doc "Parse a cron expression, or say why it cannot be parsed."
  @spec parse_cron(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  def parse_cron(expression, caller_context) when is_binary(expression) do
    CallerContext.validated(caller_context, fn -> Cron.parse(expression) end)
  end

  @doc "The next UTC instant a parsed cron fires after `after_utc`, in `tz`."
  @spec next_fire(term(), String.t(), DateTime.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  def next_fire(cron, tz, %DateTime{} = after_utc, caller_context) do
    CallerContext.validated(caller_context, fn -> Cron.next_fire(cron, tz, after_utc) end)
  end

  defp schedule_read(caller_context, fun) do
    CallerContext.scoped_read(
      caller_context,
      :"schedule.list",
      CallerContext.system_scope(),
      fun
    )
  end

  defp gated(caller_context, action, name, fun) do
    CallerContext.gated(
      caller_context,
      action,
      CallerContext.system_scope(),
      @audit_source,
      %{schedule: name},
      fun
    )
  end

  # `assert_entry/2` takes a term, so the audit reads the name only when there
  # is one to read.
  defp schedule_name(%{name: name}), do: name
  defp schedule_name(_entry), do: nil
end
