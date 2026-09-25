defmodule Flezha.Service.Schedule do
  @moduledoc """
  Published service-API surface for scheduled operator starts: the stored
  entries, the hub's view of them, and the cron arithmetic a caller needs to
  show a user when a schedule fires next.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.

  Schedule management is a `_system` privilege. The store is one durable table
  for the whole controller and the hub is a cluster-wide singleton: a listing
  crosses every namespace, and an entry's own `namespace` field says where its
  firings land rather than partitioning the registry. So every `schedule.*`
  action is judged in the system namespace — an `admin@_system` token, or a
  principal bound to `_system` with the role the action needs. Reading the
  registry is a viewer's; changing it is a user's.

  The cron arithmetic carries no action. `parse_cron/2` and `next_fire/4` read
  nothing and reach no store; they are the same arithmetic a caller could do
  itself, published so it does not have to.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.Schedule.Cron
  alias Flezha.Schedule.Hub
  alias Flezha.Schedule.Store
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @doc "Every stored schedule, by name."
  @spec list(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb list(caller_context), verb: :schedule_list do
    {:ok, Store.list()}
  end

  @doc "Store a schedule from `fields`."
  @spec add(map(), caller_context()) :: term() | {:error, term()}
  verb add(fields, caller_context), verb: :schedule_add, target: &schedule_target/1 do
    Store.add(fields)
  end

  @doc "Forget the schedule `name`."
  @spec remove(String.t(), caller_context()) :: term() | {:error, term()}
  verb remove(name, caller_context), verb: :schedule_remove do
    Store.remove(name)
  end

  @doc "What the hub currently holds for each schedule."
  @spec statuses(caller_context()) :: {:ok, term()} | {:error, term()}
  verb statuses(caller_context), verb: :schedule_list do
    {:ok, Hub.statuses()}
  end

  @doc "Assert `entry` on the hub, so it starts firing."
  @spec assert_entry(term(), caller_context()) :: term() | {:error, term()}
  verb assert_entry(entry, caller_context), verb: :schedule_assert, target: &schedule_target/1 do
    Hub.assert(entry)
  end

  @doc "Retract the schedule `name` from the hub, so it stops firing."
  @spec retract(String.t(), caller_context()) :: term() | {:error, term()}
  verb retract(name, caller_context), verb: :schedule_retract do
    Hub.retract(name)
  end

  @doc "Parse a cron expression, or say why it cannot be parsed."
  @spec parse_cron(String.t(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb parse_cron(expression, caller_context), verb: :cron_arithmetic do
    Cron.parse(expression)
  end

  @doc "The next UTC instant a parsed cron fires after `after_utc`, in `tz`."
  @spec next_fire(term(), String.t(), DateTime.t(), caller_context()) ::
          {:ok, term()} | {:error, term()}
  verb next_fire(cron, tz, after_utc, caller_context), verb: :cron_arithmetic do
    Cron.next_fire(cron, tz, after_utc)
  end

  # `add/2` and `assert_entry/2` take a map or a term, so the audit reads the
  # name only when there is one to read.
  defp schedule_target([%{name: name} | _]), do: %{schedule: name}
  defp schedule_target(_args), do: %{}
end
