defmodule Flezha.Service.Schedule do
  @moduledoc """
  Published service-API surface for scheduled operative starts: the stored
  entries, the hub's view of them, and the cron arithmetic a caller needs to
  show an operator when a schedule fires next.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument and answers data. None of them authorizes an action, because
  `Flezha.AuthZ.Action` names no `schedule.*` verb and `flz schedule` authorizes
  nothing either — this surface is as gated as the terminal is, and no more.
  Naming those actions and granting them is a policy decision recorded for the
  repository owner.
  """

  alias Flezha.Schedule.Cron
  alias Flezha.Schedule.Hub
  alias Flezha.Schedule.Store
  alias Flezha.Service.CallerContext

  @type caller_context :: CallerContext.t()

  @doc "Every stored schedule, by name."
  @spec list(caller_context()) :: {:ok, [map()]} | {:error, term()}
  def list(caller_context) do
    CallerContext.validated(caller_context, fn -> {:ok, Store.list()} end)
  end

  @doc "Store a schedule from `fields`."
  @spec add(map(), caller_context()) :: term() | {:error, term()}
  def add(fields, caller_context) when is_map(fields) do
    CallerContext.validated(caller_context, fn -> Store.add(fields) end)
  end

  @doc "Forget the schedule `name`."
  @spec remove(String.t(), caller_context()) :: term() | {:error, term()}
  def remove(name, caller_context) when is_binary(name) do
    CallerContext.validated(caller_context, fn -> Store.remove(name) end)
  end

  @doc "What the hub currently holds for each schedule."
  @spec statuses(caller_context()) :: {:ok, term()} | {:error, term()}
  def statuses(caller_context) do
    CallerContext.validated(caller_context, fn -> {:ok, Hub.statuses()} end)
  end

  @doc "Assert `entry` on the hub, so it starts firing."
  @spec assert_entry(term(), caller_context()) :: term() | {:error, term()}
  def assert_entry(entry, caller_context) do
    CallerContext.validated(caller_context, fn -> Hub.assert(entry) end)
  end

  @doc "Retract the schedule `name` from the hub, so it stops firing."
  @spec retract(String.t(), caller_context()) :: term() | {:error, term()}
  def retract(name, caller_context) when is_binary(name) do
    CallerContext.validated(caller_context, fn -> Hub.retract(name) end)
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
end
