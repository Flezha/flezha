defmodule Flezha.Process.Cspec do
  @moduledoc """
  A supervised child declares a restart KIND; the six keys that kind implies
  are written here once.

      Cspec.build(:my_worker, {MyWorker, :start_link, [arg]}, :worker_transient)

  Every kind is a row of `@restart_policy`, so the set of restart behaviours
  in the tree is a list that can be read rather than twenty-one literal maps
  that have to be compared. `kinds/0` is what a test asserts against.

  `start_arg/1` is the one reader of a persisted child spec's single start
  argument. ProcessHub reads its specs back after a restart and their shape is
  durable state, so this only READS one — it never re-shapes a spec ProcessHub
  wrote.
  """

  @type kind :: atom()
  @type mfa_tuple :: {module(), atom(), [term()]}

  # `restart` and `type` are what a kind IS; `shutdown` is per-row because a
  # supervisor's default (`:infinity`) and a worker's (5_000) differ, and a
  # one-shot task must not hold the tree open at all.
  @restart_policy %{
    worker_permanent: %{restart: :permanent, type: :worker, shutdown: 5_000},
    worker_transient: %{restart: :transient, type: :worker, shutdown: 5_000},
    worker_temporary: %{restart: :temporary, type: :worker, shutdown: 5_000},
    supervisor_permanent: %{restart: :permanent, type: :supervisor, shutdown: :infinity},
    supervisor_transient: %{restart: :transient, type: :supervisor, shutdown: :infinity},
    one_shot_temporary: %{restart: :temporary, type: :worker, shutdown: :brutal_kill}
  }

  # The intensity a dynamic supervisor of independently-restarting children
  # runs at: a child that keeps failing must not take the supervisor with it,
  # and three supervisors had written the same pair by hand.
  @intensity [max_restarts: 100, max_seconds: 5]

  @doc "The restart intensity a dynamic supervisor of independent children uses."
  @spec intensity() :: keyword()
  def intensity(), do: @intensity

  @doc "Every declared restart kind."
  @spec kinds() :: [kind()]
  def kinds(), do: Map.keys(@restart_policy)

  @doc "The row `kind` names."
  @spec policy(kind()) :: map()
  def policy(kind) when is_map_key(@restart_policy, kind), do: Map.fetch!(@restart_policy, kind)

  @doc """
  A child spec for `id` starting `{m, f, a}` under `kind`.

  `opts` overrides a row's own keys — `:shutdown` for a child whose drain is
  its own quantity, `:modules` for one started behind a wrapper — and nothing
  else: a child that needs a restart behaviour no row names needs a row.
  """
  @spec build(term(), mfa_tuple(), kind(), keyword()) :: Supervisor.child_spec()
  def build(id, {m, f, a} = start, kind, opts \\ [])
      when is_atom(m) and is_atom(f) and is_list(a) and is_map_key(@restart_policy, kind) do
    @restart_policy
    |> Map.fetch!(kind)
    |> Map.merge(%{id: id, start: start})
    |> Map.merge(Map.new(Keyword.take(opts, [:shutdown, :modules])))
  end

  @doc """
  The single start argument of a persisted child spec, or `:error` when the
  spec does not carry exactly one.
  """
  @spec start_arg(map() | keyword()) :: {:ok, term()} | :error
  def start_arg(%{start: {_m, _f, [arg]}}), do: {:ok, arg}
  def start_arg(%{start: {_m, _f, _args}}), do: :error
  def start_arg(spec) when is_list(spec), do: spec |> Map.new() |> start_arg()
  def start_arg(_spec), do: :error
end
