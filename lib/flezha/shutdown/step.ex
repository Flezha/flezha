defmodule Flezha.Shutdown.Step do
  @moduledoc """
  The one shape a teardown step has, declared by the module that owns the
  resource it tears down.

  A module declares its steps with `shutdown_steps/0`; `Flezha.Shutdown.run/3`
  discovers every declaring module and folds the steps of one scope in their
  declared order, each once, each bounded, so no stop path names a step and a
  new resource with a teardown is one new step. Scopes nest downward: a
  `:node` drain moves every relay, a `:namespace` stop takes its runners,
  plugins and hub, an `:relay` stop takes one relay's children, disk
  and quota, and a `:process` teardown is what the Overwatch itself owes when
  it exits.
  """

  @type scope :: :process | :relay | :namespace | :node

  @typedoc """
  One step: its scope, its order within that scope (unique), a name for the
  report, and the function that runs it against the walk's target under the
  remaining budget. `{:ok, term}` carries a result the caller reads back.
  """
  @type t :: %{
          scope: scope(),
          order: pos_integer(),
          name: atom(),
          run: (term(), non_neg_integer() -> :ok | {:ok, term()} | {:error, term()})
        }

  @callback shutdown_steps() :: [t()]
end
