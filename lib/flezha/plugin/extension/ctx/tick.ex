defmodule Flezha.Plugin.Extension.Ctx.Tick do
  @moduledoc """
  Context struct for the `:on_interval` extension (kind: `:timer`). Scheduled
  invocation carries operator identity and the subscriber's plugin id.
  """

  @type t() :: %__MODULE__{
          operator_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil,
          plugin_id: binary()
        }

  defstruct [:operator_id, :namespace, :runner_id, :plugin_id]
end
