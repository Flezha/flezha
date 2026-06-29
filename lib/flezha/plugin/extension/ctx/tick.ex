defmodule Flezha.Plugin.Extension.Ctx.Tick do
  @moduledoc """
  Context struct for the `:on_interval` extension (kind: `:timer`). Scheduled
  invocation carries operative identity and the subscriber's plugin id.
  """

  @type t() :: %__MODULE__{
          operative_id: String.t(),
          namespace: atom() | String.t(),
          executor_id: String.t() | nil,
          plugin_id: binary()
        }

  defstruct [:operative_id, :namespace, :executor_id, :plugin_id]
end
