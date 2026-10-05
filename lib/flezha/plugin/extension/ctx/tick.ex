defmodule Flezha.Plugin.Extension.Ctx.Tick do
  @moduledoc """
  Context struct for the `:on_interval` extension (kind: `:timer`). Scheduled
  invocation carries relay identity and the subscriber's plugin id.
  """

  @type t() :: %__MODULE__{
          relay_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil,
          plugin_id: binary()
        }

  defstruct [:relay_id, :namespace, :runner_id, :plugin_id]
end
