defmodule Flezha.Plugin.Extension.Ctx.Tick do
  @moduledoc """
  Context struct for the `:on_interval` extension (kind: `:timer`). Scheduled
  invocation carries processor identity and the subscriber's plugin id.
  """

  @type t() :: %__MODULE__{
          processor_id: String.t(),
          namespace: atom() | String.t(),
          compute_engine_id: String.t() | nil,
          plugin_id: binary()
        }

  defstruct [:processor_id, :namespace, :compute_engine_id, :plugin_id]
end
