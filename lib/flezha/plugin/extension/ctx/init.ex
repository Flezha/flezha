defmodule Flezha.Plugin.Extension.Ctx.Init do
  @moduledoc """
  Context struct for the `:on_relay_init` extension (kind: `:fold`).
  `accumulated` is threaded through each subscriber.
  """

  @type t() :: %__MODULE__{
          relay_id: String.t(),
          namespace: atom() | String.t(),
          factory_id: String.t(),
          accumulated: map()
        }

  defstruct [:relay_id, :namespace, :factory_id, accumulated: %{}]
end
