defmodule Flezha.Plugin.Extension.Ctx.Msg do
  @moduledoc """
  Context struct for the `:on_message` extension (kind: `:request`). The
  `key` field selects which subscriber handles the request; `reply_to` and
  `ref` form the reply envelope.
  """

  @type t() :: %__MODULE__{
          key: atom(),
          payload: binary(),
          reply_to: pid(),
          ref: reference(),
          relay_id: String.t(),
          namespace: atom() | String.t(),
          executor_id: String.t() | nil
        }

  defstruct [
    :key,
    :payload,
    :reply_to,
    :ref,
    :relay_id,
    :namespace,
    :executor_id
  ]
end
