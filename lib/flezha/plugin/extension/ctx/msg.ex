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
          processor_id: String.t(),
          namespace: atom() | String.t(),
          compute_engine_id: String.t() | nil
        }

  defstruct [
    :key,
    :payload,
    :reply_to,
    :ref,
    :processor_id,
    :namespace,
    :compute_engine_id
  ]
end
