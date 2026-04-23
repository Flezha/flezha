defmodule Flezha.Plugin do
  @moduledoc """
  Unified plugin struct used by every extension-point subscription on a host
  (e.g. `Flezha.Processor.Overwatch`).

  A single `%Flezha.Plugin{}` may subscribe to multiple extension points.
  Each entry in `subscriptions` is a `{extension_name, sub_opts}` tuple; the
  provider module MUST export a callback for each subscribed extension kind
  (e.g. `on_input/2`, `on_message/2`, `on_processor_init/2`, ...).
  """

  @type extension_name :: atom()
  @type sub_opts :: keyword()
  @type subscription :: {extension_name(), sub_opts()}

  @type t() :: %__MODULE__{
          id: binary(),
          provider: module(),
          opts: map(),
          subscriptions: [subscription()],
          namespace: atom() | binary() | nil,
          processor_id: binary() | nil
        }

  @enforce_keys [:id, :provider]
  defstruct [
    :id,
    :provider,
    opts: %{},
    subscriptions: [],
    namespace: nil,
    processor_id: nil
  ]

  @doc """
  Construct a new `%Flezha.Plugin{}` from a keyword list.

  Raises `ArgumentError` if `:id` or `:provider` is missing.
  """
  @spec new(keyword()) :: t()
  def new(fields) when is_list(fields) do
    unless Keyword.has_key?(fields, :id) and Keyword.has_key?(fields, :provider) do
      raise ArgumentError,
            "Flezha.Plugin.new/1 requires :id and :provider — got #{inspect(fields)}"
    end

    struct!(__MODULE__, fields)
  end

  @doc """
  Strip transient fields that can't be persisted across a BEAM restart.
  Used by the recovery subsystem before writing to DETS.
  """
  @spec strip_transient(t()) :: t()
  def strip_transient(%__MODULE__{} = plugin), do: plugin
end
