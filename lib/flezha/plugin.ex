defmodule Flezha.Plugin do
  @moduledoc """
  Unified plugin struct used by every extension-point subscription on a host
  (e.g. `Flezha.Relay.Overwatch`).

  A single `%Flezha.Plugin{}` may subscribe to multiple extension points.
  Each entry in `subscriptions` is a `{extension_name, sub_opts}` tuple; the
  provider module MUST export a callback for each subscribed extension kind
  (e.g. `on_input/2`, `on_message/2`, `on_relay_init/2`, ...).
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
          relay_id: binary() | nil
        }

  @enforce_keys [:id, :provider]
  defstruct [
    :id,
    :provider,
    opts: %{},
    subscriptions: [],
    namespace: nil,
    relay_id: nil
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
  Strip transient fields that cannot be persisted across a BEAM
  restart, returning a value safe to embed in any persistence layer
  (HotSwap handover state, ProcessHub child-spec, cluster snapshots,
  DETS, ...).

  Today every field on `%Flezha.Plugin{}` (`:id`, `:provider`, `:opts`,
  `:subscriptions`, `:namespace`, `:relay_id`) is inert data —
  there are no live pids, monitor refs, timer refs, or closures held in
  the struct itself — so this function is the identity. It exists as a
  contract surface: any future field that holds runtime-only state
  (e.g. a worker pid or timer ref tracked alongside the plugin) MUST
  be dropped here, otherwise it will be persisted into the cspec via
  `Flezha.Service.HubManager.update_relay_plugins/3` and resurface
  on the next Overwatch (re)start as a stale reference.

  Verified by `test/flezha/plugin_test.exs` — round-tripping the
  output through `:erlang.term_to_binary/1` plus
  `:erlang.binary_to_term/1` SHALL be a no-op.
  """
  @spec strip_transient(t()) :: t()
  def strip_transient(%__MODULE__{} = plugin), do: plugin
end
