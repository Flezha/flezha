defmodule Flezha.Plugin do
  @moduledoc """
  Unified plugin struct used by every extension-point subscription on a host
  (e.g. `Flezha.Operative.Overwatch`).

  A single `%Flezha.Plugin{}` may subscribe to multiple extension points.
  Each entry in `subscriptions` is a `{extension_name, sub_opts}` tuple; the
  provider module MUST export a callback for each subscribed extension kind
  (e.g. `on_input/2`, `on_message/2`, `on_operative_init/2`, ...).
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
          operative_id: binary() | nil
        }

  @enforce_keys [:id, :provider]
  defstruct [
    :id,
    :provider,
    opts: %{},
    subscriptions: [],
    namespace: nil,
    operative_id: nil
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
  Point this plugin at one member of a partition group.

  The one definition of what a fanned-out plugin instance is: the member's
  operative id, the member's `#pK` appended to the plugin id (which is also
  what keeps this node's plugin ids unique when two members land on it), and
  the member's partition opts merged in as integers. `plugin_id` is
  deliberately not injected — providers that key workers by it default to a
  per-member-unique value, and an injected shared one would collide.

  Re-applying it to an already-fanned-out plugin re-points it: the suffix is
  replaced, never stacked, which is what lets a rescale mint member `k` of the
  new set from member `0` of the old one.
  """
  @spec for_partition(t(), binary(), map()) :: t()
  def for_partition(%__MODULE__{} = plugin, member_id, identity) do
    %__MODULE__{
      plugin
      | id: base_id(plugin.id) <> Flezha.Partition.member_suffix(identity.index),
        operative_id: member_id,
        opts: Map.merge(plugin.opts, Flezha.Partition.provider_opts(identity))
    }
  end

  defp base_id(plugin_id) do
    case Flezha.Partition.split_member_suffix(plugin_id) do
      {base, _index} -> base
      :error -> plugin_id
    end
  end

  @doc """
  Strip transient fields that cannot be persisted across a BEAM
  restart, returning a value safe to embed in any persistence layer
  (HotSwap handover state, ProcessHub child-spec, cluster snapshots,
  DETS, ...).

  Today every field on `%Flezha.Plugin{}` (`:id`, `:provider`, `:opts`,
  `:subscriptions`, `:namespace`, `:operative_id`) is inert data —
  there are no live pids, monitor refs, timer refs, or closures held in
  the struct itself — so this function is the identity. It exists as a
  contract surface: any future field that holds runtime-only state
  (e.g. a worker pid or timer ref tracked alongside the plugin) MUST
  be dropped here, otherwise it will be persisted into the cspec by
  `Flezha.Service.PluginManager` (through `ProcessRegistry.update/3`) and
  resurface on the next Overwatch (re)start as a stale reference.

  Verified by `test/flezha/plugin_test.exs` — round-tripping the
  output through `:erlang.term_to_binary/1` plus
  `:erlang.binary_to_term/1` SHALL be a no-op.
  """
  @spec strip_transient(t()) :: t()
  def strip_transient(%__MODULE__{} = plugin), do: plugin
end
