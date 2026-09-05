defmodule Flezha.Plugin do
  @moduledoc """
  Unified plugin struct used by every extension-point subscription on a host
  (e.g. `Flezha.Operative.Overwatch`).

  A single `%Flezha.Plugin{}` may subscribe to multiple extension points.
  Each entry in `subscriptions` is a `{extension_name, sub_opts}` tuple; the
  provider module MUST export a callback for each subscribed extension kind
  (e.g. `on_input/2`, `on_message/2`, `on_operative_init/2`, ...).

  At the boundary the name may be text or an atom and the opts a map or a
  keyword list — the shapes the CLI, the operative-start spec, an installed
  package manifest and direct construction produce.
  `Flezha.Plugin.Extension.Table.canonicalise/2` reduces all of them to
  `{atom, keyword}` with typed values, which is what every later reader sees.
  """

  @type extension_name :: atom() | binary()
  @type sub_opts :: keyword() | map()
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
  This plugin with only the options an operator may write.

  A running plugin's opts carry what `init_provider/1` derived at start (the
  websocket source's live `subscription`, for one), and re-enabling a partition
  member with those is refused as unknown options. The provider's declaration
  names what is writable, so the member starts from that; the derived values are
  re-derived by the new member's own `init_provider/1`, which is the only thing
  that can produce them for it.
  """
  @spec declared_opts_only(t()) :: t()
  def declared_opts_only(%__MODULE__{provider: provider, opts: opts} = plugin)
      when is_map(opts) and is_atom(provider) and not is_nil(provider) do
    writable =
      provider.__provider__()
      |> Flezha.Plugin.ProviderBehaviour.Decl.public_options()
      |> MapSet.new(&elem(&1, 0))

    %__MODULE__{plugin | opts: Map.filter(opts, &MapSet.member?(writable, elem(&1, 0)))}
  rescue
    # A module that answers no declaration is not a provider this can read.
    UndefinedFunctionError -> plugin
  end

  def declared_opts_only(%__MODULE__{} = plugin), do: plugin

  @doc """
  Strip transient fields that cannot be persisted across a BEAM
  restart, returning a value safe to embed in any persistence layer
  (HotSwap handover state, ProcessHub child-spec, DETS, ...).

  Every field on `%Flezha.Plugin{}` itself is inert data, so what this drops is
  the provider's own: an option row declared `transient: true` is held in memory
  and never written down. A resolved credential is the case that exists today.

  Any future field that holds runtime-only state (a worker pid, a timer ref)
  MUST be dropped here too, or it is persisted into the cspec by
  `Flezha.Service.PluginManager` (through `ProcessRegistry.update/3`) and
  resurfaces on the next Overwatch (re)start as a stale reference.

  Verified by `test/flezha/plugin_test.exs`: round-tripping the output through
  `:erlang.term_to_binary/1` plus `:erlang.binary_to_term/1` SHALL be a no-op.
  """
  @spec strip_transient(t()) :: t()
  def strip_transient(%__MODULE__{provider: provider, opts: opts} = plugin)
      when is_map(opts) and is_atom(provider) and not is_nil(provider) do
    transient = Flezha.Plugin.ProviderBehaviour.Decl.transient_options(provider.__provider__())
    %__MODULE__{plugin | opts: Map.drop(opts, transient)}
  rescue
    # A module that answers no declaration is not a provider this can read, and
    # a persistence boundary is the wrong place to fail over it.
    UndefinedFunctionError -> plugin
  end

  def strip_transient(%__MODULE__{} = plugin), do: plugin
end
