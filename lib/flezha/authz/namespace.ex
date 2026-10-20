defmodule Flezha.AuthZ.Namespace do
  @moduledoc """
  Namespace is a hard cell: every Relay, every Tasklet, every
  persisted-state path, every audit record, and every observability
  signal carries one.

  `_system` is the singleton compatibility namespace — it exists at
  boot, owns everything created before this capability landed, and is
  the only namespace from which auth-state mutations on other
  namespaces are permitted.

  This module defines the namespace name validation and the default
  quota set; the durable catalog lives in
  `Flezha.AuthZ.NamespaceStore`.
  """

  alias Flezha.Identificator

  @system "_system"
  @default "default"

  @default_quotas %{
    max_relays: 100,
    max_cells_per_relay: 1_024,
    max_mailbox_bytes_total: 268_435_456,
    max_message_rate_per_second: 100_000,
    max_state_bytes: 107_374_182_400,
    max_runners: 32
  }

  @type name :: binary()
  @type quotas :: %{
          required(:max_relays) => non_neg_integer(),
          required(:max_cells_per_relay) => non_neg_integer(),
          required(:max_mailbox_bytes_total) => non_neg_integer(),
          required(:max_message_rate_per_second) => non_neg_integer(),
          required(:max_state_bytes) => non_neg_integer(),
          required(:max_runners) => non_neg_integer()
        }

  @doc "The system namespace name (`_system`)."
  @spec system_name() :: name()
  def system_name, do: @system

  @doc "The namespace an argument names: `default` when the argument was left empty."
  @spec or_default(name()) :: name()
  def or_default(""), do: @default
  def or_default(name) when is_binary(name), do: name

  @doc "True when `name` is the system namespace."
  @spec system?(name()) :: boolean()
  def system?(@system), do: true
  def system?(_), do: false

  @doc """
  The namespace a listing may show `caller_context`, or `:all` for the whole
  cluster.

  An admin in `_system` sees everything, which is the caller view. Every
  caller whose context carries a namespace sees that one.

  `unbound` is what a context carrying **no** namespace answers, and it is a
  required decision rather than a property of the caller. It defaults to
  `:system`, the closed answer, so a surface that does not think about it
  cannot inherit an open one. A context reaches a verb body with no namespace
  in three ways and only one of them is "authorization is off":

    * the posture is `:open`, where nothing was judged at all;
    * `source: :internal`, which `Flezha.AuthZ` judges as `admin@_system`;
    * under `:enforced`, a caller whose principal is bound in
      `Flezha.AuthZ.RoleStore` rather than on the context. A `:caller`-scoped
      verb then judges it in `_system`, so it passes only when its stored
      binding IS `_system`; a tenant-bound principal is refused with
      `:namespace_mismatch` before any body runs.

  So `unbound: :all` is a deliberate fail-open, sound only for a
  `:caller`-scoped verb, where the third case cannot smuggle a tenant in. A
  verb judged at `:system` or `{:argument, n}` scope must not pass `:all`:
  there, an unbound context says nothing about what the caller may read.
  """
  @spec listing_scope(map(), :all | :system) :: :all | name()
  def listing_scope(caller_context, unbound \\ :system)
  def listing_scope(%{role: :admin, namespace: @system}, _unbound), do: :all
  def listing_scope(%{namespace: namespace}, _unbound), do: namespace || @system
  def listing_scope(_no_namespace, :all), do: :all
  def listing_scope(_no_namespace, :system), do: @system

  @doc """
  The members of `names` that `caller_context` may be shown, by
  `listing_scope/2` — whose `unbound` argument this takes and whose warning
  about it applies here unchanged.

  The one place a namespace listing is narrowed, so the terminal's catalog
  answer and the console's running-hub answer cannot disagree about which
  namespaces a tenant-bound caller knows exist.
  """
  @spec visible([name()], map(), :all | :system) :: [name()]
  def visible(names, caller_context, unbound \\ :system) do
    case listing_scope(caller_context, unbound) do
      :all -> names
      namespace -> Enum.filter(names, &(&1 == namespace))
    end
  end

  @doc """
  Validate a namespace name.

  Returns `:ok` for `_system` and any string matching
  `[a-z][a-z0-9_-]{0,62}`. Other inputs return
  `{:error, {:invalid_namespace_name, name}}`.
  """
  @spec validate_name(term()) :: :ok | {:error, {:invalid_namespace_name, term()}}
  def validate_name(@system), do: :ok

  # The rule is the `:namespace` row. The leading letter is what keeps the
  # underscore prefix for system-owned namespaces; an underscore after it is
  # ordinary.
  def validate_name(name) when is_binary(name) do
    case Identificator.validate(:namespace, name) do
      :ok -> :ok
      {:error, _refusal} -> {:error, {:invalid_namespace_name, name}}
    end
  end

  def validate_name(name), do: {:error, {:invalid_namespace_name, name}}

  @doc "Every quota field with its default ceiling."
  @spec default_quotas() :: quotas()
  def default_quotas, do: @default_quotas

  @doc """
  Merge user-supplied quota overrides over the defaults, validating
  that every value is a non-negative integer.
  """
  @spec build_quotas(map()) :: {:ok, quotas()} | {:error, term()}
  def build_quotas(overrides) when is_map(overrides) do
    defaults = default_quotas()

    Enum.reduce_while(overrides, {:ok, defaults}, fn {k, v}, {:ok, acc} ->
      cond do
        not Map.has_key?(defaults, k) ->
          {:halt, {:error, {:unknown_quota_field, k}}}

        not (is_integer(v) and v >= 0) ->
          {:halt, {:error, {:invalid_quota_value, k, v}}}

        true ->
          {:cont, {:ok, Map.put(acc, k, v)}}
      end
    end)
  end
end
