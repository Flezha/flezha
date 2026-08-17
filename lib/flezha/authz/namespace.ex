defmodule Flezha.AuthZ.Namespace do
  @moduledoc """
  Namespace is a hard entity: every Operative, every Procedure, every
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

  @system "_system"

  # Reserved prefix; new namespace names cannot start with an
  # underscore so we keep room for future system-owned namespaces.
  @name_regex ~r/\A[a-z][a-z0-9-]{0,62}\z/

  @default_quotas %{
    max_operatives: 100,
    max_entities_per_operative: 1_024,
    max_mailbox_bytes_total: 268_435_456,
    max_message_rate_per_second: 100_000,
    max_state_bytes: 107_374_182_400,
    max_runners: 32
  }

  @type name :: binary()
  @type quotas :: %{
          required(:max_operatives) => non_neg_integer(),
          required(:max_entities_per_operative) => non_neg_integer(),
          required(:max_mailbox_bytes_total) => non_neg_integer(),
          required(:max_message_rate_per_second) => non_neg_integer(),
          required(:max_state_bytes) => non_neg_integer(),
          required(:max_runners) => non_neg_integer()
        }

  @doc "The system namespace name (`_system`)."
  @spec system_name() :: name()
  def system_name, do: @system

  @doc "True when `name` is the system namespace."
  @spec system?(name()) :: boolean()
  def system?(@system), do: true
  def system?(_), do: false

  @doc """
  Validate a namespace name.

  Returns `:ok` for `_system` and any string matching
  `[a-z][a-z0-9-]{0,62}`. Other inputs return
  `{:error, :invalid_namespace_name}`.
  """
  @spec validate_name(term()) :: :ok | {:error, :invalid_namespace_name}
  def validate_name(@system), do: :ok

  def validate_name(name) when is_binary(name) do
    if Regex.match?(@name_regex, name),
      do: :ok,
      else: {:error, :invalid_namespace_name}
  end

  def validate_name(_), do: {:error, :invalid_namespace_name}

  @doc "Default quota map used when a namespace is created without overrides."
  @spec default_quotas() :: quotas()
  def default_quotas, do: @default_quotas

  @doc """
  Merge user-supplied quota overrides over the defaults, validating
  that every value is a non-negative integer.
  """
  @spec build_quotas(map()) :: {:ok, quotas()} | {:error, term()}
  def build_quotas(overrides) when is_map(overrides) do
    Enum.reduce_while(overrides, {:ok, @default_quotas}, fn {k, v}, {:ok, acc} ->
      cond do
        not Map.has_key?(@default_quotas, k) ->
          {:halt, {:error, {:unknown_quota_field, k}}}

        not (is_integer(v) and v >= 0) ->
          {:halt, {:error, {:invalid_quota_value, k, v}}}

        true ->
          {:cont, {:ok, Map.put(acc, k, v)}}
      end
    end)
  end
end
