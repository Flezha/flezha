defmodule Flezha.AuthZ.Config do
  @moduledoc """
  The authn/authz settings, read through the one table.

  Every one is a row of `Flezha.Config.Keys`, so the precedence an operator
  reads about — the environment variable over the configuration file over the
  default — is the boot translation's, and this module answers the settled
  value rather than re-deriving it. What it adds is the vocabulary: a token
  kind asks for its own TTL, and the bootstrap path is one name rather than a
  keyword lookup.
  """

  alias Flezha.Config

  @doc "Whether authn/authz enforcement is on."
  @spec config_enabled?() :: boolean()
  def config_enabled?(), do: Config.get(:authz_enabled)

  @doc "Path of the bootstrap token file."
  @spec bootstrap_token_path() :: String.t()
  def bootstrap_token_path(), do: Config.get(:authz_bootstrap_token_path)

  @doc """
  Default TTL in seconds for an issued token.

  One value, because every credential with an expiry is one a human holds and a
  human can always get another. No unattended process holds an expiring
  credential at all: a runner proves its namespace with a registry entry that
  is refused an expiry, so a second, longer default for a machine would be the
  trap rather than the convenience.
  """
  @spec default_token_ttl_seconds() :: pos_integer()
  def default_token_ttl_seconds(), do: Config.get(:authz_token_ttl_default_seconds)

  @doc "Per-namespace quota overrides from the config map (may be empty)."
  @spec namespace_quota_overrides() :: %{optional(binary()) => map()}
  def namespace_quota_overrides(), do: Config.get(:authz_namespaces)
end
