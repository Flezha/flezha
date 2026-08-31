defmodule Flezha.AuthZ.Config do
  @moduledoc """
  Runtime configuration accessors with `env-var > app-config > default`
  precedence.

  Centralises every `Application.get_env(:flezha, :authn_authz, ...)`
  lookup so config tweaks live in one place.
  """

  @default_dir "app_data/auth"
  @default_bootstrap_token_path "app_data/auth/bootstrap.token"
  @default_cli_token_ttl_seconds 86_400
  @default_sdk_token_ttl_seconds 2_592_000

  @doc "Returns `true` when the application config has `:enabled` set to `true`."
  @spec config_enabled?() :: boolean()
  def config_enabled?() do
    case env("FLEZHA_AUTHZ_ENABLED") do
      nil ->
        Keyword.get(authn_authz_kw(), :enabled, false)

      val ->
        env_truthy?(val)
    end
  end

  @doc "Returns `true` when the explicit env override is set to a false-y value."
  @spec env_force_disabled?() :: boolean()
  def env_force_disabled?() do
    case env("FLEZHA_AUTHZ_ENABLED") do
      nil -> false
      v -> not env_truthy?(v)
    end
  end

  @doc "Resolve the auth directory (`FLEZHA_AUTHZ_DIR` > config > default)."
  @spec auth_dir() :: String.t()
  def auth_dir() do
    env("FLEZHA_AUTHZ_DIR") ||
      Keyword.get(authn_authz_kw(), :auth_dir, @default_dir)
  end

  @doc "Path of the bootstrap token file."
  @spec bootstrap_token_path() :: String.t()
  def bootstrap_token_path() do
    env("FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH") ||
      Keyword.get(authn_authz_kw(), :bootstrap_token_path, @default_bootstrap_token_path)
  end

  @doc "Default TTL in seconds for a token issued to the given source kind."
  @spec default_token_ttl_seconds(:cli | :ui | :sdk) :: pos_integer()
  def default_token_ttl_seconds(:sdk) do
    Keyword.get(authn_authz_kw(), :sdk_token_ttl_seconds, @default_sdk_token_ttl_seconds)
  end

  def default_token_ttl_seconds(_kind) do
    Keyword.get(authn_authz_kw(), :token_ttl_default_seconds, @default_cli_token_ttl_seconds)
  end

  @doc "Per-namespace quota overrides from the config map (may be empty)."
  @spec namespace_quota_overrides() :: %{optional(binary()) => map()}
  def namespace_quota_overrides() do
    Keyword.get(authn_authz_kw(), :namespaces, %{})
  end

  # ---------- private ----------

  defp authn_authz_kw() do
    Application.get_env(:flezha, :authn_authz, [])
  end

  defp env(name), do: System.get_env(name)

  defp env_truthy?(""), do: false
  defp env_truthy?(v) when is_binary(v), do: String.downcase(v) in ["1", "true", "yes", "on"]
end
