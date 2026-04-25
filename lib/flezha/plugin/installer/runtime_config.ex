defmodule Flezha.Plugin.Installer.RuntimeConfig do
  @moduledoc """
  Pure helpers for resolving installable-plugin settings from env vars.

  Extracted from `config/runtime.exs` so the parse logic — truthiness
  coercion, PEM-list loading, integer parsing — is unit-testable. The
  `runtime.exs` file just calls these helpers and feeds the result into
  `Application.put_env/3`.
  """

  @doc """
  True when any of the `FLEZHA_PLUGIN_*` knobs (other than the enabled
  toggle) are present in `env_map`.
  """
  @spec any_env_var_present?(%{optional(String.t()) => String.t()}) :: boolean()
  def any_env_var_present?(env_map) do
    Enum.any?(
      ~w(FLEZHA_PLUGIN_ROOT FLEZHA_PLUGIN_REQUIRE_SIGNATURE FLEZHA_PLUGIN_ALLOW_UNSIGNED
         FLEZHA_PLUGIN_TRUSTED_KEYS FLEZHA_PLUGIN_MAX_PACKAGE_BYTES FLEZHA_PLUGIN_MAX_SOURCE_FILES),
      fn var -> Map.has_key?(env_map, var) end
    )
  end

  @doc """
  Resolves `plugin_installer_enabled` from the env map, falling back to
  `base_default`.
  """
  @spec resolve_enabled(%{optional(String.t()) => String.t()}, boolean()) :: boolean()
  def resolve_enabled(env_map, base_default) do
    case Map.get(env_map, "FLEZHA_PLUGIN_INSTALLER_ENABLED") do
      nil -> base_default
      val -> truthy?(val)
    end
  end

  @doc """
  Resolves the full `:plugin_installer` keyword list from `env_map`,
  merging over `base` (the compile-time config). `key_loader` is called
  on each `FLEZHA_PLUGIN_TRUSTED_KEYS` path entry to produce its PEM
  bytes; the default reads the file from disk.
  """
  @spec resolve_plugin_installer(%{optional(String.t()) => String.t()}, keyword(), (String.t() ->
                                                                                      binary())) ::
          keyword()
  def resolve_plugin_installer(env_map, base, key_loader \\ &File.read!/1) do
    Keyword.merge(base,
      plugin_root:
        Map.get(
          env_map,
          "FLEZHA_PLUGIN_ROOT",
          Keyword.get(base, :plugin_root, "app_data/plugins")
        ),
      trusted_keys: resolve_trusted_keys(env_map, base, key_loader),
      require_signature:
        resolve_bool(env_map, "FLEZHA_PLUGIN_REQUIRE_SIGNATURE", base, :require_signature, false),
      allow_unsigned:
        resolve_bool(env_map, "FLEZHA_PLUGIN_ALLOW_UNSIGNED", base, :allow_unsigned, true),
      max_package_bytes:
        resolve_int(
          env_map,
          "FLEZHA_PLUGIN_MAX_PACKAGE_BYTES",
          base,
          :max_package_bytes,
          50_000_000
        ),
      max_source_files:
        resolve_int(env_map, "FLEZHA_PLUGIN_MAX_SOURCE_FILES", base, :max_source_files, 200)
    )
  end

  ## Private

  defp resolve_trusted_keys(env_map, base, key_loader) do
    case Map.get(env_map, "FLEZHA_PLUGIN_TRUSTED_KEYS") do
      nil ->
        Keyword.get(base, :trusted_keys, [])

      "" ->
        []

      csv ->
        csv
        |> String.split(",", trim: true)
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))
        |> Enum.map(key_loader)
    end
  end

  defp resolve_bool(env_map, var, base, key, default) do
    case Map.get(env_map, var) do
      nil -> Keyword.get(base, key, default)
      val -> truthy?(val)
    end
  end

  defp resolve_int(env_map, var, base, key, default) do
    case Map.get(env_map, var) do
      nil -> Keyword.get(base, key, default)
      val -> String.to_integer(val)
    end
  end

  defp truthy?(val), do: String.downcase(val) in ["true", "1", "yes"]
end
