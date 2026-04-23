defmodule Flezha.Plugin.Installer.RuntimeConfigTest do
  @moduledoc """
  Unit tests for the env-var resolution logic used by `config/runtime.exs`.
  Exercises truthiness coercion, trusted-key PEM-list loading, integer
  parsing, and the presence check that gates whether the runtime.exs
  block fires at all.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Installer.RuntimeConfig

  test "any_env_var_present? returns false on empty env" do
    refute RuntimeConfig.any_env_var_present?(%{})
    refute RuntimeConfig.any_env_var_present?(%{"UNRELATED" => "x"})
    refute RuntimeConfig.any_env_var_present?(%{"FLEZHA_PLUGIN_INSTALLER_ENABLED" => "true"})
  end

  test "any_env_var_present? returns true for each knob" do
    for var <- ~w(FLEZHA_PLUGIN_ROOT FLEZHA_PLUGIN_REQUIRE_SIGNATURE FLEZHA_PLUGIN_ALLOW_UNSIGNED
                  FLEZHA_PLUGIN_TRUSTED_KEYS FLEZHA_PLUGIN_MAX_PACKAGE_BYTES
                  FLEZHA_PLUGIN_MAX_SOURCE_FILES) do
      assert RuntimeConfig.any_env_var_present?(%{var => "x"}),
             "expected #{var} to register as present"
    end
  end

  test "resolve_enabled reads truthy env values" do
    assert RuntimeConfig.resolve_enabled(%{"FLEZHA_PLUGIN_INSTALLER_ENABLED" => "true"}, false)
    assert RuntimeConfig.resolve_enabled(%{"FLEZHA_PLUGIN_INSTALLER_ENABLED" => "1"}, false)
    assert RuntimeConfig.resolve_enabled(%{"FLEZHA_PLUGIN_INSTALLER_ENABLED" => "YES"}, false)
    refute RuntimeConfig.resolve_enabled(%{"FLEZHA_PLUGIN_INSTALLER_ENABLED" => "no"}, true)
    refute RuntimeConfig.resolve_enabled(%{"FLEZHA_PLUGIN_INSTALLER_ENABLED" => ""}, true)
  end

  test "resolve_enabled falls back to default when unset" do
    assert RuntimeConfig.resolve_enabled(%{}, true)
    refute RuntimeConfig.resolve_enabled(%{}, false)
  end

  test "resolve_plugin_installer overrides every knob from env" do
    base = [
      plugin_root: "/base/plugins",
      trusted_keys: [],
      require_signature: false,
      allow_unsigned: true,
      max_package_bytes: 1_000_000,
      max_source_files: 100
    ]

    env = %{
      "FLEZHA_PLUGIN_ROOT" => "/env/plugins",
      "FLEZHA_PLUGIN_REQUIRE_SIGNATURE" => "true",
      "FLEZHA_PLUGIN_ALLOW_UNSIGNED" => "false",
      "FLEZHA_PLUGIN_MAX_PACKAGE_BYTES" => "2000000",
      "FLEZHA_PLUGIN_MAX_SOURCE_FILES" => "250"
    }

    # Use a fake key_loader so we don't touch disk.
    result = RuntimeConfig.resolve_plugin_installer(env, base, fn _ -> "stub" end)

    assert result[:plugin_root] == "/env/plugins"
    assert result[:require_signature] == true
    assert result[:allow_unsigned] == false
    assert result[:max_package_bytes] == 2_000_000
    assert result[:max_source_files] == 250
  end

  test "resolve_plugin_installer preserves base for unset knobs" do
    base = [
      plugin_root: "/base/plugins",
      trusted_keys: ["pre-existing-key"],
      require_signature: true,
      allow_unsigned: false,
      max_package_bytes: 999,
      max_source_files: 7
    ]

    result = RuntimeConfig.resolve_plugin_installer(%{}, base, fn _ -> "should_not_call" end)

    assert result == base
  end

  test "resolve_plugin_installer parses trusted_keys csv and calls loader" do
    env = %{"FLEZHA_PLUGIN_TRUSTED_KEYS" => "/etc/key_a.pem,/etc/key_b.pem"}

    loader = fn
      "/etc/key_a.pem" -> "KEY_A_BYTES"
      "/etc/key_b.pem" -> "KEY_B_BYTES"
    end

    result = RuntimeConfig.resolve_plugin_installer(env, [trusted_keys: []], loader)
    assert result[:trusted_keys] == ["KEY_A_BYTES", "KEY_B_BYTES"]
  end

  test "resolve_plugin_installer handles empty trusted_keys env as no keys" do
    result =
      RuntimeConfig.resolve_plugin_installer(
        %{"FLEZHA_PLUGIN_TRUSTED_KEYS" => ""},
        [trusted_keys: ["base_key"]],
        fn _ -> flunk("loader should not be invoked") end
      )

    assert result[:trusted_keys] == []
  end

  test "resolve_plugin_installer trims whitespace in trusted_keys csv" do
    env = %{"FLEZHA_PLUGIN_TRUSTED_KEYS" => " /etc/a.pem , /etc/b.pem "}

    loader = fn path ->
      assert not String.starts_with?(path, " "), "path leaked whitespace: #{inspect(path)}"
      assert not String.ends_with?(path, " "), "path leaked whitespace: #{inspect(path)}"
      "LOADED_#{path}"
    end

    result = RuntimeConfig.resolve_plugin_installer(env, [], loader)
    assert length(result[:trusted_keys]) == 2
  end
end
