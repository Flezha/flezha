defmodule Flezha.Plugin.Installer.ManifestTest do
  @moduledoc """
  "Manifest schema" and "A package SHALL load only on the runtime line it was
  built with", on the manifest the task writes for the example plugin.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  alias Flezha.Plugin.Installer.Manifest
  alias Flezha.Plugin.Installer.Package
  alias Flezha.TestHelpers.PluginPackage

  @opts [max_package_bytes: 50_000_000, require_signature: false, trusted_keys: []]

  setup %{tmp: tmp} do
    dir = Path.join(tmp, "x")
    {:ok, verified} = Package.verify(PluginPackage.example!(), dir, @opts)
    {:ok, manifest: verified.manifest, apps: verified.apps}
  end

  test "the manifest the task writes parses back and validates", %{manifest: m, apps: apps} do
    assert {:ok, ^m} = Manifest.parse(Manifest.render(m))
    assert :ok = Manifest.validate(m, apps)
    assert m.providers == %{"example_stamp" => FlezhaExamplePlugin.Stamp}
    assert m.built_with == Manifest.runtime()
  end

  test "a missing required field is refused, naming it", %{manifest: m, apps: apps} do
    assert {:error, {:package_invalid, :missing_field, %{field: :built_with}}} =
             Manifest.validate(Map.delete(m, :built_with), apps)
  end

  test "an invalid id or a non-semver version is refused, naming it", %{manifest: m, apps: apps} do
    assert {:error, {:package_invalid, :invalid_field, %{field: :id, value: "My-Plugin"}}} =
             Manifest.validate(%{m | id: "My-Plugin"}, apps)

    assert {:error, {:package_invalid, :invalid_field, %{field: :version, value: "beta-2"}}} =
             Manifest.validate(%{m | version: "beta-2"}, apps)
  end

  test "a provider whose module the plugin application does not list is refused",
       %{manifest: m, apps: apps} do
    m = %{m | providers: %{"example_stamp" => Some.Other}}

    assert {:error,
            {:package_invalid, :provider_not_in_app,
             %{provider: "example_stamp", module: Some.Other}}} =
             Manifest.validate(m, apps)
  end

  test "apps and lib/ must be the same set", %{manifest: m, apps: apps} do
    assert {:error, {:package_invalid, :apps_mismatch, %{application: :ghost}}} =
             Manifest.validate(%{m | apps: m.apps ++ [{:ghost, "1.0.0"}]}, apps)
  end

  test "a manifest containing code is refused without evaluating it" do
    assert {:error, {:package_invalid, :manifest_not_literal, _}} =
             Manifest.parse(~s|%{id: send(self(), :evaluated), version: System.get_env("FOO")}|)

    refute_received :evaluated
  end

  test "a package built on another OTP is refused, naming both runtimes", %{manifest: m} do
    runtime = %{otp: "28", elixir: m.built_with.elixir, flezha: m.built_with.flezha}
    m = put_in(m.built_with.otp, "29")

    assert {:error, {:runtime_mismatch, %{package_otp: "29", otp: "28"}} = reason} =
             Manifest.check_runtime(m, runtime)

    error = Flezha.Error.from_reason(reason, nil)
    assert error.code == "plugin.runtime_mismatch"
    assert error.hint =~ "flezha/plugin-builder:"
  end

  test "a package built for an older patch of the same Flezha line loads", %{manifest: m} do
    runtime = %{otp: m.built_with.otp, elixir: m.built_with.elixir, flezha: "0.1.3"}
    assert :ok = Manifest.check_runtime(put_in(m.built_with.flezha, "0.1.0"), runtime)
    assert {:error, _} = Manifest.check_runtime(put_in(m.built_with.flezha, "0.2.0"), runtime)
  end
end
