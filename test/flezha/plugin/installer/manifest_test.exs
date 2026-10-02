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

  @running %{otp: "29", elixir: "1.20.2", flezha: "0.1.0"}

  defp check(m, built, running \\ @running),
    do: Manifest.check_runtime(%{m | built_with: Map.merge(@running, built)}, running)

  defp rule({:error, {:runtime_mismatch, %{rule: rule}}}), do: rule

  test "a package built on a newer OTP is refused, naming both runtimes and the rule",
       %{manifest: m} do
    assert {:error, {:runtime_mismatch, %{package_otp: "29", otp: "28"}} = reason} =
             check(m, %{otp: "29"}, %{@running | otp: "28"})

    error = Flezha.Error.from_reason(reason, nil)
    assert error.code == "plugin.runtime_mismatch"
    assert error.details["rule"] == "otp_newer"

    assert error.message =~
             ~r/^a package built on a newer OTP release .+; the package was built on OTP 29,/

    assert error.message =~ "this controller runs OTP 28"
    assert error.hint =~ "flezha/plugin-builder:"
  end

  test "a package built on the running OTP or the two releases before it loads", %{manifest: m} do
    for otp <- ~w(29 28 27), do: assert(:ok = check(m, %{otp: otp}))
  end

  test "a package built three OTP releases back is refused", %{manifest: m} do
    assert rule(check(m, %{otp: "26"})) == "otp_too_old"
  end

  test "a package built on an older Elixir minor loads with a note naming both versions",
       %{manifest: m} do
    assert {:ok, {:elixir_minor, "1.19.4", "1.20.2"}} = check(m, %{elixir: "1.19.4"})
  end

  test "a package built on an older patch of the running Elixir minor loads without a note",
       %{manifest: m} do
    assert :ok = check(m, %{elixir: "1.20.0"})
    assert :ok = check(m, %{elixir: "1.20.3"})
  end

  test "a package built on a newer Elixir minor or another major is refused", %{manifest: m} do
    assert rule(check(m, %{elixir: "1.21.0"})) == "elixir_newer"
    assert rule(check(m, %{elixir: "2.0.0"})) == "elixir_major"
    assert rule(check(m, %{elixir: "1.20.2"}, %{@running | elixir: "2.0.0"})) == "elixir_major"
  end

  test "a package built for an older patch of the same Flezha line loads", %{manifest: m} do
    assert :ok = check(m, %{flezha: "0.1.0"}, %{@running | flezha: "0.1.3"})
  end

  test "before 1.0 a Flezha minor needs a rebuild, and an older Flezha never loads",
       %{manifest: m} do
    assert rule(check(m, %{flezha: "0.1.0"}, %{@running | flezha: "0.2.0"})) == "flezha_line"
    assert rule(check(m, %{flezha: "0.1.3"}, %{@running | flezha: "0.1.0"})) == "flezha_line"
  end

  test "from 1.0 a Flezha major holds", %{manifest: m} do
    assert :ok = check(m, %{flezha: "1.2.0"}, %{@running | flezha: "1.5.1"})
    assert rule(check(m, %{flezha: "1.2.0"}, %{@running | flezha: "2.0.0"})) == "flezha_line"
    assert rule(check(m, %{flezha: "1.2.0"}, %{@running | flezha: "1.1.0"})) == "flezha_line"
  end

  test "each refusal's message leads with its rule's sentence", %{manifest: m} do
    for {built, running, lead} <- [
          {%{otp: "29"}, %{otp: "28"}, "a package built on a newer OTP release"},
          {%{otp: "26"}, %{}, "a package loads on the OTP release it was built on"},
          {%{elixir: "1.21.0"}, %{}, "a package built on a newer Elixir"},
          {%{elixir: "2.0.0"}, %{}, "a package loads only on the Elixir major"},
          {%{flezha: "0.1.0"}, %{flezha: "0.2.0"}, "a package loads on the Flezha version"}
        ] do
      assert {:error, reason} = check(m, built, Map.merge(@running, running))
      assert String.starts_with?(Flezha.Error.from_reason(reason, nil).message, lead)
    end
  end
end
