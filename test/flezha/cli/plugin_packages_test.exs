defmodule Flezha.Service.Cli.PluginPackagesTest do
  @moduledoc """
  What `flz plugin` says about packages: the installed list in text and JSON,
  the refused-package line under the provider list, an enable of a refused
  package's provider, and the controllers of a namespace that lack a provider.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  @moduletag :capture_log

  import ExUnit.CaptureIO

  alias Flezha.Service.Cli.Plugin, as: CliPlugin
  alias Flezha.TestHelpers.PluginPackage

  setup %{tmp: tmp} do
    root = Path.join(tmp, "plugins")
    on_exit(fn -> PluginPackage.unload!(root) end)

    refused =
      PluginPackage.sibling(Path.join(tmp, "flz_refused.fpz"), "flz_refused", [
        {:manifest, &put_in(&1.built_with.otp, "0")}
      ])

    PluginPackage.load!(root, [PluginPackage.example!(), refused])
    :ok
  end

  test "the installed list: one line per package in text, the rows in JSON" do
    text = capture_io(fn -> CliPlugin.list_installed_plugins(%{opts: []}) end)
    assert text =~ "flezha_example_plugin 0.1.0  loaded  providers: example_stamp"

    assert text =~
             "flz_refused 0.1.0  refused  a package loads on the OTP release it was built on " <>
               "and the two after it; the package was built on OTP 0"

    assert text =~ "(plugin.runtime_mismatch)"

    json = capture_io(fn -> CliPlugin.list_installed_plugins(%{opts: [output: "json"]}) end)
    rows = Map.new(Jason.decode!(json), &{&1["id"], &1})

    assert %{
             "status" => "loaded",
             "version" => "0.1.0",
             "error" => nil,
             "providers" => ["example_stamp"]
           } =
             rows["flezha_example_plugin"]

    assert %{
             "status" => "refused",
             "error" => %{"code" => "plugin.runtime_mismatch", "class" => "conflict"}
           } =
             rows["flz_refused"]
  end

  test "the provider list ends naming how many packages were refused" do
    out = capture_io(fn -> CliPlugin.list_providers(%{}) end)
    assert out =~ "example_stamp"
    refute out =~ "flz_refused_stamp"
    assert out =~ "1 plugin package(s) were refused at boot: flz plugin list --installed"
  end

  test "enabling a refused package's provider names the package and its refusal" do
    assert {:error, reason} = CliPlugin.enable_row(["p", "flz_refused_stamp", "{}", "{}"])
    error = Flezha.Error.from_reason(reason, "plugin.enable")
    assert error.code == "plugin.provider_unavailable"
    assert error.details["plugin"] == "flz_refused"
    assert error.details["code"] == "plugin.runtime_mismatch"

    assert {:error, {:unknown_provider, "nobody"}} =
             CliPlugin.enable_row(["p", "nobody", "{}", "{}"])
  end

  test "a controller of the namespace lacking the provider, or not answering, is named" do
    ghost = :"ghost@127.0.0.1"
    assert CliPlugin.lacking([], "example_stamp") == []
    assert CliPlugin.lacking([node()], "example_stamp") == []
    assert CliPlugin.lacking([node()], "flz_refused_stamp") == [node()]
    assert CliPlugin.lacking([ghost], "example_stamp") == [ghost]
  end
end
