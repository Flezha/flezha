defmodule Flezha.Service.Cli.PluginPackagesTest do
  @moduledoc """
  What `flz plugin` says about packages: the installed list in text and JSON,
  the refused-package line under the provider list, an enable of a refused
  package's provider, and the controllers of a namespace that lack a provider.
  """

  use FlezhaTest.Case, async: false

  @moduletag :capture_log

  import ExUnit.CaptureIO

  alias Flezha.Plugin.Providers
  alias Flezha.Service.Cli
  alias Flezha.TestHelpers.PluginPackage
  alias FlezhaTest.Helper.NodeTestHelper

  # Every clause reads the one boot, so the packages load once for the module.
  setup_all do
    tmp = NodeTestHelper.claim_tmp_dir("flz_cli_packages")
    root = Path.join(tmp, "plugins")
    plugins = Application.fetch_env(:flezha, :plugins)

    on_exit(fn ->
      PluginPackage.unload!(root)

      case plugins do
        {:ok, value} -> Application.put_env(:flezha, :plugins, value)
        :error -> Application.delete_env(:flezha, :plugins)
      end

      File.rm_rf!(tmp)
    end)

    refused =
      PluginPackage.sibling(Path.join(tmp, "flz_refused.fpz"), "flz_refused", [
        {:manifest, &put_in(&1.built_with.otp, "0")}
      ])

    PluginPackage.load!(root, [PluginPackage.example!(), refused])
    :ok
  end

  test "the installed list: one line per package in text, the rows in JSON" do
    text = capture_io(fn -> Cli.call("Fleet.installed_plugins", "[]", []) end)
    assert text =~ "flezha_example_plugin 0.1.0  loaded  providers: example_stamp"

    assert text =~
             "flz_refused 0.1.0  refused  a package loads on the OTP release it was built on " <>
               "and the two after it; the package was built on OTP 0"

    assert text =~ "(plugin.runtime_mismatch)"

    json = capture_io(fn -> Cli.call("Fleet.installed_plugins", "[]", output: "json") end)
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
    out = capture_io(fn -> Cli.call("Fleet.providers", "[]", []) end)
    assert out =~ "example_stamp"
    refute out =~ "flz_refused_stamp"
    assert out =~ "1 plugin package(s) were refused at boot: flz plugin list --installed"
  end

  test "enabling a refused package's provider names the package and its refusal" do
    assert {:error, reason} = Providers.resolve("flz_refused_stamp")
    error = Flezha.Error.from_reason(reason, "plugin.enable")
    assert error.code == "plugin.provider_unavailable"
    assert error.details["plugin"] == "flz_refused"
    assert error.details["code"] == "plugin.runtime_mismatch"

    assert {:error, {:unknown_provider, "nobody"}} = Providers.resolve("nobody")
  end

  test "a controller of the namespace lacking the provider, or not answering, is named" do
    ghost = :"ghost@127.0.0.1"
    assert Providers.lacking([], "example_stamp") == []
    assert Providers.lacking([node()], "example_stamp") == []
    assert Providers.lacking([node()], "flz_refused_stamp") == [node()]
    assert Providers.lacking([ghost], "example_stamp") == [ghost]
  end
end
