defmodule FlezhaTest.System.PluginPackageReleaseTest do
  @moduledoc """
  A plugin package through the released binaries: packaged with the task,
  installed by `flz plugin install` into a release that has never started,
  loaded when the controller starts, and every status of `flz plugin list
  --installed` across restarts, with the audit log recording what each boot
  loaded and refused. The VM those verbs run in keeps OTP's boot reports off
  their stderr.

  Builds the prod release of this tree into a scratch directory (`cargo build
  -p flz --release`, then `MIX_ENV=prod mix release --path`), so it never reads
  a release left in `_build/prod/rel` by an earlier version. Tagged `:system`;
  run with `mix test --include system test/system/plugin_package_release_test.exs`.
  """

  # Proves the controller's packaging and drives no runner; the capability is
  # one every SDK carries, so no SDK's deferral skips it.
  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  @moduletag timeout: 600_000

  alias Flezha.TestHelpers.PluginPackage
  alias FlezhaTest.Helper.NodeTestHelper

  setup_all do
    tmp = Path.join(System.tmp_dir!(), "flzpkg#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)
    release = build_release!(Path.join(tmp, "rel"))

    dirs =
      for name <- ~w(plugins recovery sockets state audit), into: %{} do
        path = Path.join(tmp, name)
        File.mkdir_p!(path)
        {name, path}
      end

    env = [
      {"FLEZHA_NODE_NAME", "flzpkg#{System.unique_integer([:positive])}"},
      {"RELEASE_COOKIE", "flz_pkg_release_test"},
      {"FLEZHA_PLUGIN_ROOT", dirs["plugins"]},
      {"FLEZHA_DATA_DIR", dirs["recovery"]},
      {"FLEZHA_SOCKET_FILES_PATH", dirs["sockets"] <> "/"},
      {"FLEZHA_STATE_DIR", dirs["state"]},
      {"FLEZHA_LOG_PATH", Path.join(tmp, "flezha.log")},
      {"FLEZHA_AUDIT_ENABLED", "true"},
      {"FLEZHA_AUDIT_DIR", dirs["audit"]},
      {"FLEZHA_AUTHZ_ENABLED", "false"},
      {"FLEZHA_DURABILITY_BACKEND", "none"},
      {"FLEZHA_TRANSPORT", "uds"},
      {"FLEZHA_TRANSPORTS", "uds"},
      {"FLEZHA_TESTNET_ENABLED", "false"}
    ]

    ctx = %{tmp: tmp, env: env, root: dirs["plugins"], audit: dirs["audit"], release: release}

    on_exit(fn ->
      stop!(ctx)
      File.rm_rf!(tmp)
    end)

    {:ok, ctx}
  end

  test "install before the first start, load at boot, and every status across restarts", ctx do
    example = PluginPackage.example!()

    # Installed into a release that has never started: no controller to reach.
    {out, 0} = flz(ctx, ["plugin", "install", example])
    assert out =~ "Installed flezha_example_plugin 0.1.0 (unsigned) into #{ctx.root}"
    assert out =~ "Restart the controller to load it."
    assert File.regular?(Path.join(ctx.root, "flezha_example_plugin.fpz"))

    start!(ctx)

    assert [%{"id" => "flezha_example_plugin", "status" => "loaded", "error" => nil}] =
             installed(ctx)

    {providers, 0} = flz(ctx, ["plugin", "list", "--providers"])
    assert providers =~ "example_stamp"
    {help, 0} = flz(ctx, ["plugin", "help", "example_stamp"])
    assert help =~ "path"

    # The release protocol dispatches the package's struct, and still its own.
    assert rpc(ctx, ~s|IO.puts(to_string(struct(StampLib.Stamp, text: "x")))|) =~ "[x]"
    assert rpc(ctx, ~s|IO.puts(to_string(:a) <> to_string(1))|) =~ "a1"

    assert rpc(ctx, """
           Flezha.Supervisor
           |> Supervisor.which_children()
           |> Enum.any?(&(elem(&1, 0) == FlezhaExamplePlugin.Stamp.WorkerSup))
           |> IO.puts()
           """) =~ "true"

    for beam <- Path.wildcard(Path.join(ctx.release, "releases/*/consolidated/*.beam")) do
      {:ok, _, chunks} = :beam_lib.all_chunks(String.to_charlist(beam))
      assert List.keymember?(chunks, ~c"Dbgi", 0), "#{beam} carries no debug info"
    end

    # A package bundling another version of a release library, placed the way
    # a derived image places one, and a new version of the example: both wait
    # for the restart.
    conflicting =
      PluginPackage.sibling(Path.join(ctx.tmp, "flz_conflict.fpz"), "flz_conflict", [
        {:put, "lib/jason-0.0.1/ebin/jason.app",
         :io_lib.format(~c"~p.~n", [{:application, :jason, [vsn: ~c"0.0.1", modules: []]}])},
        {:manifest, &%{&1 | apps: &1.apps ++ [{:jason, "0.0.1"}]}}
      ])

    File.cp!(conflicting, Path.join(ctx.root, "flz_conflict.fpz"))

    newer =
      PluginPackage.rewrite(example, Path.join(ctx.tmp, "newer.fpz"), [
        {:app_version, :flezha_example_plugin, "0.2.0"}
      ])

    {_, 0} = flz(ctx, ["plugin", "install", newer])

    rows = Map.new(installed(ctx), &{&1["id"], &1})
    assert %{"status" => "pending", "version" => "0.1.0"} = rows["flz_conflict"]

    assert %{"status" => "pending", "version" => "0.2.0", "loaded_version" => "0.1.0"} =
             rows["flezha_example_plugin"]

    restart!(ctx)

    rows = Map.new(installed(ctx), &{&1["id"], &1})
    assert %{"status" => "loaded", "version" => "0.2.0"} = rows["flezha_example_plugin"]

    assert %{"status" => "refused", "error" => %{"code" => "plugin.library_conflict"}} =
             rows["flz_conflict"]

    {refusal, code} =
      flz(ctx, ["plugin", "enable", "c", "flz_conflict_stamp", "--relay_id", "op"])

    assert code != 0
    assert refusal =~ "(plugin.provider_unavailable)"

    {_, 0} = flz(ctx, ["plugin", "uninstall", "flezha_example_plugin"])

    assert %{"status" => "removed"} =
             Map.new(installed(ctx), &{&1["id"], &1})["flezha_example_plugin"]

    restart!(ctx)
    refute Enum.any?(installed(ctx), &(&1["id"] == "flezha_example_plugin"))

    audit = File.read!(Path.join(ctx.audit, "audit-current.jsonl"))
    assert audit =~ ~s|"source":"flezha_plugin_packages"|
    assert audit =~ ~s|"name":"loaded"|
    assert audit =~ ~s|"name":"refused"|
    assert audit =~ "plugin.library_conflict"
  end

  test "the VM a plugin verb runs in prints no boot report, and still its warnings", ctx do
    # `started_applications/0` returns once the application controller has
    # logged the report it makes after starting `:logger`, and `flush/0` once
    # the handler has written it: a report the boot let through is in `out`.
    {out, 0} =
      flzvm(ctx, [
        "eval",
        ~s|Application.started_applications(); require Logger; Logger.warning("eval warns"); Logger.flush(); IO.puts("answered")|
      ])

    refute out =~ "Application logger started"
    assert out =~ "[warning] eval warns"
    assert out =~ "answered"
  end

  # The release `flz` copies the release-built binary, so both are built from
  # the tree under test; `_build/prod/rel` is left alone.
  defp build_release!(path) do
    for {cmd, args, env} <- [
          {"cargo", ~w(build -p flz --release), []},
          {"mix", ~w(release flezha --overwrite --quiet --path) ++ [path], [{"MIX_ENV", "prod"}]}
        ] do
      {out, status} = System.cmd(cmd, args, env: env, stderr_to_stdout: true)
      if status != 0, do: flunk("#{cmd} #{Enum.join(args, " ")} failed (#{status}):\n#{out}")
    end

    path
  end

  defp flz(ctx, args), do: run(ctx, "flz", args)
  defp flzvm(ctx, args), do: run(ctx, "flzvm", args)

  # Run from the scratch directory: every path the controller does not get from
  # `env` resolves against its working directory, which is the repository's
  # own `app_data` otherwise.
  defp run(ctx, bin, args) do
    System.cmd(Path.join(ctx.release, "bin/#{bin}"), args,
      cd: ctx.tmp,
      env: ctx.env,
      stderr_to_stdout: true
    )
  end

  defp rpc(ctx, expression) do
    {out, 0} = flzvm(ctx, ["rpc", expression])
    out
  end

  defp installed(ctx) do
    {out, 0} = flz(ctx, ["plugin", "list", "--installed", "--json"])
    out |> String.split("\n", trim: true) |> List.last() |> Jason.decode!()
  end

  defp start!(ctx) do
    {_, 0} = flzvm(ctx, ["daemon"])

    assert NodeTestHelper.wait_until(
             fn -> match?({_, 0}, flz(ctx, ["boot", "await", "--timeout", "0"])) end,
             120_000,
             500
           ),
           "the controller did not report ready"
  end

  defp restart!(ctx) do
    stop!(ctx)
    start!(ctx)
  end

  defp stop!(ctx) do
    {_, _} = flzvm(ctx, ["stop"])

    assert NodeTestHelper.wait_until(
             fn -> not match?({_, 0}, flzvm(ctx, ["pid"])) end,
             60_000,
             500
           ),
           "the controller did not stop"
  end
end
