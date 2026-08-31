defmodule Flezha.Plugin.Installer.ReleaseCliSystemTest do
  @moduledoc """
  System-level test: boots the built release as a daemon with the
  installable-plugin feature flag on, then exercises the Rust `flz` CLI
  subprocess end-to-end (flz plugin install/list/info/uninstall).

  Tagged `:system` and excluded from the default `mix test` run because:

    * it requires `mix release` + `cargo build -p flz` to have been run first,
    * the daemon it starts holds ports and files for the duration,
    * a single test takes ~30s including daemon boot.

  Run with:

      mix release --overwrite
      mix test --only system

  The test skips with a clear message when the release binaries are not
  on disk so `mix test --only system` on a clean tree fails loudly rather
  than silently.
  """

  use ExUnit.Case

  @moduletag :system
  @moduletag timeout: 180_000

  alias FlezhaTest.Helper.NodeTestHelper

  @release_dir Path.expand("../../../../_build/dev/rel/flezha", __DIR__)
  @flz Path.join(@release_dir, "bin/flz")
  @flzvm Path.join(@release_dir, "bin/flzvm")

  setup_all do
    unless File.regular?(@flz) and File.regular?(@flzvm) do
      # Emit a directly visible failure instead of silently skipping so
      # runners running `--only system` on an unbuilt tree get a real
      # error message pointing at the fix.
      flunk("""
      Release binaries not found. Run:
          mix release --overwrite
      then re-run `mix test --only system`.
      Expected:
          #{@flz}
          #{@flzvm}
      """)
    end

    tmp =
      Path.join(System.tmp_dir!(), "flz_release_sys_test_#{System.unique_integer([:positive])}")

    plugin_root = Path.join(tmp, "plugins")
    data_dir = Path.join(tmp, "recovery")
    socket_dir = Path.join(tmp, "sockets")
    log_path = Path.join(tmp, "flezha.log")
    File.mkdir_p!(tmp)
    File.mkdir_p!(plugin_root)
    File.mkdir_p!(data_dir)
    File.mkdir_p!(socket_dir)

    env =
      [
        {"RELEASE_NODE", "flezha_sys_test_#{System.unique_integer([:positive])}@127.0.0.1"},
        {"RELEASE_COOKIE", "flz_sys_test_cookie"},
        {"FLEZHA_PLUGIN_INSTALLER_ENABLED", "true"},
        {"FLEZHA_PLUGIN_ROOT", plugin_root},
        {"FLEZHA_DATA_DIR", data_dir},
        {"FLEZHA_SOCKET_FILES_PATH", socket_dir <> "/"},
        {"FLEZHA_LOG_PATH", log_path},
        {"FLEZHA_TRANSPORT", "uds"},
        {"FLEZHA_TRANSPORTS", "uds"},
        {"FLEZHA_TESTNET_ENABLED", "false"}
      ]

    {_, 0} = System.cmd(@flzvm, ["daemon"], env: env, stderr_to_stdout: true)

    wait_until_ready!(env)

    on_exit(fn ->
      # Best-effort shutdown — if the daemon already exited, `stop` returns
      # non-zero but that's fine.
      _ = System.cmd(@flzvm, ["stop"], env: env, stderr_to_stdout: true)
      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp, env: env, plugin_root: plugin_root}
  end

  test "flz plugin install/list/info/uninstall full lifecycle", %{tmp: tmp, env: env} do
    unique = System.unique_integer([:positive])
    id = "rel_cli_#{unique}"
    fpz = build_fpz!(tmp, id, "1.0.0", ["RelCli.Sys#{unique}"])

    # install
    {out, 0} = System.cmd(@flz, ["plugin", "install", fpz], env: env, stderr_to_stdout: true)
    assert out =~ "Installed plugin #{id}@1.0.0"
    assert out =~ "status: disabled"

    # list --installed (text)
    {list_out, 0} =
      System.cmd(@flz, ["plugin", "list", "--installed"], env: env, stderr_to_stdout: true)

    assert list_out =~ "#{id}@1.0.0"
    assert list_out =~ "(disabled)"

    # list --installed --json
    {json_out, 0} =
      System.cmd(@flz, ["plugin", "list", "--installed", "--json"],
        env: env,
        stderr_to_stdout: true
      )

    decoded = Jason.decode!(last_json_line(json_out))
    assert Enum.any?(decoded, fn p -> p["id"] == id end)

    # info
    {info_out, 0} = System.cmd(@flz, ["plugin", "info", id], env: env, stderr_to_stdout: true)
    assert info_out =~ "#{id}@1.0.0"
    assert info_out =~ "Status: disabled"

    # uninstall
    {un_out, 0} =
      System.cmd(@flz, ["plugin", "uninstall", id], env: env, stderr_to_stdout: true)

    assert un_out =~ "Uninstalled plugin #{id}"

    # info should now say not-installed
    {after_info, 0} =
      System.cmd(@flz, ["plugin", "info", id], env: env, stderr_to_stdout: true)

    assert after_info =~ "not installed"
  end

  ## Helpers

  defp wait_until_ready!(env, deadline_ms \\ 60_000) do
    if NodeTestHelper.wait_until(fn -> ready?(env) end, deadline_ms, 250),
      do: :ok,
      else: flunk("daemon did not become ready within #{deadline_ms}ms")
  end

  # The daemon answers `pid`, and its Boot.Loader reports ready over rpc.
  defp ready?(env) do
    with {_, 0} <- System.cmd(@flzvm, ["pid"], env: env, stderr_to_stdout: true),
         {out, 0} <-
           System.cmd(
             @flzvm,
             ["rpc", "GenServer.call(Flezha.Boot.Loader, :boot_ready?, 5000) |> IO.puts()"],
             env: env,
             stderr_to_stdout: true
           ) do
      out =~ "true"
    else
      _ -> false
    end
  end

  defp build_fpz!(tmp, id, version, mods) do
    fpz = Path.join(tmp, "#{id}-#{version}.fpz")

    [first_mod | _] = mods
    src_filename = Macro.underscore(first_mod) <> ".ex"

    src_body =
      Enum.map_join(mods, "\n", fn m -> "defmodule #{m} do\n  def hi, do: :ok\nend" end)

    manifest = """
    %{
      id: "#{id}",
      version: "#{version}",
      description: "sys test",
      author: "sys",
      modules: [#{Enum.join(mods, ", ")}],
      provides: [
        %{
          id: "main",
          provider: #{first_mod},
          subscriptions: [{:on_input, []}],
          default_opts: %{},
          scope: :operative
        }
      ],
      dependencies: []
    }
    """

    manifest_hash = sha256(manifest)
    src_hash = sha256(src_body)
    checksums = "#{manifest_hash}  manifest.exs\n#{src_hash}  src/#{src_filename}\n"

    entries = [
      {~c"manifest.exs", manifest},
      {String.to_charlist("src/#{src_filename}"), src_body},
      {~c"CHECKSUMS.sha256", checksums}
    ]

    :ok = :erl_tar.create(String.to_charlist(fpz), entries, [:compressed])
    fpz
  end

  defp sha256(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)

  # `mix rpc` output contains operational noise on some lines; the final JSON
  # blob is always on its own line. Pick the last non-empty line.
  defp last_json_line(output) do
    output
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> List.last()
  end
end
