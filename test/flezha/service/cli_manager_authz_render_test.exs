defmodule Flezha.Service.CliManagerAuthzRenderTest do
  @moduledoc """
  finish-platform-authn-authz §8.1: the `flz auth|token|role|namespace` verb
  families as the operator sees them — thin renderers over the authorized
  wrappers, driven end to end against live auth stores.
  """
  # async: false: a shared on-disk root.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.{AuthState, Bootstrap, Config, TokenStore}

  alias Flezha.Service.CliManager

  setup do
    _ = FlezhaTest.Helper.AuthTestHelper.setup_auth_stores(prefix: "flz_authz_render")
    :ok
  end

  test "the whole verb-family flow: bootstrap → namespace → role → token → whoami" do
    # `flz auth bootstrap <token>` — claims and prints the minted admin token.
    :ok = Bootstrap.check_and_write()
    raw_bootstrap = File.read!(Config.bootstrap_token_path()) |> String.trim()

    bootstrap_out = capture_io(fn -> CliManager.authz_render_bootstrap(raw_bootstrap) end)
    assert bootstrap_out =~ "bootstrap claimed"
    assert bootstrap_out =~ "shown once"
    [admin_token] = Regex.run(~r/flzt_[a-z0-9]+/, bootstrap_out)

    # `flz namespace create` with quota overrides (the CLI's `--X` JSON).
    create_out =
      capture_io(fn ->
        CliManager.authz_render_namespace_create(
          "render-ns",
          ~s({"max_runners": "2"}),
          token: admin_token
        )
      end)

    assert create_out =~ "namespace render-ns created"
    assert create_out =~ "max_runners: 2"

    # `flz namespace show` / `set-quota`.
    show_out =
      capture_io(fn ->
        CliManager.authz_render_namespace_show("render-ns", token: admin_token)
      end)

    assert show_out =~ "namespace: render-ns"

    set_out =
      capture_io(fn ->
        CliManager.authz_render_namespace_set_quota(
          "render-ns",
          ~s({"max_operatives": 7}),
          token: admin_token
        )
      end)

    assert set_out =~ "max_operatives: 7"

    # `flz role bind` / `list`.
    bind_out =
      capture_io(fn ->
        CliManager.authz_render_role_bind("carol", "operator", "render-ns", token: admin_token)
      end)

    assert bind_out =~ "bound carol as operator@render-ns"

    role_list_out = capture_io(fn -> CliManager.authz_render_role_list(token: admin_token) end)
    assert role_list_out =~ "carol  operator@render-ns"

    # `flz token issue` / `whoami` / `list` / `revoke`.
    issue_out =
      capture_io(fn ->
        CliManager.authz_render_token_issue("carol", "render-ns", "operator", 3_600,
          token: admin_token
        )
      end)

    assert issue_out =~ "token issued for carol"
    [carol_token] = Regex.run(~r/flzt_[a-z0-9]+/, issue_out)
    [_, token_id] = Regex.run(~r/id:\s+([0-9a-f]+)/, issue_out)

    whoami_out = capture_io(fn -> CliManager.authz_render_whoami(token: carol_token) end)
    assert whoami_out =~ "principal: carol"
    assert whoami_out =~ "namespace: render-ns"

    list_out = capture_io(fn -> CliManager.authz_render_token_list(token: admin_token) end)
    assert list_out =~ "carol  operator@render-ns  active"

    revoke_out =
      capture_io(fn -> CliManager.authz_render_token_revoke(token_id, token: admin_token) end)

    assert revoke_out =~ "revoked token"
    assert {:error, :token_revoked} = TokenStore.verify(carol_token)

    # `flz role unbind` + `flz namespace delete` close the loop.
    capture_io(fn -> CliManager.authz_render_role_unbind("carol", token: admin_token) end)

    delete_out =
      capture_io(fn ->
        CliManager.authz_render_namespace_delete("render-ns", token: admin_token)
      end)

    assert delete_out =~ "namespace render-ns deleted"
  end

  test "a gated verb without identity renders the token requirement" do
    :ok = AuthState.set_enabled()

    out =
      capture_io(:stderr, fn ->
        assert catch_exit(CliManager.authz_render_token_list([])) == :cli_error
      end)

    assert out =~ "--token"
  end

  test "an unknown role string is refused by name" do
    :ok = AuthState.set_enabled()

    out =
      capture_io(:stderr, fn ->
        assert catch_exit(CliManager.authz_render_role_bind("dave", "superuser", "render-ns", [])) ==
                 :cli_error
      end)

    assert out =~ "unknown_role"
  end
end
