defmodule Flezha.Service.Cli.ContextTest do
  @moduledoc """
  The one bad-token rule as `flz` keeps it (`Flezha.Service.Admin.context/2`):
  a token that does not verify is refused with its own code before the verb
  runs, even while authorization is open, and an empty one is no token, as
  `flz` reads an empty `--token`.
  """
  # async: false: the auth stores are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_cli_context", enabled: false}],
    async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ
  alias Flezha.Service.Cli

  setup do
    assert AuthZ.enforcement_posture() == :open
    :ok
  end

  # Fails when: a token that does not verify is taken for no token at all.
  test "a token that does not verify is refused under the open posture" do
    stderr =
      capture_io(:stderr, fn ->
        assert catch_exit(list_namespaces(token: "flzt_not_a_real_token")) == {:shutdown, 4}
      end)

    assert stderr =~ "(authz.token_unknown)"
  end

  test "an empty token is no token" do
    capture_io(fn -> assert list_namespaces(token: "") == :ok end)
  end

  defp list_namespaces(opts), do: Cli.call("Relay.list_namespaces", "[]", opts)
end
