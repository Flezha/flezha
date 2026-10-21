defmodule Flezha.Service.Cli.TokenOnStdinTest do
  @moduledoc """
  `flz` hands the caller's token to `flzvm rpc` on stdin, never on its command
  line, and the request's options list reads it there, on the controller,
  through the caller's group leader. The CLI's own tests answer with a shim, so
  only this drive sees the read come back: it runs the `elixir --rpc-eval` that
  `flzvm rpc` runs against this suite's node, with a token piped in, and
  evaluates the read `crates/cli/src/rpc.rs` writes into every request.
  """
  use ExUnit.Case, async: true

  @rpc_rs Path.expand("../../../crates/cli/src/rpc.rs", __DIR__)

  @tag timeout: 20_000
  test "the read every request ends with gives back exactly the token piped in" do
    assert Node.alive?(), "the suite's node is not distributed, so no rpc can reach it"

    elixir = System.find_executable("elixir") || flunk("no elixir on PATH")
    id = System.unique_integer([:positive])
    # A second line and a trailing newline: the read must end at EOF only.
    token = ~s|flzt_a"b\n\#{x}\n|

    pipe = [~s|printf '%s' "$TOKEN" \| "$0" "$@"|, elixir]

    args = [
      "--hidden",
      "--cookie",
      to_string(Node.get_cookie()),
      "--name",
      "token_on_stdin_#{id}@127.0.0.1",
      "--rpc-eval",
      to_string(node()),
      "IO.write(inspect(#{cli_token_read()}))"
    ]

    {out, status} = System.cmd("sh", ["-c" | pipe ++ args], env: [{"TOKEN", token}])

    assert {status, out} == {0, inspect(token)}
  end

  defp cli_token_read do
    case Regex.run(~r/"token: (IO\.read\(:eof\))"/, File.read!(@rpc_rs)) do
      [_, read] -> read
      nil -> flunk("crates/cli/src/rpc.rs no longer ends a request with `token: IO.read(:eof)`")
    end
  end
end
