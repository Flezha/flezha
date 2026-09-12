defmodule EmbeddedControllerTest do
  use ExUnit.Case, async: false

  # The application under test declares no supervision tree of its own. What
  # these assertions check is that depending on flezha was enough: the
  # controller booted with this application and reports itself ready.

  @ready_timeout_ms 60_000

  test "the controller tree came up with this application" do
    assert is_pid(Process.whereis(Flezha.Supervisor))
  end

  test "readiness reaches ready, with every precondition met" do
    assert {:ready, rows} = await_ready(@ready_timeout_ms)
    assert Enum.all?(rows, & &1.met)
  end

  test "/healthz answers 200 once the controller is ready" do
    await_ready(@ready_timeout_ms)
    port = Application.get_env(:flezha, :healthz_port, 9569)
    url = ~c"http://127.0.0.1:#{port}/healthz"

    assert {:ok, {{_, 200, _}, _headers, body}} = :httpc.request(:get, {url, []}, [], [])
    assert to_string(body) =~ ~s("status":"healthy")
  end

  # A controller answers `recovering` for a moment after start, while its hubs
  # replay. Poll the verdict rather than sleeping a guessed interval.
  defp await_ready(timeout_ms, waited \\ 0) do
    case EmbeddedController.status() do
      {:ready, _rows} = verdict ->
        verdict

      {phase, rows} when waited >= timeout_ms ->
        flunk("still #{phase} after #{timeout_ms}ms: #{inspect(Enum.reject(rows, & &1.met))}")

      {_phase, _rows} ->
        Process.sleep(200)
        await_ready(timeout_ms, waited + 200)
    end
  end
end
