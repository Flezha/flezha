defmodule FlezhaTest.Helper.RelayTestHelper do
  alias Flezha.Service.RelayManager

  @default_timeout_ms 2_000

  @doc """
  Send `msg` to `relay_id` and block until a `{:message_forward, _}` reply
  arrives or the timeout fires. The default per-call timeout is 2 s; QUIC
  callers should pass `:quic` as the `timeout` opt to pick up the
  `:quic_test_timeout_ms` Application env (default 5 s) — the QUIC stack
  is slower than UDS on first-message paths.

  ## Options

    * `:timeout` — `non_neg_integer()` to override the default in ms, or
      `:quic` to read `Application.get_env(:flezha, :quic_test_timeout_ms, 5_000)`.
  """
  def send_sync_io(relay_id, namespace, msg, metadata \\ %{}, opts \\ []) do
    RelayManager.send_io(relay_id, namespace, msg, metadata)

    timeout = resolve_timeout(Keyword.get(opts, :timeout))

    receive do
      {:message_forward, msg} -> msg
    after
      timeout -> raise "Timeout waiting for message (after #{timeout}ms)"
    end
  end

  defp resolve_timeout(:quic),
    do: Application.get_env(:flezha, :quic_test_timeout_ms, 5_000)

  defp resolve_timeout(ms) when is_integer(ms) and ms > 0, do: ms
  defp resolve_timeout(nil), do: @default_timeout_ms
end
