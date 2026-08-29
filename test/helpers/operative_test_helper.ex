defmodule FlezhaTest.Helper.OperativeTestHelper do
  alias Flezha.Service.OperativeManager

  @default_timeout_ms 2_000
  @ingest_retry_ms 50

  @doc """
  Send `msg` to `operative_id` and block until a `{:message_forward, _}` reply
  arrives or the timeout fires. The default per-call timeout is 2 s; QUIC
  callers should pass `:quic` as the `timeout` opt to pick up the
  `:quic_test_timeout_ms` Application env (default 5 s) — the QUIC stack
  is slower than UDS on first-message paths.

  Delivery rides `send_io_confirming/5`, never the `send_io/4` cast. An
  operative whose runner-side bind has not finished answers a retryable
  verdict, and a cast drops that input with nothing to re-drive it — the
  first input after a start is exactly when that happens. Retrying until the
  ingest is ACCEPTED keeps the input landing exactly once, so a caller
  asserting exact accumulated state stays valid; re-sending blind would not.

  ## Options

    * `:timeout` — `non_neg_integer()` to override the default in ms, or
      `:quic` to read `Application.get_env(:flezha, :quic_test_timeout_ms, 5_000)`.
  """
  def send_sync_io(operative_id, namespace, msg, metadata \\ %{}, opts \\ []) do
    timeout = resolve_timeout(Keyword.get(opts, :timeout))
    deadline = System.monotonic_time(:millisecond) + timeout

    ingest_until_accepted(operative_id, namespace, msg, metadata, deadline)

    receive do
      {:message_forward, reply} -> reply
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        raise "Timeout waiting for message (after #{timeout}ms)"
    end
  end

  defp ingest_until_accepted(operative_id, namespace, msg, metadata, deadline) do
    accepted? =
      FlezhaTest.Helper.NodeTestHelper.wait_until(
        fn ->
          remaining = max(deadline - System.monotonic_time(:millisecond), 1)

          case OperativeManager.send_io_confirming(
                 operative_id,
                 namespace,
                 msg,
                 metadata,
                 remaining
               ) do
            {:error, _retryable} -> false
            _accepted -> true
          end
        end,
        max(deadline - System.monotonic_time(:millisecond), 0),
        @ingest_retry_ms
      )

    if accepted?, do: :ok, else: raise("Operative never accepted the input before the timeout")
  end

  defp resolve_timeout(:quic),
    do: Application.get_env(:flezha, :quic_test_timeout_ms, 5_000)

  defp resolve_timeout(ms) when is_integer(ms) and ms > 0, do: ms
  defp resolve_timeout(nil), do: @default_timeout_ms
end
