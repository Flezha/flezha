defmodule FlezhaTest.Helper.RelayTestHelper do
  @moduledoc """
  Input delivery a test can wait on: `cast_io/4` is fire-and-forget, so these
  retry until the relay is bound and the ingest is accepted.
  """

  alias Flezha.Plugin.Host
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.NodeTestHelper

  @default_timeout_ms 10_000
  @ingest_retry_ms 50

  @doc """
  Cast `msg` to `relay_id` as a source's `:fire_and_forget` inject does:
  `:ok`, or the lookup's error for a relay that cannot be found.
  """
  def cast_io(relay_id, namespace, msg, metadata) do
    with {:ok, pid} <- RelayManager.get_relay_pid(namespace, relay_id),
         do: Host.inject(pid, msg, metadata, :fire_and_forget)
  end

  @doc """
  Send `msg` to `relay_id` and block until a `{:message_forward, _}` reply
  arrives or the timeout fires, by default 10 s: a first input on a loaded
  machine, or over QUIC, can take seconds to come back.

  Delivery rides `send_io_confirming/5`, never the `cast_io/4` cast. A
  relay whose runner-side bind has not finished answers a retryable
  verdict, and a cast drops that input with nothing to re-drive it — the
  first input after a start is exactly when that happens. Retrying until the
  ingest is ACCEPTED keeps the input landing exactly once, so a caller
  asserting exact accumulated state stays valid; re-sending blind would not.

  ## Options

    * `:timeout` — `pos_integer()` to override the default in ms.
  """
  def send_sync_io(relay_id, namespace, msg, metadata \\ %{}, opts \\ []) do
    timeout = resolve_timeout(Keyword.get(opts, :timeout))
    deadline = System.monotonic_time(:millisecond) + timeout

    send_confirmed(relay_id, namespace, msg, metadata, deadline)

    receive do
      {:message_forward, reply} -> reply
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        raise "Timeout waiting for message (after #{timeout}ms)"
    end
  end

  @doc """
  Send `msg` to `relay_id` as a confirming source does, again until the relay
  takes it or `deadline` (monotonic ms) passes, and answer the verdict that
  took it. Each send waits for its answer up to the deadline, so it is sent
  again only when no relay could take it.
  """
  def send_confirmed(relay_id, namespace, msg, metadata, deadline) do
    accepted =
      NodeTestHelper.wait_until(
        fn ->
          remaining = max(deadline - System.monotonic_time(:millisecond), 1)

          case RelayManager.send_io_confirming(
                 relay_id,
                 namespace,
                 msg,
                 metadata,
                 remaining
               ) do
            {:error, _retryable} -> false
            accepted -> accepted
          end
        end,
        max(deadline - System.monotonic_time(:millisecond), 0),
        @ingest_retry_ms
      )

    accepted || raise("Relay never accepted the input before the timeout")
  end

  defp resolve_timeout(ms) when is_integer(ms) and ms > 0, do: ms
  defp resolve_timeout(nil), do: @default_timeout_ms
end
