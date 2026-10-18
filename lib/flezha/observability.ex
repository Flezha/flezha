defmodule Flezha.Observability do
  @moduledoc """
  Top-level convenience API for the observability foundation.

  Most callers use:

    * `Flezha.Observability.Bus.emit/1` — emit a `Signal`.
    * `Flezha.Observability.subscribe/1` — subscribe the current
      process to live signals from one source.
    * `Flezha.Observability.RingBufferSink.query/2` — read recent
      signals back out of the ring buffer.

  For user-facing access (CLI / future Phoenix UI) use the
  service module `Flezha.Service.Observability`, which validates a
  `caller_context` and emits audit events.
  """

  alias Flezha.Config
  alias Flezha.Observability.PubSubSink

  @pubsub Flezha.Observability.PubSub

  @doc """
  Subscribe the calling process to signals from one source via
  `Phoenix.PubSub`. Messages arrive as
  `{:flezha_signal, %Flezha.Observability.Signal{}}`.

  Returns `{:error, :pubsub_sink_disabled}` when the PubSub sink is
  not running (e.g. in test config or with the master flag off).
  """
  @spec subscribe(atom()) :: :ok | {:error, term()}
  def subscribe(source) when is_atom(source) do
    if pubsub_running?() do
      Phoenix.PubSub.subscribe(@pubsub, PubSubSink.topic_for(source))
    else
      {:error, :pubsub_sink_disabled}
    end
  end

  @spec unsubscribe(atom()) :: :ok | {:error, term()}
  def unsubscribe(source) when is_atom(source) do
    if pubsub_running?() do
      Phoenix.PubSub.unsubscribe(@pubsub, PubSubSink.topic_for(source))
    else
      {:error, :pubsub_sink_disabled}
    end
  end

  @spec enabled?() :: boolean()
  def enabled?() do
    Config.get(:observability_enabled)
  end

  defp pubsub_running?() do
    Process.whereis(@pubsub) != nil and Process.whereis(PubSubSink) != nil
  end
end
