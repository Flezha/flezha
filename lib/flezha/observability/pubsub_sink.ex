defmodule Flezha.Observability.PubSubSink do
  @moduledoc """
  Fan-out sink that re-publishes every accepted signal on
  `Phoenix.PubSub` (server name `Flezha.Observability.PubSub`) under a
  topic-per-source: `flezha.observability.source.<source-atom>`.

  Subscribers receive `{:flezha_signal, %Flezha.Observability.Signal{}}`
  messages. Use `Flezha.Observability.subscribe/1` and
  `Flezha.Observability.unsubscribe/1` from caller code.

  The sink can be disabled via the `:pubsub_sink_enabled` knob in
  `:observability` runtime config (default `true` in prod, `false` in
  test). When disabled the supervisor does not start it and
  `subscribe/1` returns `{:error, :pubsub_sink_disabled}`.
  """

  use GenServer, restart: :transient

  @behaviour Flezha.Observability.Sink

  @pubsub Flezha.Observability.PubSub

  # ---------- Sink behaviour ----------

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  # ---------- Public API ----------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec topic_for(atom()) :: String.t()
  def topic_for(source) when is_atom(source) do
    "flezha.observability.source." <> Atom.to_string(source)
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  @impl true
  def handle_info({:signal, signal}, state) do
    if Process.whereis(@pubsub) do
      Phoenix.PubSub.broadcast(@pubsub, topic_for(signal.source), {:flezha_signal, signal})
    end

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
