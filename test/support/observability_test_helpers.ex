defmodule Flezha.Observability.TestSinks do
  @moduledoc false
  # Sink implementations used only by tests under
  # `test/flezha/observability/` and `test/flezha/service/`.
end

defmodule Flezha.Observability.TestSinks.Counting do
  @moduledoc false
  # Counts every received signal. Each instance owns a `:counters`
  # array passed via config so multiple counting sinks can co-exist.
  #
  # The bus-level `filter/0` is `:any/:any` so the sink receives
  # every signal. The handler ignores signals whose `:source` is
  # not in the per-instance `config.sources` allowlist when one is
  # provided — keeps the test process's mailbox from filling up with
  # unrelated signals (telemetry bridge, logger handler, neighbouring
  # tests).

  use GenServer

  @behaviour Flezha.Observability.Sink

  @impl Flezha.Observability.Sink
  def handle_signal(_signal, state), do: {:ok, state}

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{counter_ref: :term, parent: :term}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(opts), do: {:ok, opts}

  @impl true
  def handle_info(
        {:signal, %{source: s} = signal},
        %{config: %{counter_ref: ref, parent: parent} = config} = state
      ) do
    if accept_source?(config, s) do
      :counters.add(ref, 1, 1)
      if is_pid(parent), do: send(parent, {:counted, signal})
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp accept_source?(%{sources: list}, source) when is_list(list), do: source in list
  defp accept_source?(_, _), do: true
end

defmodule Flezha.Observability.TestSinks.Slow do
  @moduledoc false
  # Sleeps in handle_signal to deliberately back the mailbox up.

  use GenServer

  @behaviour Flezha.Observability.Sink

  @impl Flezha.Observability.Sink
  def handle_signal(_signal, state), do: {:ok, state}

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{sleep_ms: :pos_integer}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(opts), do: {:ok, opts}

  @impl true
  def handle_info({:signal, _signal}, %{config: %{sleep_ms: ms}} = state) do
    Process.sleep(ms)
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end

defmodule Flezha.Observability.TestSinks.Blocking do
  @moduledoc false
  # Used to verify the `:block` overflow policy. Handles
  # `{:signal, signal}` via GenServer.call.

  use GenServer

  @behaviour Flezha.Observability.Sink

  @impl Flezha.Observability.Sink
  def handle_signal(_signal, state), do: {:ok, state}

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{parent: :term}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(opts), do: {:ok, opts}

  @impl true
  def handle_call({:signal, signal}, _from, %{config: %{parent: parent}} = state) do
    if is_pid(parent), do: send(parent, {:blocked_received, signal})
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end
