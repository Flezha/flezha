defmodule Flezha.Observability.AuditLog.Sink do
  @moduledoc """
  Durable audit sink.

  Subscribes to the foundation bus under the `:audit_log` registry
  name with overflow policy `:block`. The foundation Bus filter is
  coarse (`kinds: :any, sources: :any`); the fine-grained
  `(source, name | :any)` allow-list is applied inside the handler
  so policy decisions stay inside the sink.

  Default allow-list:

      [{:observability_service, :any},
       {:flezha_shutdown, :any},
       {:flezha_plugin_installer, :any}]

  Extended at runtime via `:audit_log, :extra_subscriptions`.

  Records are encoded via `Codec.encode/1` and handed to `Writer.write/1`
  synchronously; the `GenServer.call` chain from the producer through
  the sink to the writer gives the `:block` overflow policy
  end-to-end back-pressure under sustained load.

  Post-close gate: once the Writer is closed (graceful shutdown),
  signals are discarded and a per-sink `:post_close_dropped` counter
  is incremented.
  """

  use GenServer

  @behaviour Flezha.Observability.Sink

  alias Flezha.Observability.AuditLog.{Codec, Writer}
  alias Flezha.Observability.Registry, as: ObsRegistry

  @sink_name :audit_log
  @default_high_watermark 10_000
  @default_block_timeout_ms 30_000

  @default_allow_list [
    {:observability_service, :any},
    {:flezha_shutdown, :any},
    {:flezha_plugin_installer, :any},
    {:audit_log, :any}
  ]

  # ---------- Sink behaviour ----------

  @impl Flezha.Observability.Sink
  def handle_signal(_signal, state), do: {:ok, state}

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  # ---------- GenServer ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Number of signals dropped after writer close."
  @spec post_close_dropped() :: non_neg_integer()
  def post_close_dropped() do
    case Process.whereis(__MODULE__) do
      nil -> 0
      pid -> GenServer.call(pid, :post_close_dropped)
    end
  end

  @doc false
  @spec sink_name() :: atom()
  def sink_name(), do: @sink_name

  @impl GenServer
  def init(_opts) do
    allow_list = compute_allow_list()
    register_self()

    {:ok,
     %{
       allow_list: allow_list,
       allow_index: index_allow_list(allow_list),
       writer_closed?: false,
       post_close_dropped: 0
     }}
  end

  @impl GenServer
  def handle_call({:signal, signal}, _from, state) do
    state =
      cond do
        state.writer_closed? ->
          %{state | post_close_dropped: state.post_close_dropped + 1}

        not allowed?(state.allow_index, signal) ->
          state

        true ->
          case safe_write(Codec.encode(signal)) do
            :ok ->
              state

            {:error, :closed} ->
              %{state | writer_closed?: true, post_close_dropped: state.post_close_dropped + 1}

            {:error, _} ->
              state
          end
      end

    {:reply, :ok, state}
  end

  def handle_call(:post_close_dropped, _from, state) do
    {:reply, state.post_close_dropped, state}
  end

  @impl GenServer
  def handle_info({:signal, signal}, state) do
    {:reply, _, state} = handle_call({:signal, signal}, nil, state)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # ---------- Internals ----------

  defp register_self() do
    pid = self()

    :ok =
      ObsRegistry.register_external_sink(%{
        name: @sink_name,
        module: __MODULE__,
        config: %{},
        filter: filter(),
        overflow: :block,
        mailbox_high_watermark: @default_high_watermark,
        block_timeout_ms: @default_block_timeout_ms,
        pid: pid
      })

    :ok
  end

  defp compute_allow_list() do
    extra =
      Application.get_env(:flezha, :audit_log, [])
      |> Keyword.get(:extra_subscriptions, [])
      |> Enum.filter(&valid_entry?/1)

    @default_allow_list ++ extra
  end

  defp valid_entry?({source, name}) when is_atom(source) and (is_atom(name) or name == :any),
    do: true

  defp valid_entry?(_), do: false

  defp index_allow_list(allow_list) do
    Enum.reduce(allow_list, %{}, fn {source, name}, acc ->
      Map.update(acc, source, MapSet.new([name]), &MapSet.put(&1, name))
    end)
  end

  defp allowed?(index, signal) do
    case Map.fetch(index, signal.source) do
      :error ->
        false

      {:ok, names} ->
        MapSet.member?(names, :any) or MapSet.member?(names, signal.name)
    end
  end

  defp safe_write(bin) do
    try do
      Writer.write(bin)
    catch
      :exit, {:noproc, _} -> {:error, :closed}
      :exit, _ -> {:error, :closed}
    end
  end
end
