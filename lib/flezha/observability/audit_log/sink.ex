defmodule Flezha.Observability.AuditLog.Sink do
  @moduledoc """
  Durable audit sink.

  Registered as `:audit_log` with overflow `:block`. The bus filter is
  coarse (`kinds: :any, sources: :any`) and the fine-grained
  `(source, name | :any)` allow-list is applied inside the handler, so
  the policy decision stays in the sink.

  `Codec.encode/1` then a synchronous `Writer.write/1`: the
  `GenServer.call` chain from producer through sink to writer is what
  makes `:block` back-pressure end to end. Once the Writer is closed,
  signals are discarded against a `:post_close_dropped` counter.

  The default allow-list is the system pairs plus every source
  `Flezha.AuthZ.Verbs` declares; `:extra_subscriptions` extends it:
  `docs/operations/audit_log.md`.
  """

  use GenServer

  @behaviour Flezha.Observability.Sink

  alias Flezha.AuthZ.Verbs
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Observability.AuditLog.{Codec, Writer}
  alias Flezha.Observability.Registry, as: ObsRegistry

  @sink_name :audit_log
  @default_high_watermark 10_000

  # The system pairs. Every source an operator verb's row declares joins them
  # at boot, so a declared audit is durable by construction.
  @system_allow_list [
    {:flezha_shutdown, :any},
    {:flezha_plugin_installer, :any},
    # Auth-state mutations and authorization denials emit on
    # `:platform_authz`; the audit log subscribes by default so every
    # token issue/revoke, bootstrap completion, and forbidden decision is
    # durably recorded.
    {:platform_authz, :any}
  ]

  # ---------- Sink behaviour ----------

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  # ---------- GenServer ----------

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
        block_timeout_ms: Durations.get(:audit_log_block_timeout_ms),
        pid: pid
      })

    :ok
  end

  defp compute_allow_list() do
    extra =
      Config.get(:audit_log_extra_subscriptions)
      |> Enum.filter(&valid_entry?/1)

    @system_allow_list ++ Enum.map(Verbs.audit_sources(), &{&1, :any}) ++ extra
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
