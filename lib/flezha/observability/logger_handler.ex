defmodule Flezha.Observability.LoggerHandler do
  @moduledoc """
  OTP `:logger` handler turning every log record into a `:log` signal
  on the observability bus.

  Uses the native handler-module API (`log/2`, `adding_handler/1`,
  `removing_handler/1`), never `Logger.Backends`.

  One invariant: a broken observability subsystem must never lose an
  application log. If `Bus.emit/1` raises, the handler catches it,
  increments `:logger_bridge_failures` and returns normally, so the
  `Logger` pipeline continues.
  """

  use GenServer, restart: :transient

  alias Flezha.Observability.{Bus, Counters, Signal}

  @handler_id :flezha_observability
  @source :logger

  # ---------- Public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc false
  @spec handler_id() :: atom()
  def handler_id(), do: @handler_id

  # ---------- :logger handler callbacks ----------

  @doc false
  def adding_handler(config), do: {:ok, config}

  @doc false
  def removing_handler(_config), do: :ok

  @doc false
  def log(%{level: level, msg: msg, meta: meta} = _event, _config) do
    try do
      signal =
        Signal.new(%{
          kind: :log,
          source: @source,
          name: level,
          measurements: %{count: 1},
          metadata: build_metadata(level, msg, meta)
        })

      Bus.emit(signal)
    rescue
      _ ->
        Counters.bump(:logger_bridge_failures)
        :ok
    catch
      _kind, _reason ->
        Counters.bump(:logger_bridge_failures)
        :ok
    end

    :ok
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    case :logger.add_handler(@handler_id, __MODULE__, %{level: :all}) do
      :ok -> :ok
      {:error, {:already_exist, _}} -> :ok
    end

    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :logger.remove_handler(@handler_id)
    :ok
  end

  # ---------- Internal helpers ----------

  defp build_metadata(level, msg, meta) do
    %{
      level: level,
      message: render_message(msg),
      mfa: Map.get(meta, :mfa),
      file: Map.get(meta, :file),
      line: Map.get(meta, :line),
      logger: meta
    }
  end

  defp render_message({:string, iodata}), do: safe_iodata_to_binary(iodata)

  defp render_message({:report, %{format: format, args: args}}) do
    safe_iodata_to_binary(:io_lib.format(format, args))
  end

  defp render_message({:report, report}), do: inspect(report)

  defp render_message({format, args}) when is_list(format) or is_binary(format) do
    safe_iodata_to_binary(:io_lib.format(format, args))
  end

  defp render_message(other), do: inspect(other)

  defp safe_iodata_to_binary(iodata) do
    try do
      IO.iodata_to_binary(iodata)
    rescue
      _ -> inspect(iodata)
    end
  end
end
