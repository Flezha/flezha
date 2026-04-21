defmodule Flezha.Processor.InitStateResolver do
  @moduledoc """
  Runs an ordered chain of `%Flezha.Processor.InitStatePlugin{}` entries to
  produce a processor's initial state at cold-start.

  Each provider receives a context map
  (see `Flezha.Processor.InitStateProvider`) carrying `processor_id`,
  `namespace`, `factory_id`, the `accumulated` result so far, and the
  plugin's own `opts`. A successful `{:ok, map}` return is shallow-merged
  into the accumulator; keys from later plugins take precedence.

  Failures (explicit `{:error, _}`, timeout, exception, or invalid plugin
  configuration) are dispatched by each plugin's `on_error`:

    * `:fail` — abort the chain with `{:error, reason}`
    * `:empty` — log at debug and pass the accumulator through unchanged

  Each provider runs in an isolated `Task`; `timeout_ms` enforces a hard
  upper bound per plugin and the task is forcibly killed on timeout so a
  hanging provider cannot starve `Overwatch.init/1`.
  """

  alias Flezha.Processor.InitStatePlugin

  require Logger

  @type base_context :: %{
          processor_id: String.t(),
          namespace: atom(),
          factory_id: String.t()
        }

  @spec resolve([InitStatePlugin.t()], base_context()) :: {:ok, map()} | {:error, term()}
  def resolve(plugins, base_context) when is_list(plugins) and is_map(base_context) do
    Enum.reduce_while(plugins, {:ok, %{}}, fn plugin, {:ok, acc} ->
      case run_plugin(plugin, acc, base_context) do
        {:ok, merged} -> {:cont, {:ok, merged}}
        {:skip, acc} -> {:cont, {:ok, acc}}
        {:abort, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp run_plugin(plugin, acc, base_context) do
    case validate(plugin) do
      :ok ->
        execute(plugin, acc, base_context)

      {:error, reason} ->
        on_error(plugin, {:invalid_init_state_plugin, reason}, acc)
    end
  end

  defp validate(%InitStatePlugin{provider: provider}) when is_atom(provider) do
    cond do
      not Code.ensure_loaded?(provider) ->
        {:error, {:module_not_loaded, provider}}

      not function_exported?(provider, :resolve, 1) ->
        {:error, {:missing_callback, provider, :resolve, 1}}

      true ->
        :ok
    end
  end

  defp validate(%InitStatePlugin{provider: provider}) do
    {:error, {:invalid_provider, provider}}
  end

  defp validate(other) do
    {:error, {:not_a_struct, other}}
  end

  defp execute(%InitStatePlugin{} = plugin, acc, base_context) do
    context =
      base_context
      |> Map.put(:accumulated, acc)
      |> Map.put(:opts, plugin.opts)

    case safe_invoke(plugin, context) do
      {:ok, %{} = result} ->
        {:ok, Map.merge(acc, result)}

      {:ok, other} ->
        on_error(plugin, {:invalid_return, other}, acc)

      {:error, reason} ->
        on_error(plugin, reason, acc)

      :timeout ->
        on_error(plugin, :timeout, acc)

      {:exception, kind, reason, stacktrace} ->
        on_error(plugin, {:exception, kind, reason, stacktrace}, acc)
    end
  end

  defp safe_invoke(%InitStatePlugin{provider: provider, timeout_ms: timeout_ms}, context) do
    task =
      Task.async(fn ->
        try do
          {:ok_return, provider.resolve(context)}
        catch
          kind, reason ->
            {:exception, kind, reason, __STACKTRACE__}
        end
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, {:ok_return, {:ok, _} = ok}} ->
        ok

      {:ok, {:ok_return, {:error, _} = err}} ->
        err

      {:ok, {:ok_return, other}} ->
        {:ok, other}

      {:ok, {:exception, _, _, _} = exc} ->
        exc

      {:exit, reason} ->
        {:error, {:exit, reason}}

      nil ->
        Task.shutdown(task, :brutal_kill)
        :timeout
    end
  end

  defp on_error(%InitStatePlugin{on_error: :fail}, reason, _acc), do: {:abort, reason}

  defp on_error(%InitStatePlugin{on_error: :empty, provider: provider}, reason, acc) do
    Logger.debug(fn ->
      "[InitStateResolver] skipping provider #{inspect(provider)} on error: #{inspect(reason)}"
    end)

    {:skip, acc}
  end

  defp on_error(_invalid_plugin, reason, _acc) do
    {:abort, reason}
  end
end
