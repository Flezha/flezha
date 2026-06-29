defmodule Flezha.Plugin.Extension do
  @moduledoc """
  Typed extension-point dispatch API. The host module holds a
  `Flezha.Plugin.Extension.Table` in its state and invokes `run/3` at each
  extension-point call site.

  Dispatch kinds:

    * `:fold`      — ordered accumulator, halts on `{:error, _}`
    * `:transform` — ordered payload transform, halts on `{:skip, _}`,
                     tolerates `{:error, _}` (prior ctx passes through)
    * `:fanout`    — runs every subscriber, errors logged, always `:ok`
    * `:request`   — keyed dispatch to a single subscriber, in a short-lived
                     Task under the host's `message_handler_task_sup`
    * `:timer`     — scheduled invocation, driven by `fire_timer/3`

  The first test in `run/3` is a bitmask check against
  `table.active_mask`; when the extension has no subscribers the call
  returns the no-op result without allocating. Chains for `:fold`,
  `:transform`, and `:fanout` are pre-composed at subscribe time and stored
  in `table.chains[name]`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table

  require Logger

  @type run_result :: {:ok, struct()} | {:skip, term()} | {:error, term()} | :ok

  @message_handler_task_sup Flezha.Operative.MessageHandlerTaskSup

  @doc """
  Dispatch the extension named `name` with context `ctx`. Returns a
  kind-dependent result.
  """
  @spec run(Table.t(), atom(), struct()) :: run_result()
  def run(%Table{} = table, name, ctx) do
    case Map.fetch(table.catalog, name) do
      {:ok, %{kind: kind, bit_index: bit}} ->
        if Bitwise.band(table.active_mask, Bitwise.bsl(1, bit)) == 0 do
          empty_result(kind, ctx)
        else
          dispatch(kind, name, table, ctx)
        end

      :error ->
        {:error, {:unknown_extension, name}}
    end
  end

  defp empty_result(:fold, ctx), do: {:ok, ctx}
  defp empty_result(:transform, ctx), do: {:ok, ctx}
  defp empty_result(:fanout, _ctx), do: :ok
  defp empty_result(:request, ctx), do: reply_unknown_key(ctx)
  defp empty_result(:timer, _ctx), do: :ok

  defp dispatch(:fold, name, table, ctx), do: apply_chain(table, name, ctx, {:ok, ctx})
  defp dispatch(:transform, name, table, ctx), do: apply_chain(table, name, ctx, {:ok, ctx})
  defp dispatch(:fanout, name, table, ctx), do: apply_chain(table, name, ctx, :ok)
  defp dispatch(:request, name, table, ctx), do: dispatch_request(table, name, ctx)
  defp dispatch(:timer, _name, _table, _ctx), do: :ok

  defp apply_chain(%Table{chains: chains}, name, ctx, fallback) do
    case Map.get(chains, name) do
      nil -> fallback
      fun when is_function(fun, 1) -> fun.(ctx)
    end
  end

  defp dispatch_request(%Table{} = table, name, %Ctx.Msg{key: key} = ctx) do
    case get_in(table.request_subs, [name, key]) do
      nil ->
        reply_unknown_key(ctx)
        :ok

      {_plugin_id, %{provider: provider, provider_opts: provider_opts}} ->
        Task.Supervisor.start_child(
          @message_handler_task_sup,
          fn ->
            result =
              try do
                provider.on_message(ctx, provider_opts)
              catch
                kind, reason ->
                  {:error, {:exception, kind, reason}}
              end

            send(ctx.reply_to, {:flezha_response, ctx.ref, result})
          end
        )

        :ok
    end
  end

  defp reply_unknown_key(%Ctx.Msg{reply_to: reply_to, ref: ref, key: key}) do
    send(reply_to, {:flezha_response, ref, {:error, {:unknown_key, :on_message, key}}})
    :ok
  end

  defp reply_unknown_key(_other), do: :ok
end
