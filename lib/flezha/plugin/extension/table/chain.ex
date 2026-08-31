defmodule Flezha.Plugin.Extension.Table.Chain do
  @moduledoc """
  The extension table's compiled dispatch chains and its `active_mask`: one
  closure per extension name, rebuilt whenever that name's subscriber list
  changes, and the bit `Extension.run/3` short-circuits on.

  It takes the table as a plain map and never names `Extension.Table`; it reads
  the subscriber lists through `Extension.Table.Subscription`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table.Subscription

  require Logger

  # A subscription change is complete once the compiled chain and the active
  # bit follow it. `add_subscription/6` and `remove_subscription/3` mutate only
  # the subscriber lists; the caller refreshes, so the lists never drive the
  # chain. Only the chain-bearing kinds compile one — `compile_chain/3` has no
  # clause for `:request` or `:timer`.
  @doc """
  Recompile `name`'s chain and set its active bit after a subscriber change.
  """
  @spec refresh(map(), atom()) :: map()
  def refresh(table, name) do
    case Map.fetch(table.catalog, name) do
      {:ok, %{kind: kind}} when kind in [:fold, :transform, :fanout] ->
        table |> rebuild_chain(name) |> mark_active(name, subscribed?(table, name))

      {:ok, _entry} ->
        mark_active(table, name, subscribed?(table, name))

      :error ->
        table
    end
  end

  defp subscribed?(table, name) do
    Map.get(table.subs, name, []) != [] or map_size(Map.get(table.request_subs, name, %{})) > 0
  end

  defp mark_active(table, name, active?) do
    case Map.fetch(table.catalog, name) do
      {:ok, %{bit_index: bit}} ->
        mask_bit = Bitwise.bsl(1, bit)

        new_mask =
          if active? do
            Bitwise.bor(table.active_mask, mask_bit)
          else
            Bitwise.band(table.active_mask, Bitwise.bnot(mask_bit))
          end

        %{table | active_mask: new_mask}

      :error ->
        table
    end
  end

  defp rebuild_chain(table, name) do
    %{kind: kind} = Map.fetch!(table.catalog, name)
    entries = Map.get(table.subs, name, [])
    ordered = Enum.sort_by(entries, & &1.position)
    fun = compile_chain(kind, name, ordered)
    %{table | chains: Map.put(table.chains, name, fun)}
  end

  defp compile_chain(_kind, _name, []), do: nil

  defp compile_chain(:fold, name, entries) do
    fn ctx -> Enum.reduce_while(entries, {:ok, ctx}, &fold_step(name, &1, &2)) end
  end

  defp compile_chain(:transform, name, entries) do
    fn ctx ->
      Enum.reduce_while(entries, {:ok, ctx}, fn entry, {:ok, current_ctx} ->
        try do
          case apply(entry.provider, name, [current_ctx, entry.provider_opts]) do
            {:ok, new_ctx} ->
              {:cont, {:ok, new_ctx}}

            {:skip, reason} ->
              {:halt, {:skip, reason}}

            {:error, reason} ->
              Logger.error(
                "[Extension.Table] transform subscriber #{entry.plugin_id} returned error: " <>
                  "#{inspect(reason)}"
              )

              {:cont, {:ok, current_ctx}}
          end
        catch
          kind, reason ->
            Logger.error(
              "[Extension.Table] transform subscriber #{entry.plugin_id} raised " <>
                "#{inspect(kind)}: #{inspect(reason)}"
            )

            {:cont, {:ok, current_ctx}}
        end
      end)
    end
  end

  defp compile_chain(:fanout, name, entries) do
    fn ctx ->
      Enum.each(entries, fn entry ->
        try do
          apply(entry.provider, name, [ctx, entry.provider_opts])
        catch
          kind, reason ->
            Logger.error(
              "[Extension.Table] fanout subscriber #{entry.plugin_id} raised " <>
                "#{inspect(kind)}: #{inspect(reason)}"
            )
        end
      end)

      :ok
    end
  end

  defp fold_step(name, entry, {:ok, acc_ctx}) do
    timeout_ms = Subscription.sub_opt(entry.sub_opts, :timeout_ms)
    on_error = Subscription.sub_opt(entry.sub_opts, :on_error)

    case invoke_fold(name, entry.provider, acc_ctx, entry.provider_opts, timeout_ms) do
      {:ok, new_ctx} ->
        merged = merge_fold_ctx(acc_ctx, new_ctx)
        {:cont, {:ok, merged}}

      {:error, reason} ->
        handle_fold_error(on_error, acc_ctx, reason)

      :timeout ->
        handle_fold_error(on_error, acc_ctx, :timeout)

      {:exception, kind, reason, stacktrace} ->
        handle_fold_error(on_error, acc_ctx, {:exception, kind, reason, stacktrace})
    end
  end

  defp invoke_fold(name, provider, ctx, opts, timeout_ms) do
    task =
      Task.async(fn ->
        try do
          {:ok_return, apply(provider, name, [ctx, opts])}
        catch
          kind, reason -> {:exception, kind, reason, __STACKTRACE__}
        end
      end)

    case Task.yield(task, timeout_ms) do
      {:ok, {:ok_return, {:ok, %_{} = new_ctx}}} ->
        {:ok, new_ctx}

      {:ok, {:ok_return, {:ok, other}}} ->
        {:error, {:invalid_return, other}}

      {:ok, {:ok_return, {:error, reason}}} ->
        {:error, reason}

      {:ok, {:ok_return, other}} ->
        {:error, {:invalid_return, other}}

      {:ok, {:exception, _, _, _} = exc} ->
        exc

      {:exit, reason} ->
        {:error, {:exit, reason}}

      nil ->
        Task.shutdown(task, :brutal_kill)
        :timeout
    end
  end

  ## Private — :fold merge/error handling

  defp merge_fold_ctx(%Ctx.Init{accumulated: acc} = _prev, %Ctx.Init{accumulated: new} = next) do
    merged = Map.merge(acc, new)
    %Ctx.Init{next | accumulated: merged}
  end

  defp merge_fold_ctx(_prev, next), do: next

  defp handle_fold_error("fail", _acc_ctx, reason), do: {:halt, {:error, reason}}

  defp handle_fold_error("empty", acc_ctx, reason) do
    Logger.debug(fn ->
      "[Extension.Table] fold subscriber skipped on error: #{inspect(reason)}"
    end)

    {:cont, {:ok, acc_ctx}}
  end
end
