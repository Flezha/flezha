defmodule Flezha.Plugin.Extension.Table.Chain do
  @moduledoc """
  The extension table's compiled dispatch chains and its `active_mask`: one
  closure per extension name, rebuilt whenever that name's subscriber list
  changes, and the bit `Extension.run/3` short-circuits on.

  It takes the table as a plain map and never names `Extension.Table`; it reads
  and changes the subscriber lists through `Extension.Table.Subscription`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table.Subscription

  require Logger

  # A subscription change is complete once the compiled chain and the active
  # bit follow it. `add_subscription/6` and `remove_subscription/3` mutate only
  # the subscriber lists; `add_hooks/3` and `remove_hooks/3` refresh after
  # them, so the lists never drive the chain. Only the chain-bearing kinds
  # compile one — `compile_chain/3` has no clause for `:request` or `:timer`.
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

  @doc "Register `plugin`'s hooks under `plugin_id`, each chain following."
  @spec add_hooks(map(), binary(), Flezha.Plugin.t()) :: map()
  def add_hooks(table, plugin_id, %Flezha.Plugin{hooks: hooks} = plugin) do
    Enum.reduce(hooks, table, fn {name, sub_opts}, acc ->
      %{kind: kind} = Map.fetch!(acc.catalog, name)

      acc
      |> Subscription.add_subscription(kind, name, plugin_id, plugin, sub_opts)
      |> refresh(name)
    end)
  end

  @doc "Remove `plugin_id`'s `hooks`, each chain following."
  @spec remove_hooks(map(), binary(), list()) :: map()
  def remove_hooks(table, plugin_id, hooks) do
    Enum.reduce(hooks, table, fn {name, _sub_opts}, acc ->
      acc |> Subscription.remove_subscription(name, plugin_id) |> refresh(name)
    end)
  end

  @doc """
  Register `plugin`'s hooks under `plugin_id` afresh: every entry carries a copy
  of the provider opts and a timer hook its timer, so a fresh provider instance
  or a new host process needs entries of its own.
  """
  @spec resubscribe(map(), binary(), Flezha.Plugin.t()) :: map()
  def resubscribe(table, plugin_id, %Flezha.Plugin{} = plugin),
    do: table |> remove_hooks(plugin_id, plugin.hooks) |> add_hooks(plugin_id, plugin)

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
    # Each subscriber's failure latch lives as long as this chain, which every
    # subscriber change rebuilds: a subscriber failing on every record is
    # logged once per chain, not once per record.
    ordered =
      entries
      |> Enum.sort_by(& &1.position)
      |> Enum.map(&Map.put(&1, :failure_logged, :atomics.new(1, [])))

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
              log_first_failure(entry, fn ->
                "[Extension.Table] transform subscriber #{entry.plugin_id} returned error: " <>
                  "#{inspect(reason)}"
              end)

              {:cont, {:ok, current_ctx}}
          end
        catch
          kind, reason ->
            log_first_failure(entry, fn ->
              "[Extension.Table] transform subscriber #{entry.plugin_id} raised " <>
                "#{inspect(kind)}: #{inspect(reason)}"
            end)

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
            log_first_failure(entry, fn ->
              "[Extension.Table] fanout subscriber #{entry.plugin_id} raised " <>
                "#{inspect(kind)}: #{inspect(reason)}"
            end)
        end
      end)

      :ok
    end
  end

  defp log_first_failure(%{failure_logged: latch}, message) do
    if :atomics.compare_exchange(latch, 1, 0, 1) == :ok, do: Logger.error(message)
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
