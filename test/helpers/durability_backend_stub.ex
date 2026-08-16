defmodule FlezhaTest.Helper.DurabilityBackendStub do
  @moduledoc """
  In-memory `Flezha.State.DurabilityTier.Backend` for the tier's unit suite,
  plus the fault injection the durability properties are actually about:
  a backend that is down, and a crash landing exactly between the blob uploads
  and the manifest write.

  Blobs live in one public ETS table so a test asserts on what really reached
  the store rather than on what the shipper believes it sent.
  """

  @behaviour Flezha.State.DurabilityTier.Backend

  @table :flezha_durability_stub
  @control :flezha_durability_stub_control

  # ---------- test control ----------

  @doc "Fresh, empty store with no faults armed. Selects this backend in config."
  def install() do
    reset()

    Application.put_env(:flezha, :durability_tier,
      backend: {__MODULE__, []},
      ship_interval_ms: 60_000,
      rpo_target_ms: 60_000,
      gc_interval_ms: 3_600_000
    )

    :ok
  end

  def uninstall() do
    Application.delete_env(:flezha, :durability_tier)
    reset()
  end

  def reset() do
    for table <- [@table, @control] do
      case :ets.whereis(table) do
        :undefined -> :ets.new(table, [:set, :public, :named_table])
        _ -> :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  @doc "Every subsequent operation fails with `{:error, :backend_down}` until re-enabled."
  def set_down(down?), do: control(:down, down?)

  @doc """
  Refuse the next manifest write. Models the one crash window the manifest-last
  ordering exists for: blobs are up, the entry naming them is not.
  """
  def fail_next_manifest(), do: control(:fail_manifest, true)

  @doc "Answer every `put_if/4` with `{:stale_epoch, epoch}` — a fencing claimant."
  def fence_at(epoch), do: control(:fence, epoch)

  @doc "Keys currently in the store, sorted."
  def keys(), do: :ets.select(@table, [{{:"$1", :_, :_}, [], [:"$1"]}]) |> Enum.sort()

  @doc "How many `put`/`put_if` calls have landed — the ship's real upload count."
  def writes(), do: control_get(:writes, 0)

  defp control(key, value), do: :ets.insert(@control, {key, value})

  defp control_get(key, default) do
    case :ets.lookup(@control, key) do
      [{^key, value}] -> value
      [] -> default
    end
  end

  defp count_write(), do: control(:writes, control_get(:writes, 0) + 1)

  defp down?(), do: control_get(:down, false)

  # ---------- backend ----------

  @impl true
  def child_spec(_opts), do: :ignore

  @impl true
  def put(key, blob, _opts) do
    if down?() do
      {:error, :backend_down}
    else
      count_write()
      :ets.insert(@table, {key, blob, 0})
      :ok
    end
  end

  @impl true
  def get(key, _opts) do
    if down?() do
      {:error, :backend_down}
    else
      case :ets.lookup(@table, key) do
        [{^key, blob, _epoch}] -> {:ok, blob}
        [] -> {:error, :not_found}
      end
    end
  end

  @impl true
  def list(prefix, _opts) do
    if down?() do
      {:error, :backend_down}
    else
      {:ok, Enum.filter(keys(), &String.starts_with?(&1, prefix))}
    end
  end

  @impl true
  def put_if(key, blob, epoch, _opts) do
    cond do
      down?() ->
        {:error, :backend_down}

      control_get(:fail_manifest, false) ->
        control(:fail_manifest, false)
        {:error, :manifest_write_lost}

      is_integer(control_get(:fence, nil)) ->
        {:error, {:stale_epoch, control_get(:fence, nil)}}

      true ->
        case :ets.lookup(@table, key) do
          [{^key, _blob, stored}] when stored > epoch ->
            {:error, {:stale_epoch, stored}}

          _ ->
            count_write()
            :ets.insert(@table, {key, blob, epoch})
            :ok
        end
    end
  end

  @impl true
  def delete(key, _opts) do
    if down?() do
      {:error, :backend_down}
    else
      :ets.delete(@table, key)
      :ok
    end
  end
end
