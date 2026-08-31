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

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.State.DurabilityTier.Backend
  alias FlezhaTest.Helper.NodeTestHelper

  @table :flezha_durability_stub
  @control :flezha_durability_stub_control
  @owner :flezha_durability_stub_owner

  # A swept node ships its platform stores, so a unit test running with the tier
  # on must not be pointed at the developer's real `app_data/{auth,schedules,dlq}`
  # — what a case collects would then depend on the machine it runs on. An empty
  # tmp root makes them absent rather than special-cased; a case that wants one
  # of them puts its own dir over this.
  @isolated_store_dirs [{:authn_authz, :auth_dir}, {:schedules, :dir}, {:dlq, :dir}]
  @isolated_prefix "flezha_durability_stub"

  # ---------- test control ----------

  @doc "Fresh, empty store with no faults armed. Selects this backend in config."
  def install() do
    reset()

    # The two collection thresholds keep their real ordering — grace well below
    # abandoned — so a case reaches either one by saying how much time passed
    # (`advance_clock/1`) rather than by collapsing them to zero.
    Application.put_env(:flezha, :durability_tier,
      backend: {__MODULE__, []},
      ship_interval_ms: 60_000,
      rpo_target_ms: 60_000,
      gc_interval_ms: 3_600_000,
      gc_grace_ms: 60_000,
      gc_abandoned_after_ms: 3_600_000
    )

    isolate_store_dirs()
    :ok
  end

  def uninstall() do
    Application.delete_env(:flezha, :durability_tier)
    restore_store_dirs()
    reset()
  end

  defp isolate_store_dirs() do
    root = NodeTestHelper.claim_tmp_dir(@isolated_prefix)

    # The stores mkdir this tree the first time one of them writes; reclaiming
    # it belongs here, beside the only place that names it — `uninstall/0` is
    # not enough, since a module installs once per test and uninstalls once.
    on_exit(fn -> File.rm_rf(root) end)

    for {key, subkey} <- @isolated_store_dirs do
      previous = Application.get_env(:flezha, key, [])

      # A second `install/0` in one test must not save the isolated dir as the
      # thing to restore.
      unless isolated?(Keyword.get(previous, subkey)) do
        control({:saved, key}, previous)
      end

      Application.put_env(:flezha, key, Keyword.put(previous, subkey, Path.join(root, "#{key}")))
    end
  end

  defp restore_store_dirs() do
    for {key, _subkey} <- @isolated_store_dirs do
      case control_get({:saved, key}, :never_saved) do
        :never_saved -> :ok
        previous -> Application.put_env(:flezha, key, previous)
      end
    end
  end

  defp isolated?(path) when is_binary(path), do: String.contains?(path, @isolated_prefix)
  defp isolated?(_path), do: false

  def reset() do
    owner = ensure_owner()

    Agent.update(owner, fn state ->
      for table <- [@table, @control] do
        case :ets.whereis(table) do
          :undefined -> :ets.new(table, [:set, :public, :named_table])
          _ -> :ets.delete_all_objects(table)
        end
      end

      state
    end)
  end

  # The tables are created inside the agent so the AGENT owns them. Created from
  # a test process instead, they would vanish the moment that process exits —
  # and a shipper started with `GenServer.start` outlives it, so its next drain
  # would hit a table that no longer exists rather than a store that is merely
  # empty.
  defp ensure_owner() do
    case Process.whereis(@owner) do
      nil ->
        case Agent.start(fn -> :ok end, name: @owner) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end

      pid ->
        pid
    end
  end

  @doc "Every subsequent operation fails with `{:error, :backend_down}` until re-enabled."
  def set_down(down?), do: control(:down, down?)

  @doc """
  Refuse the next manifest write. Models the one crash window the manifest-last
  ordering exists for: blobs are up, the entry naming them is not.
  """
  def fail_next_manifest(), do: control(:fail_manifest, true)

  @doc """
  Refuse every `put_if/4` with `{:claim_held, claim}`, whatever is stored — a
  writer that took the lineage over. Who holds it is read back from the entry,
  so a case that needs a specific holder should stage a real second writer.
  """
  def fence_at(claim), do: control(:fence, claim)

  @doc """
  Pretend every stored artifact is `ms` older than it is, so a test can reach
  the collection grace without waiting for it.
  """
  def advance_clock(ms), do: control(:clock_skew_ms, ms)

  @doc "Keys currently in the store, sorted."
  def keys(), do: :ets.select(@table, [{{:"$1", :_, :_, :_}, [], [:"$1"]}]) |> Enum.sort()

  @doc "The manifest entry stored for `ref`, decoded."
  def stored_manifest(ref) do
    alias Flezha.State.DurabilityTier.Manifest
    {:ok, blob} = get(Manifest.key(ref), [])
    {:ok, manifest} = Manifest.decode(blob)
    manifest
  end

  @doc "How many `put`/`put_if` calls have landed — the ship's real upload count."
  def writes(), do: control_get(:writes, 0)

  defp control(key, value), do: :ets.insert(@control, {key, value})

  # Tolerates an absent table: `uninstall/0` runs from an `on_exit` that a case
  # which never installed the stub still registers.
  defp control_get(key, default) do
    if :ets.whereis(@control) == :undefined do
      default
    else
      case :ets.lookup(@control, key) do
        [{^key, value}] -> value
        [] -> default
      end
    end
  end

  defp count_write(), do: control(:writes, control_get(:writes, 0) + 1)

  defp down?(), do: control_get(:down, false)

  defp now_ms(), do: System.monotonic_time(:millisecond)

  # ---------- backend ----------

  @impl true
  def child_spec(_opts), do: :ignore

  @impl true
  def put(key, blob, _opts) do
    if down?() do
      {:error, :backend_down}
    else
      count_write()
      :ets.insert(@table, {key, blob, 0, now_ms()})
      :ok
    end
  end

  @impl true
  def get(key, _opts) do
    if down?() do
      {:error, :backend_down}
    else
      case :ets.lookup(@table, key) do
        [{^key, blob, _epoch, _written_at}] -> {:ok, blob}
        [] -> {:error, :not_found}
      end
    end
  end

  @impl true
  def list(prefix, _opts) do
    if down?() do
      {:error, :backend_down}
    else
      skew = control_get(:clock_skew_ms, 0)
      now = now_ms()

      entries =
        :ets.select(@table, [{{:"$1", :_, :_, :"$2"}, [], [{{:"$1", :"$2"}}]}])
        |> Enum.filter(fn {key, _written_at} -> String.starts_with?(key, prefix) end)
        |> Enum.map(fn {key, written_at} -> {key, now - written_at + skew} end)
        |> Enum.sort()

      {:ok, entries}
    end
  end

  @impl true
  def put_if(key, blob, guard, _opts) do
    claim = elem(guard, 1)

    cond do
      down?() ->
        {:error, :backend_down}

      control_get(:fail_manifest, false) ->
        control(:fail_manifest, false)
        {:error, :manifest_write_lost}

      is_integer(control_get(:fence, nil)) ->
        {:error, {:claim_held, control_get(:fence, nil)}}

      true ->
        case :ets.lookup(@table, key) do
          [{^key, _blob, stored, _written_at}] ->
            if Backend.guard_passes?(guard, stored) do
              write(key, blob, claim)
            else
              {:error, {:claim_held, stored}}
            end

          [] ->
            write(key, blob, claim)
        end
    end
  end

  defp write(key, blob, claim) do
    count_write()
    :ets.insert(@table, {key, blob, claim, now_ms()})
    :ok
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
