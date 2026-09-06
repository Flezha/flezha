defmodule Flezha.Dlq.Store do
  @moduledoc """
  Durable dead-letter store (`dead-letter-queue`).

  One controller-wide GenServer owning `app_data/dlq/<operative_class>/<procedure_id>/`:
  a JSONL metadata index (`entries.jsonl`, one record per parked payload)
  with payload bytes in adjacent `<entry_id>.bin` blob files. Appends are
  fsynced before the caller's durable-ack callback fires — the SDK holds
  the poison payload until then, so a park is never acked before it is
  crash-safe.

  Idempotence rides the entry id, a hash of the park's origin identity:
  `entity_key@origin_offset` when the effectively-once tier provides an
  offset, else `channel:sha256(payload)` — so ack-lost re-sends and
  crash-replay re-parks land on the existing entry instead of duplicating
  it. A re-park that arrives while the entry is `replaying` (an operator
  replay that failed again) increments `generation` instead.

  Retention is bounded by age AND total size (more restrictive wins,
  audit-log precedent); deleting an entry by retention is genuine loss of
  already-parked data, so each deletion logs loudly and emits
  `[:flezha, :dlq, :retention_dropped]`.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:sweep]

  require Logger

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Dlq.Entry
  alias Flezha.State.DurabilityTier
  alias Flezha.State.Paths

  @entries_file "entries.jsonl"

  # ---------- public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Durably append a parked payload, built through `Flezha.Dlq.Entry.new/1`.
  Returns `{:ok, entry_ref}` after the index line (and blob) are fsynced;
  idempotent on origin identity.
  """
  @spec append(pid() | atom(), Entry.t()) :: {:ok, String.t()} | {:error, term()}
  def append(server \\ __MODULE__, entry), do: GenServer.call(server, {:append, entry}, 30_000)

  @doc """
  Non-blocking append for the data-channel path: `on_durable.(entry_ref)`
  runs in the store process once the entry is fsynced (the ParkPayloadAck
  send site).
  """
  @spec append_async(pid() | atom(), Entry.t(), (String.t() -> any())) :: :ok
  def append_async(server \\ __MODULE__, entry, on_durable),
    do: GenServer.cast(server, {:append, entry, on_durable})

  @doc """
  Park what a runner's `park_payload` frame decoded to, for `entity_id`. The
  frame's fields ARE the entry's, so the record is built here rather than at the
  data channel: the wire form and the durable record are one shape, and the
  channel that carries it does not need to know the second.
  """
  @spec park_async(
          pid() | atom(),
          {binary(), binary(), binary()},
          map(),
          (String.t() -> any())
        ) :: :ok
  def park_async(server \\ __MODULE__, entity_id, park, on_durable)

  def park_async(server, {operative_class, procedure_id, entity_key}, park, on_durable) do
    entry =
      park
      |> Map.take([:channel, :origin_offset, :error, :attempts, :payload, :park_id])
      |> Map.merge(%{
        operative_class: operative_class,
        procedure_id: procedure_id,
        entity_key: entity_key
      })
      |> Entry.new!()

    append_async(server, entry, on_durable)
  end

  @doc """
  List parked entries, nearest-to-expiry (oldest) first. `operative_class`
  narrows to one class; `nil` lists everything.
  """
  @spec list(pid() | atom(), String.t() | nil) :: [map()]
  def list(server \\ __MODULE__, operative_class),
    do: GenServer.call(server, {:list, operative_class}, 30_000)

  @doc "Fetch one entry (with payload bytes) by `<class>/<procedure>/<id>` ref."
  @spec get(pid() | atom(), String.t()) :: {:ok, map(), binary() | nil} | {:error, term()}
  def get(server \\ __MODULE__, entry_ref), do: GenServer.call(server, {:get, entry_ref}, 30_000)

  @doc "Delete one entry and its payload blob. Idempotent."
  @spec drop(pid() | atom(), String.t()) :: :ok | {:error, term()}
  def drop(server \\ __MODULE__, entry_ref),
    do: GenServer.call(server, {:drop, entry_ref}, 30_000)

  @doc """
  Mark an entry as being replayed. A subsequent re-park of the same origin
  identity increments `generation`; a `resolve/2` removes it.
  """
  @spec mark_replaying(pid() | atom(), String.t()) :: :ok | {:error, term()}
  def mark_replaying(server \\ __MODULE__, entry_ref),
    do: GenServer.call(server, {:mark_replaying, entry_ref}, 30_000)

  @doc "Remove an entry whose replay succeeded."
  @spec resolve(pid() | atom(), String.t()) :: :ok | {:error, term()}
  def resolve(server \\ __MODULE__, entry_ref), do: drop(server, entry_ref)

  @doc "Per-procedure usage: `[%{operative_class, procedure_id, entries, bytes}]`."
  @spec stats(pid() | atom()) :: [map()]
  def stats(server \\ __MODULE__), do: GenServer.call(server, :stats, 30_000)

  @doc "Run a retention sweep now (tests / CLI)."
  @spec sweep(pid() | atom()) :: :ok
  def sweep(server \\ __MODULE__), do: GenServer.call(server, :sweep, 60_000)

  @doc """
  Every `{operative_class, procedure_id}` with a parked-failure directory on
  disk, as the directory names — `nil` lists every class.

  The names are already path-sanitised, which is what makes them safe to use as
  a durable artifact identity: `proc_dir/2` maps one back to the same directory.
  """
  @spec procedures(String.t() | nil) :: [{String.t(), String.t()}]
  def procedures(operative_class \\ nil) do
    classes =
      case operative_class do
        nil -> File.ls(root()) |> ok_or([])
        rc -> [safe(rc)]
      end

    Enum.flat_map(classes, fn rc ->
      Path.join(root(), rc)
      |> File.ls()
      |> ok_or([])
      |> Enum.filter(&File.dir?(Path.join([root(), rc, &1])))
      |> Enum.map(&{rc, &1})
    end)
  end

  @doc "Directory holding one procedure's parked failures."
  @spec proc_dir(String.t(), String.t()) :: String.t()
  def proc_dir(rc, sh), do: Path.join([root(), safe(rc), safe(sh)])

  @doc """
  One identity component as it appears on disk. The durable-tier artifact for a
  parked set is addressed by these, so a caller holding a live operative class
  maps it through here to name the artifact.
  """
  @spec path_component(String.t()) :: String.t()
  def path_component(component), do: safe(component)

  defp root() do
    Config.get(:dlq_dir)
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    {:ok, arm_sweep(%{})}
  end

  @impl true
  def handle_call({:append, entry}, _from, state) do
    {:reply, do_append(entry), state}
  end

  def handle_call({:list, operative_class}, _from, state) do
    entries =
      procedures(operative_class)
      |> Enum.flat_map(fn {rc, sh} -> load_entries(rc, sh) end)
      |> Enum.sort_by(& &1["ts_ms"])

    {:reply, entries, state}
  end

  def handle_call({:get, entry_ref}, _from, state) do
    reply =
      with {:ok, rc, sh, id} <- parse_ref(entry_ref),
           {:ok, entry} <- find_entry(rc, sh, id) do
        payload =
          case File.read(blob_path(rc, sh, id)) do
            {:ok, bytes} -> bytes
            {:error, _} -> nil
          end

        {:ok, entry, payload}
      end

    {:reply, reply, state}
  end

  def handle_call({:drop, entry_ref}, _from, state) do
    reply =
      with {:ok, rc, sh, id} <- parse_ref(entry_ref) do
        remove_entry(rc, sh, id)
      end

    {:reply, reply, state}
  end

  def handle_call({:mark_replaying, entry_ref}, _from, state) do
    reply =
      with {:ok, rc, sh, id} <- parse_ref(entry_ref),
           {:ok, _entry} <- find_entry(rc, sh, id),
           :ok <-
             rewrite_entries(rc, sh, fn e ->
               if e["id"] == id, do: Map.put(e, "status", "replaying"), else: e
             end) do
        dir_changed(rc, sh)
      end

    {:reply, reply, state}
  end

  def handle_call(:stats, _from, state) do
    stats =
      procedures()
      |> Enum.map(fn {rc, sh} ->
        entries = load_entries(rc, sh)

        %{
          operative_class: rc,
          procedure_id: sh,
          entries: length(entries),
          bytes: dir_bytes(rc, sh)
        }
      end)

    {:reply, stats, state}
  end

  def handle_call(:sweep, _from, state) do
    do_sweep()
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:append, entry, on_durable}, state) do
    case do_append(entry) do
      {:ok, ref} ->
        on_durable.(ref)

      {:error, reason} ->
        Logger.error("[dlq] durable append failed (no ack sent): #{inspect(reason)}")
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    do_sweep()
    {:noreply, arm_sweep(state)}
  end

  # ---------- append ----------

  defp do_append(entry) do
    rc = Map.fetch!(entry, :operative_class)
    sh = Map.fetch!(entry, :procedure_id)
    id = entry_id(entry)
    payload = Map.get(entry, :payload)

    case find_entry(rc, sh, id) do
      {:ok, %{"status" => "replaying"} = existing} ->
        bump_generation(rc, sh, existing, entry)

      {:ok, _existing} ->
        # Idempotent re-park (ack-lost re-send or crash-replay): the entry
        # is already durable — re-acking is all the caller needs.
        {:ok, ref(rc, sh, id)}

      {:error, :not_found} ->
        write_new_entry(rc, sh, id, entry, payload)
    end
  end

  defp write_new_entry(rc, sh, id, entry, payload) do
    dir = proc_dir(rc, sh)
    File.mkdir_p!(dir)

    if payload do
      blob = blob_path(rc, sh, id)
      File.write!(blob, payload, [:binary])
      fsync_file(blob)
    end

    record = %{
      "id" => id,
      "ts_ms" => System.system_time(:millisecond),
      "operative_class" => rc,
      "procedure_id" => sh,
      "entity_key" => Map.get(entry, :entity_key, ""),
      "channel" => Map.get(entry, :channel, ""),
      "origin_offset" => Map.get(entry, :origin_offset),
      "error" => Map.get(entry, :error, ""),
      "attempts" => Map.get(entry, :attempts, 0),
      "generation" => 1,
      "status" => "parked",
      "payload_bytes" => if(payload, do: byte_size(payload), else: nil)
    }

    append_line(rc, sh, record)

    :telemetry.execute([:flezha, :dlq, :parked], %{count: 1}, %{
      operative_class: rc,
      procedure_id: sh,
      entity_key: record["entity_key"],
      channel: record["channel"],
      generation: record["generation"]
    })

    dir_changed(rc, sh)
    {:ok, ref(rc, sh, id)}
  rescue
    e -> {:error, e}
  end

  defp bump_generation(rc, sh, existing, entry) do
    id = existing["id"]

    :ok =
      rewrite_entries(rc, sh, fn e ->
        if e["id"] == id do
          e
          |> Map.put("status", "parked")
          |> Map.put("generation", (e["generation"] || 1) + 1)
          |> Map.put("attempts", Map.get(entry, :attempts, e["attempts"]))
          |> Map.put("error", Map.get(entry, :error, e["error"]))
          |> Map.put("ts_ms", System.system_time(:millisecond))
        else
          e
        end
      end)

    :telemetry.execute([:flezha, :dlq, :parked], %{count: 1}, %{
      operative_class: rc,
      procedure_id: sh,
      entity_key: existing["entity_key"],
      channel: existing["channel"],
      generation: (existing["generation"] || 1) + 1
    })

    dir_changed(rc, sh)
    {:ok, ref(rc, sh, id)}
  end

  # Origin identity: tier-on parks carry the input-log offset (stable
  # across crash-replays); tier-off parks key on content so ack-lost
  # re-sends and failed-replay re-parks correlate. A byteless tier-off
  # park falls back to its park_id (unique per park attempt).
  defp entry_id(entry) do
    key =
      case {Map.get(entry, :origin_offset), Map.get(entry, :payload)} do
        {offset, _} when is_integer(offset) ->
          "#{Map.get(entry, :entity_key, "")}@#{offset}"

        {nil, payload} when is_binary(payload) ->
          "#{Map.get(entry, :channel, "")}:#{Base.encode16(:crypto.hash(:sha256, payload))}"

        {nil, nil} ->
          "park:#{Map.get(entry, :park_id, "")}"
      end

    :crypto.hash(:sha256, key) |> Base.encode16(case: :lower) |> binary_part(0, 16)
  end

  # ---------- retention (age AND total size, more restrictive wins) ----------

  defp do_sweep() do
    retention_s = Config.get(:dlq_retention_seconds)
    max_bytes = Config.get(:dlq_max_total_bytes)
    cutoff_ms = System.system_time(:millisecond) - retention_s * 1000

    all =
      procedures()
      |> Enum.flat_map(fn {rc, sh} -> load_entries(rc, sh) end)
      |> Enum.sort_by(& &1["ts_ms"])

    {expired, fresh} = Enum.split_with(all, &(&1["ts_ms"] < cutoff_ms))
    Enum.each(expired, &retention_drop(&1, :age))

    total = Enum.sum(Enum.map(fresh, &entry_bytes/1))
    shrink_to_cap(fresh, total, max_bytes)
  end

  defp shrink_to_cap(_entries, total, cap) when total <= cap, do: :ok

  defp shrink_to_cap([oldest | rest], total, cap) do
    retention_drop(oldest, :size)
    shrink_to_cap(rest, total - entry_bytes(oldest), cap)
  end

  defp shrink_to_cap([], _total, _cap), do: :ok

  defp retention_drop(entry, reason) do
    rc = entry["operative_class"]
    sh = entry["procedure_id"]
    bytes = entry_bytes(entry)

    Logger.warning(
      "[dlq] RETENTION DROP (#{reason}): discarding parked payload " <>
        "#{ref(rc, sh, entry["id"])} channel=#{entry["channel"]} " <>
        "entity=#{entry["entity_key"]} error=#{inspect(entry["error"])} — " <>
        "this data was never replayed and is now permanently lost"
    )

    :telemetry.execute([:flezha, :dlq, :retention_dropped], %{count: 1, bytes: bytes}, %{
      operative_class: rc,
      procedure_id: sh,
      entity_key: entry["entity_key"],
      channel: entry["channel"],
      entry_id: entry["id"],
      reason: reason
    })

    remove_entry(rc, sh, entry["id"])
  end

  defp entry_bytes(entry) do
    (entry["payload_bytes"] || 0) + 256
  end

  defp arm_sweep(state),
    do: arm_timer(state, :sweep, Durations.get(:dlq_sweep_interval_ms), :sweep)

  # ---------- index file plumbing ----------

  defp entries_path(rc, sh), do: Path.join(proc_dir(rc, sh), @entries_file)

  defp blob_path(rc, sh, id), do: Path.join(proc_dir(rc, sh), id <> ".bin")

  defp ref(rc, sh, id), do: "#{rc}/#{sh}/#{id}"

  defp parse_ref(entry_ref) do
    case String.split(entry_ref, "/") do
      [rc, sh, id] when rc != "" and sh != "" and id != "" -> {:ok, rc, sh, id}
      _ -> {:error, :bad_entry_ref}
    end
  end

  # Path-safety: identity entities come off the wire; never let them climb
  # out of the DLQ root.
  defp safe(component) do
    String.replace(component, ~r/[^A-Za-z0-9._-]/, "_")
  end

  defp ok_or({:ok, value}, _default), do: value
  defp ok_or({:error, _}, default), do: default

  defp load_entries(rc, sh) do
    case File.read(entries_path(rc, sh)) do
      {:ok, content} ->
        content
        |> String.split("\n", trim: true)
        |> Enum.flat_map(fn line ->
          # A torn trailing line (crash mid-append) is skipped, not fatal.
          case Jason.decode(line) do
            {:ok, %{"id" => _} = entry} -> [entry]
            _ -> []
          end
        end)

      {:error, _} ->
        []
    end
  end

  defp find_entry(rc, sh, id) do
    case Enum.find(load_entries(rc, sh), &(&1["id"] == id)) do
      nil -> {:error, :not_found}
      entry -> {:ok, entry}
    end
  end

  defp append_line(rc, sh, record) do
    path = entries_path(rc, sh)
    line = Jason.encode!(record) <> "\n"
    {:ok, io} = :file.open(path, [:append, :raw, :binary])

    try do
      :ok = :file.write(io, line)
      :ok = :file.datasync(io)
    after
      :file.close(io)
    end
  end

  defp rewrite_entries(rc, sh, map_fun) do
    entries = load_entries(rc, sh) |> Enum.map(map_fun) |> Enum.reject(&is_nil/1)
    path = entries_path(rc, sh)
    content = Enum.map_join(entries, "", &(Jason.encode!(&1) <> "\n"))
    :ok = Paths.atomic_install(path, &:file.write(&1, content), :dlq_entries)
  end

  defp remove_entry(rc, sh, id) do
    case find_entry(rc, sh, id) do
      {:ok, _entry} ->
        :ok = rewrite_entries(rc, sh, fn e -> if e["id"] == id, do: nil, else: e end)
        File.rm(blob_path(rc, sh, id))
        dir_changed(rc, sh)
        :ok

      {:error, :not_found} ->
        :ok
    end
  end

  defp dir_bytes(rc, sh) do
    dir = proc_dir(rc, sh)

    File.ls(dir)
    |> ok_or([])
    |> Enum.map(fn f ->
      case File.stat(Path.join(dir, f)) do
        {:ok, %{size: size}} -> size
        _ -> 0
      end
    end)
    |> Enum.sum()
  end

  # One procedure's parked set changed: republish the usage gauge, and offer the
  # directory to the durability tier, which ships it whole. A drop has to reach
  # the backend as surely as a park — a stale remote copy would resurrect an
  # entry the operator already resolved. Inert while no backend is configured.
  defp dir_changed(rc, sh) do
    :telemetry.execute(
      [:flezha, :dlq, :usage],
      %{entries: length(load_entries(rc, sh)), bytes: dir_bytes(rc, sh)},
      %{operative_class: rc, procedure_id: sh}
    )

    DurabilityTier.ship({:dlq, {safe(rc), safe(sh)}})
  end

  defp fsync_file(path) do
    {:ok, io} = :file.open(path, [:read, :write, :raw, :binary])

    try do
      :ok = :file.datasync(io)
    after
      :file.close(io)
    end
  end
end
