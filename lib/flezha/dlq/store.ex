defmodule Flezha.Dlq.Store do
  @moduledoc """
  Durable dead-letter store (`relay-dead-letters`).

  One controller-wide GenServer owning `storage/app/dlq/<relay_class>/<tasklet_id>/`:
  a JSONL metadata index (`entries.jsonl`, one record per parked payload)
  with payload bytes in adjacent `<entry_id>.bin` blob files. Appends are
  fsynced before the caller's durable-ack callback fires — the SDK holds
  the poison payload until then, so a park is never acked before it is
  crash-safe.

  Idempotence rides the entry id, a hash of the park's origin identity:
  `cell_key@origin_offset` when the effectively-once tier provides an
  offset, else `channel:sha256(payload)` — so ack-lost re-sends and
  crash-replay re-parks land on the existing entry instead of duplicating
  it. A re-park that arrives while the entry is `replaying` (a user
  replay that failed again) increments `generation` instead.

  Retention is bounded by age AND total size (more restrictive wins,
  audit-log precedent); deleting an entry by retention is genuine loss of
  already-parked data, so each deletion logs loudly and emits
  `[:flezha, :dlq, :retention_dropped]`.

  A tasklet directory whose last entry is resolved is removed with it when no
  durability backend is configured; with one, the retention sweep removes it
  once the tier has shipped the emptied set.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:sweep]

  require Logger

  alias Flezha.Cluster.Page
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Dlq.Entry
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Relay.Lookup
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
  idempotent on origin identity, answering `{:existing, entry_ref}` for an
  entry already parked.
  """
  @spec append(pid() | atom(), Entry.t()) ::
          {:ok | :existing, String.t()} | {:error, term()}
  def append(server \\ __MODULE__, entry), do: call(server, {:append, entry})

  @doc """
  Tell the relay serving `relay_class` that one of its inputs is durably
  dead-lettered, as `parked` (the append's answer) names it: a caller waiting on
  it under `notice.token` is answered `call.failed`, and its listeners are handed
  the `input.dead_lettered` notice only for a new entry. Every input park route
  calls this once its park is durable; a payload parked by a sink or a forward
  is an output, and none of them calls it.

  `notice` carries `token` (or `nil`), `error` (the entry's error text),
  `channel`, `offset` (or `nil`), `tasklet` and `attempts`.
  """
  @spec announce(binary(), {:ok | :existing, String.t()}, map()) :: :ok
  def announce(relay_class, {parked, entry_ref}, notice) when parked in [:ok, :existing] do
    notice = Map.merge(notice, %{entry: entry_ref, new: parked == :ok})

    with {:ok, overwatch} <- Lookup.overwatch(relay_class),
         do: GenServer.cast(overwatch, {:dead_lettered, notice})

    :ok
  end

  @doc """
  Non-blocking append for the data-channel path: `on_durable` runs in the
  store process with `append/2`'s answer once the entry is fsynced (the
  ParkPayloadAck send site).
  """
  @spec append_async(pid() | atom(), Entry.t(), ({:ok | :existing, String.t()} -> any())) ::
          :ok
  def append_async(server \\ __MODULE__, entry, on_durable),
    do: GenServer.cast(server, {:append, entry, on_durable})

  @doc """
  Park what a runner's `park_payload` frame decoded to, for `cell_id`. The
  frame's fields ARE the entry's, so the record is built here rather than at the
  data channel: the wire form and the durable record are one shape, and the
  channel that carries it does not need to know the second.
  """
  @spec park_async(
          pid() | atom(),
          {binary(), binary(), binary()},
          map(),
          ({:ok | :existing, String.t()} -> any())
        ) :: :ok
  def park_async(server \\ __MODULE__, cell_id, park, on_durable)

  def park_async(server, {relay_class, tasklet_id, cell_key}, park, on_durable) do
    entry =
      park
      |> Map.take([:channel, :origin_offset, :error, :attempts, :payload, :park_id])
      |> Map.merge(%{
        relay_class: relay_class,
        tasklet_id: tasklet_id,
        cell_key: cell_key
      })
      |> Entry.new!()

    append_async(server, entry, on_durable)
  end

  @doc """
  List parked entries, nearest-to-expiry (oldest) first. `relay_class`
  narrows to one class; `nil` lists everything.
  """
  @spec list(pid() | atom(), String.t() | nil) :: [map()]
  def list(server \\ __MODULE__, relay_class),
    do: call(server, {:list, relay_class})

  @doc "Fetch one entry (with payload bytes) by `<class>/<tasklet>/<id>` ref."
  @spec get(pid() | atom(), String.t()) :: {:ok, map(), binary() | nil} | {:error, term()}
  def get(server \\ __MODULE__, entry_ref), do: call(server, {:get, entry_ref})

  @doc "Delete one entry and its payload blob. Idempotent."
  @spec drop(pid() | atom(), String.t()) :: :ok | {:error, term()}
  def drop(server \\ __MODULE__, entry_ref),
    do: call(server, {:drop, entry_ref})

  @doc """
  Mark an entry as being replayed. A subsequent re-park of the same origin
  identity increments `generation`; a `resolve/2` removes it.
  """
  @spec mark_replaying(pid() | atom(), String.t()) :: :ok | {:error, term()}
  def mark_replaying(server \\ __MODULE__, entry_ref),
    do: call(server, {:mark_replaying, entry_ref})

  @doc "Remove an entry whose replay succeeded."
  @spec resolve(pid() | atom(), String.t()) :: :ok | {:error, term()}
  def resolve(server \\ __MODULE__, entry_ref), do: drop(server, entry_ref)

  @doc """
  One page (`Flezha.Cluster.Page`) of the parked entries, newest first by
  `ts_ms`, then relay class, tasklet and id (`page_key/1`), each with its
  `node`. Only the index lines of the page are decoded: the others are read for
  their key alone.
  """
  @spec list_page(pid() | atom(), String.t() | nil, Page.t()) :: Page.answer()
  def list_page(server \\ __MODULE__, relay_class, page),
    do: call(server, {:list, relay_class, page})

  @doc """
  The key a page of entries is sorted by, from an entry and the `node` holding
  it: newest first across controllers, two copies of one entry on two
  controllers told apart.
  """
  @spec page_key(map()) :: tuple()
  def page_key(entry),
    do:
      {entry["ts_ms"], safe(entry["relay_class"]), safe(entry["tasklet_id"]), entry["id"],
       entry.node}

  @doc """
  Per-tasklet usage: `[%{relay_class, tasklet_id, entries, bytes}]`, `bytes`
  the size of the tasklet's directory on disk. Answered from counts the Store
  keeps in memory, so it opens no file.
  """
  @spec stats(pid() | atom()) :: [map()]
  def stats(server \\ __MODULE__), do: call(server, :stats)

  @doc """
  Count `relay_class`'s parked sets again from disk, after something other
  than the Store wrote them (the durability tier's hydration).
  """
  @spec recount(pid() | atom(), String.t()) :: :ok
  def recount(server \\ __MODULE__, relay_class),
    do: GenServer.cast(server, {:recount, relay_class})

  @doc """
  The relay class, tasklet and id an entry reference names, or
  `{:error, {:bad_entry_ref, entry_ref}}` when it is not one.
  """
  @spec parse_ref(String.t()) :: {:ok, String.t(), String.t(), String.t()} | {:error, term()}
  def parse_ref(entry_ref) do
    case String.split(entry_ref, "/") do
      [rc, sh, id] when rc != "" and sh != "" and id != "" -> {:ok, rc, sh, id}
      _ -> {:error, {:bad_entry_ref, entry_ref}}
    end
  end

  @doc "Run a retention sweep now (tests / CLI)."
  @spec sweep(pid() | atom()) :: :ok
  def sweep(server \\ __MODULE__),
    do: GenServer.call(server, :sweep, Durations.get(:dlq_sweep_call_ms))

  @doc """
  Every `{relay_class, tasklet_id}` with a parked-failure directory on
  disk, as the directory names — `nil` lists every class.

  The names are already path-sanitised, which is what makes them safe to use as
  a durable artifact identity: `task_dir/2` maps one back to the same directory.
  """
  @spec tasklets(String.t() | nil) :: [{String.t(), String.t()}]
  def tasklets(relay_class \\ nil) do
    classes =
      case relay_class do
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

  @doc "Directory holding one tasklet's parked failures."
  @spec task_dir(String.t(), String.t()) :: String.t()
  def task_dir(rc, sh), do: Path.join([root(), safe(rc), safe(sh)])

  @doc """
  One identity component as it appears on disk. The durable-tier artifact for a
  parked set is addressed by these, so a caller holding a live relay class
  maps it through here to name the artifact.
  """
  @spec path_component(String.t()) :: String.t()
  def path_component(component), do: safe(component)

  defp root(), do: Layout.dir(:dlq)

  # ---------- GenServer ----------

  # `counts` is `%{{relay_class, tasklet} => %{entries:, bytes:}}` by directory
  # name: built from disk before the first call, then updated from every
  # change `dir_changed/2` publishes.
  @impl true
  def init(_opts) do
    {:ok, arm_sweep(%{counts: %{}}), {:continue, :count}}
  end

  @impl true
  def handle_continue(:count, state), do: {:noreply, count(state, tasklets())}

  @impl true
  def handle_call({:append, entry}, _from, state) do
    {:reply, do_append(entry), state}
  end

  def handle_call({:list, relay_class}, _from, state) do
    entries =
      tasklets(relay_class)
      |> Enum.flat_map(fn {rc, sh} -> load_entries(rc, sh) end)
      |> Enum.sort_by(& &1["ts_ms"])

    {:reply, entries, state}
  end

  def handle_call({:list, relay_class, page}, _from, state) do
    keyed =
      for {rc, sh} <- tasklets(relay_class),
          line <- lines(rc, sh),
          key = line_key(line, rc, sh),
          key != nil,
          do: {key, line}

    %{rows: kept} = answer = Page.slice(keyed, page, &elem(&1, 0))
    rows = for {_key, line} <- kept, do: line |> Jason.decode!() |> Map.put(:node, node())
    {:reply, %{answer | rows: rows}, state}
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
      for {{rc, sh}, usage} <- Enum.sort(state.counts),
          do: Map.merge(%{relay_class: rc, tasklet_id: sh}, usage)

    {:reply, stats, state}
  end

  def handle_call(:sweep, _from, state) do
    do_sweep()
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:recount, relay_class}, state),
    do: {:noreply, count(state, tasklets(relay_class))}

  def handle_cast({:append, entry, on_durable}, state) do
    case do_append(entry) do
      {parked, _ref} = durable when parked in [:ok, :existing] ->
        on_durable.(durable)

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

  def handle_info({:counted, set, nil}, state),
    do: {:noreply, %{state | counts: Map.delete(state.counts, set)}}

  def handle_info({:counted, set, usage}, state),
    do: {:noreply, %{state | counts: Map.put(state.counts, set, usage)}}

  # ---------- append ----------

  defp do_append(entry) do
    rc = Map.fetch!(entry, :relay_class)
    sh = Map.fetch!(entry, :tasklet_id)
    id = entry_id(entry)
    payload = Map.get(entry, :payload)

    case find_entry(rc, sh, id) do
      {:ok, %{"status" => "replaying"} = existing} ->
        bump_generation(rc, sh, existing, entry)

      {:ok, _existing} ->
        # Idempotent re-park (ack-lost re-send or crash-replay): the entry
        # is already durable — re-acking is all the caller needs.
        {:existing, ref(rc, sh, id)}

      {:error, :not_found} ->
        write_new_entry(rc, sh, id, entry, payload)
    end
  end

  defp write_new_entry(rc, sh, id, entry, payload) do
    dir = task_dir(rc, sh)
    File.mkdir_p!(dir)

    if payload do
      blob = blob_path(rc, sh, id)
      File.write!(blob, payload, [:binary])
      fsync_file(blob)
    end

    record = %{
      "id" => id,
      "ts_ms" => System.system_time(:millisecond),
      "relay_class" => rc,
      "tasklet_id" => sh,
      "cell_key" => Map.get(entry, :cell_key, ""),
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
      relay_class: rc,
      tasklet_id: sh,
      cell_key: record["cell_key"],
      channel: record["channel"],
      generation: record["generation"]
    })

    # A re-park of a replayed entry is not a new dead letter, and is not told.
    :telemetry.execute([:flezha, :dlq, :dead_lettered], %{count: 1}, %{
      relay: rc,
      entry: ref(rc, sh, id),
      channel: record["channel"],
      error: record["error"]
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
      relay_class: rc,
      tasklet_id: sh,
      cell_key: existing["cell_key"],
      channel: existing["channel"],
      generation: (existing["generation"] || 1) + 1
    })

    dir_changed(rc, sh)
    {:existing, ref(rc, sh, id)}
  end

  # Origin identity: tier-on parks carry the input-log offset (stable
  # across crash-replays); tier-off parks key on content so ack-lost
  # re-sends and failed-replay re-parks correlate. A byteless tier-off
  # park falls back to its park_id (unique per park attempt).
  defp entry_id(entry) do
    key =
      case {Map.get(entry, :origin_offset), Map.get(entry, :payload)} do
        {offset, _} when is_integer(offset) ->
          "#{Map.get(entry, :cell_key, "")}@#{offset}"

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
      tasklets()
      |> Enum.flat_map(fn {rc, sh} -> load_entries(rc, sh) end)
      |> Enum.sort_by(& &1["ts_ms"])

    {expired, fresh} = Enum.split_with(all, &(&1["ts_ms"] < cutoff_ms))
    Enum.each(expired, &retention_drop(&1, :age))

    total = Enum.sum(Enum.map(fresh, &entry_bytes/1))
    shrink_to_cap(fresh, total, max_bytes)
    release_empty(tasklets())
  end

  defp shrink_to_cap(_entries, total, cap) when total <= cap, do: :ok

  defp shrink_to_cap([oldest | rest], total, cap) do
    retention_drop(oldest, :size)
    shrink_to_cap(rest, total - entry_bytes(oldest), cap)
  end

  defp shrink_to_cap([], _total, _cap), do: :ok

  defp retention_drop(entry, reason) do
    rc = entry["relay_class"]
    sh = entry["tasklet_id"]
    bytes = entry_bytes(entry)

    Logger.warning(
      "[dlq] RETENTION DROP (#{reason}): discarding parked payload " <>
        "#{ref(rc, sh, entry["id"])} channel=#{entry["channel"]} " <>
        "cell=#{entry["cell_key"]} error=#{inspect(entry["error"])} — " <>
        "this data was never replayed and is now permanently lost"
    )

    :telemetry.execute([:flezha, :dlq, :retention_dropped], %{count: 1, bytes: bytes}, %{
      relay_class: rc,
      tasklet_id: sh,
      cell_key: entry["cell_key"],
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

  defp entries_path(rc, sh), do: Path.join(task_dir(rc, sh), @entries_file)

  defp blob_path(rc, sh, id), do: Path.join(task_dir(rc, sh), id <> ".bin")

  defp ref(rc, sh, id), do: "#{rc}/#{sh}/#{id}"

  # Path-safety: identity components come off the wire; never let them climb
  # out of the DLQ root. The character class permits `.` and `-`, so a
  # component that is nothing but dots survives it unchanged and `Path.join/1`
  # then reads it as a move rather than a name; an empty one disappears from
  # the join and puts the file one level up. Each becomes underscores instead,
  # so every component this returns is a name. `...` and longer are ordinary
  # directory names and are left alone.
  defp safe(component) do
    case String.replace(component, ~r/[^A-Za-z0-9._-]/, "_") do
      "" -> "_"
      "." -> "_"
      ".." -> "__"
      cleaned -> cleaned
    end
  end

  defp ok_or({:ok, value}, _default), do: value
  defp ok_or({:error, _}, default), do: default

  defp load_entries(rc, sh) do
    Enum.flat_map(lines(rc, sh), fn line ->
      # A torn trailing line (crash mid-append) is skipped, not fatal.
      case Jason.decode(line) do
        {:ok, %{"id" => _} = entry} -> [entry]
        _ -> []
      end
    end)
  end

  defp lines(rc, sh) do
    case File.read(entries_path(rc, sh)) do
      {:ok, content} -> String.split(content, "\n", trim: true)
      {:error, _} -> []
    end
  end

  # An index line's `page_key/1` without decoding it. `Jason.encode!/1` writes
  # `ts_ms` as a bare number and `id` as hex, and escapes every quote inside a
  # string value, so `"ts_ms":` and `"id":"` occur once each, as the keys. A
  # torn line has no key and is skipped, as `load_entries/2` skips it.
  defp line_key(line, rc, sh) do
    with [_, ts] <- :binary.split(line, ~s("ts_ms":)),
         {ts, _rest} <- Integer.parse(ts),
         [_, id] <- :binary.split(line, ~s("id":")),
         [id, _rest] <- :binary.split(id, ~s(")),
         true <- String.ends_with?(line, "}") do
      page_key(%{
        "ts_ms" => ts,
        "relay_class" => rc,
        "tasklet_id" => sh,
        "id" => id,
        node: node()
      })
    else
      _torn -> nil
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

        # With a backend, the ship just announced has not landed yet; the next
        # retention sweep releases the set instead.
        unless DurabilityTier.enabled?(), do: release_empty([{rc, sh}])
        :ok

      {:error, :not_found} ->
        :ok
    end
  end

  # An emptied set's directory is what keeps it in the durability tier's
  # sweep, so it goes only once the tier has nothing left to ship for
  # it. A restart before the empty set landed would otherwise leave the remote
  # entry naming resolved records, and the relay's next bind would hydrate them.
  defp release_empty(sets) do
    sets
    |> Enum.filter(fn {rc, sh} -> load_entries(rc, sh) == [] end)
    |> Enum.map(fn {rc, sh} -> {:dlq, {safe(rc), safe(sh)}} end)
    |> DurabilityTier.caught_up()
    |> Enum.each(fn {:dlq, {rc, sh}} ->
      send(self(), {:counted, {rc, sh}, nil})
      # Only the empty index is removed: `rmdir` keeps a directory that still
      # holds anything else, and a class directory that still holds a tasklet.
      _ = File.rm(entries_path(rc, sh))
      _ = File.rmdir(task_dir(rc, sh))
      _ = File.rmdir(Path.dirname(task_dir(rc, sh)))
    end)
  end

  defp dir_bytes(rc, sh) do
    dir = task_dir(rc, sh)

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

  # One tasklet's parked set changed: republish the usage gauge, and offer the
  # directory to the durability tier, which ships it whole. A drop has to reach
  # the backend as surely as a park — a stale remote copy would resurrect an
  # entry the user already resolved. Inert while no backend is configured.
  #
  # Every caller runs in the Store, so the new count reaches its state as a
  # message to itself, queued before the reply to the call that changed it.
  defp dir_changed(rc, sh) do
    %{entries: entries, bytes: bytes} = usage = usage({rc, sh})

    :telemetry.execute([:flezha, :dlq, :usage], %{entries: entries, bytes: bytes}, %{
      relay_class: rc,
      tasklet_id: sh
    })

    send(self(), {:counted, {safe(rc), safe(sh)}, usage})
    DurabilityTier.ship({:dlq, {safe(rc), safe(sh)}})
  end

  defp count(state, sets),
    do: %{state | counts: for(set <- sets, into: state.counts, do: {set, usage(set)})}

  defp usage({rc, sh}), do: %{entries: length(load_entries(rc, sh)), bytes: dir_bytes(rc, sh)}

  defp fsync_file(path) do
    {:ok, io} = :file.open(path, [:read, :write, :raw, :binary])

    try do
      :ok = :file.datasync(io)
    after
      :file.close(io)
    end
  end

  defp call(server, request),
    do: GenServer.call(server, request, Durations.get(:dlq_store_call_ms))
end
