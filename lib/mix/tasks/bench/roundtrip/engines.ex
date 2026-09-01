defmodule Mix.Tasks.Bench.Roundtrip.Engines do
  @moduledoc """
  The three engines under test: `passthrough` (the floor), `flezha`, and an
  Apache Flink job submitted over its REST API.

  Each answers `prepare/1` — the world that is already standing when the clock
  starts — and `start/2`, the only part inside the timed window. Takes `cfg` as
  a plain map and names no other part of the task.
  """

  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Mix.Tasks.Bench.Fleet

  defp put_unless_nil(map, _key, nil), do: map
  defp put_unless_nil(map, key, value), do: Map.put(map, key, value)

  def prepare_engine(%{engine: :passthrough}), do: nil

  def prepare_engine(%{engine: :flezha} = cfg) do
    Fleet.setup(cfg.namespace, cfg.runners, checkpoint_env(cfg))
    Fleet.preload_code()
    nil
  end

  def prepare_engine(%{engine: :flink} = cfg), do: upload_jar!(cfg)

  # The checkpoint fixture DECLARES its interval, so the knobs have to reach the
  # runner before `channels()` runs — env, not a control frame.
  defp checkpoint_env(%{durability: :checkpoint} = cfg) do
    [
      {"FLEZHA_BENCH_CHECKPOINT_EVERY", to_string(cfg.checkpoint_every)},
      {"FLEZHA_BENCH_CHECKPOINT_OUTPUTS", to_string(cfg.checkpoint_outputs)},
      {"FLEZHA_BENCH_CHECKPOINT_DELAY_MS", to_string(cfg.checkpoint_delay_ms)}
    ]
  end

  defp checkpoint_env(_cfg), do: []

  def start_engine(%{engine: :passthrough} = cfg, _prepared), do: start_passthrough(cfg)
  def start_engine(%{engine: :flezha} = cfg, _prepared), do: start_flezha(cfg)
  def start_engine(%{engine: :flink} = cfg, jar_id), do: start_flink(cfg, jar_id)

  @doc false
  def start_passthrough(cfg) do
    {startup_us, pids} =
      :timer.tc(fn ->
        Enum.map(0..(cfg.pipelines - 1), fn p ->
          spawn_link(fn -> passthrough_loop(cfg, p) end)
        end)
      end)

    %{startup: %{spawn_us: startup_us}, stop: fn -> Enum.each(pids, &Process.exit(&1, :kill)) end}
  end

  defp passthrough_loop(cfg, partition) do
    {:ok, client} =
      KafkaEx.start_link_worker(:no_name, uris: [{cfg.broker_host, cfg.broker_port}])

    passthrough_step(client, cfg, partition, 0)
  end

  defp passthrough_step(client, cfg, partition, offset) do
    opts = [max_wait_time: 50, min_bytes: 1, max_bytes: 5_000_000]

    case KafkaEx.API.fetch(client, cfg.in_topic, partition, offset, opts) do
      {:ok, %{records: []}} ->
        passthrough_step(client, cfg, partition, offset)

      {:ok, %{records: records}} ->
        messages = Enum.map(records, &%{value: &1.value})
        KafkaEx.API.produce(client, cfg.out_topic, partition, messages, required_acks: 1)
        passthrough_step(client, cfg, partition, List.last(records).offset + 1)

      {:error, _} ->
        Process.sleep(50)
        passthrough_step(client, cfg, partition, offset)
    end
  end

  @doc false
  def start_flezha(cfg) do
    factory = flezha_factory(cfg)

    # Both phases bring every pipeline up at once, the way a partition group's
    # members start; each number is that phase's wall time.
    {create_us, ids} =
      :timer.tc(fn ->
        Flezha.Parallel.map(0..(cfg.pipelines - 1), fn p ->
          id = "rt_#{cfg.run_id}_#{p}"

          {:ok, _} =
            OperativeManager.start_operative(cfg.namespace, %Flezha.Operative.Builder{
              id: id,
              factory_id: factory
            })

          id
        end)
      end)

    {plugin_us, _} =
      :timer.tc(fn ->
        ids
        |> Enum.with_index()
        |> Flezha.Parallel.map(fn {id, p} -> enable_kafka_sink!(cfg, id, p) end)
      end)

    bridges =
      ids
      |> Enum.with_index()
      |> Enum.map(fn {id, p} -> spawn_link(fn -> bridge_loop(cfg, id, p) end) end)

    %{
      startup: %{operative_create_us: create_us, plugin_enable_us: plugin_us},
      stop: fn ->
        Enum.each(bridges, &Process.exit(&1, :kill))
        Fleet.cleanup(cfg.namespace)
      end
    }
  end

  # `do_nothing` echoes the payload verbatim. `bench_keyed` / `tier_store` are
  # the same keyed per-key upsert; only the latter declares effectively-once,
  # which is a compile-time channel property rather than a runtime flag.
  # Tiers are matched to what the other engine is actually doing:
  #   off   — heap-only state, nothing leaves the runner (Flink: no checkpointing)
  #   state — every Change ships to the controller (Flink: checkpointing on)
  #   on    — effectively-once end to end (Flink: checkpointing + EXACTLY_ONCE)
  # `bench_keyed` at `off` would still ship every Change, which is why the
  # no-persist twin is the correct partner for an un-checkpointed Flink job.
  # An impossible axis is refused before a runner starts, by name: every SDK
  # that carries a tier has the fixture for it, and one that defers the tier has
  # nothing to measure. Read from the SDK's own manifest rather than a clause
  # per runtime, so a new SDK's refusals arrive with its manifest.
  # The capability a durability posture needs a fixture for.
  defp flezha_factory(%{workload: :identity}), do: "bench_echo"
  defp flezha_factory(%{workload: :keyed, durability: :off}), do: "bench_keyed_nopersist"
  defp flezha_factory(%{workload: :keyed, durability: :state}), do: "bench_keyed"
  defp flezha_factory(%{workload: :keyed, durability: :on}), do: "tier_store"
  # Exactly-once acknowledged per checkpoint instead of per record — the
  # amortised posture Flink's barrier is; Flink's side of this cell is `on`.
  defp flezha_factory(%{workload: :keyed, durability: :checkpoint}), do: "bench_checkpoint"

  defp enable_kafka_sink!(cfg, operative_id, partition) do
    plugin_id = "#{operative_id}_kafka"

    {:ok, _} =
      PluginManager.enable_plugin(plugin_id, %Flezha.Plugin{
        id: plugin_id,
        namespace: cfg.namespace,
        operative_id: operative_id,
        provider: Flezha.Plugin.Provider.Kafka.Producer,
        opts:
          %{
            hosts: [{cfg.broker_host, cfg.broker_port}],
            topic: cfg.out_topic,
            # One plugin instance is one (topic, partition), so partitions ==
            # pipelines.
            partition: partition,
            linger_ms: cfg.linger_ms,
            queue_max: 100_000,
            # Blocking propagates back-pressure to the bridge, which is what we
            # want to measure; stated so the bench never depends on the default.
            queue_overflow: :drop_nothing,
            queue_block_timeout_ms: 60_000,
            # Single-broker bench cluster; the plugin default of 2 fails.
            auto_create_replication_factor: 1,
            required_acks: cfg.acks,
            idempotent: cfg.idempotent
          }
          |> put_unless_nil(:max_in_flight, cfg.max_in_flight),
        subscriptions: [{:on_output, []}]
      })
  end

  defp bridge_loop(cfg, operative_id, partition) do
    {:ok, client} =
      KafkaEx.start_link_worker(:no_name, uris: [{cfg.broker_host, cfg.broker_port}])

    bridge_step(client, cfg, operative_id, partition, 0)
  end

  defp bridge_step(client, cfg, operative_id, partition, offset) do
    opts = [max_wait_time: 50, min_bytes: 1, max_bytes: 5_000_000]

    case KafkaEx.API.fetch(client, cfg.in_topic, partition, offset, opts) do
      {:ok, %{records: []}} ->
        bridge_step(client, cfg, operative_id, partition, offset)

      {:ok, %{records: records}} ->
        ingest!(cfg, operative_id, records)
        bridge_step(client, cfg, operative_id, partition, List.last(records).offset + 1)

      {:error, _} ->
        Process.sleep(50)
        bridge_step(client, cfg, operative_id, partition, offset)
    end
  end

  # One fetch, one pipelined window of confirming ingests — each record is its
  # own ingest, in order; only the round trips overlap. `:ok`, `{:ok, :absorbed}`
  # and `{:ok, :suspended}` are all terminal success — the operative owns the
  # payload. Same contract the HTTP-poll source uses.
  defp ingest!(cfg, operative_id, records) do
    payloads = Enum.map(records, &{&1.value, %{}})

    OperativeManager.send_io_confirming_many(
      operative_id,
      cfg.namespace,
      payloads,
      30_000,
      cfg.window
    )
    |> Enum.each(fn
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> raise "ingest to #{operative_id} refused: #{inspect(reason)}"
    end)
  end

  @doc false
  def start_flink(cfg, jar_id) do
    args = [
      "--brokers",
      "kafka:9092",
      "--in-topic",
      cfg.in_topic,
      "--out-topic",
      cfg.out_topic,
      "--workload",
      to_string(cfg.workload),
      "--durability",
      if(cfg.durability == :checkpoint, do: "on", else: to_string(cfg.durability)),
      "--checkpoint-interval-ms",
      to_string(cfg.checkpoint_interval_ms)
    ]

    {submit_us, job_ids} =
      :timer.tc(fn ->
        for _ <- 1..cfg.flink_jobs do
          id = flink_run!(cfg, jar_id, args)
          await_running!(cfg, id)
          id
        end
      end)

    %{
      startup: %{submit_to_running_us: submit_us, jobs: length(job_ids)},
      stop: fn -> Enum.each(job_ids, &flink_cancel(cfg, &1)) end
    }
  end

  # Multipart upload via curl. Runs in `prepare_engine/1`, before t0, so startup
  # measures scheduling rather than jar transfer.
  defp upload_jar!(cfg) do
    unless File.exists?(cfg.jar) do
      raise "Flink job jar not found at #{cfg.jar}. Build it with: mvn -f bench/flink/job/pom.xml package"
    end

    {out, 0} =
      System.cmd("curl", [
        "-sS",
        "-X",
        "POST",
        "-H",
        "Expect:",
        "-F",
        "jarfile=@#{cfg.jar}",
        "#{cfg.flink_rest}/jars/upload"
      ])

    %{"filename" => filename} = Jason.decode!(out)
    Path.basename(filename)
  end

  defp flink_run!(cfg, jar_id, args) do
    body = Jason.encode!(%{programArgsList: args, parallelism: cfg.pipelines})

    case http(:post, "#{cfg.flink_rest}/jars/#{jar_id}/run", body) do
      {:ok, %{"jobid" => id}} -> id
      other -> raise "flink run failed: #{inspect(other)}"
    end
  end

  defp await_running!(cfg, job_id, tries \\ 300) do
    case http(:get, "#{cfg.flink_rest}/jobs/#{job_id}", nil) do
      {:ok, %{"state" => "RUNNING"}} ->
        :ok

      {:ok, %{"state" => state}} when state in ~w(FAILED CANCELED FINISHED) ->
        raise "flink job #{job_id} reached terminal state #{state} before RUNNING"

      _ when tries > 0 ->
        Process.sleep(100)
        await_running!(cfg, job_id, tries - 1)

      _ ->
        raise "flink job #{job_id} never reached RUNNING"
    end
  end

  defp flink_cancel(cfg, job_id) do
    http(:patch, "#{cfg.flink_rest}/jobs/#{job_id}?mode=cancel", nil)
  end

  defp http(:get, url, _body), do: do_http(:get, {String.to_charlist(url), []})

  defp http(method, url, body) do
    do_http(method, {String.to_charlist(url), [], ~c"application/json", body || ""})
  end

  defp do_http(method, request) do
    case :httpc.request(method, request, [{:timeout, 30_000}], body_format: :binary) do
      {:ok, {{_, status, _}, _headers, resp}} when status in 200..299 ->
        if resp == "", do: {:ok, %{}}, else: {:ok, Jason.decode!(resp)}

      other ->
        {:error, other}
    end
  end
end
