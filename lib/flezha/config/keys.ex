defmodule Flezha.Config.Keys do
  @moduledoc """
  Every configuration knob the controller, its SDKs, its crates and its shell
  wrappers read, declared once.

  A row is a name, a type, a default, the application-env key a user
  overrides it with, the environment variable `config/runtime.exs` translates
  into that key, a per-`config_env` default, a per-run derivation, a derivation
  over another row, and the runtimes that read the name. Adding a knob is
  writing a row: `config/runtime.exs`, the documentation gate, the seam test and
  the end-to-end drill all iterate this table, so none of them is edited.

  Three ideas hold the table together:

    * **One default.** Whatever `Flezha.Config.get/1`, `config/runtime.exs`, the
      published documentation and every SDK answer for a name, they answer the
      value written here — the row is the only place it is written down.
    * **One parse rule per type.** `:ms` and `:int` take a whole number,
      `:positive_int` refuses anything below one, `:bool` reads
      `1/true/yes/on` and `0/false/no/off` case-insensitively, and each falls
      back to the default rather than raising. `test/fixtures/knob_parse_cases.txt`
      is the same rule fed to every runtime's own reader.
    * **One reader per runtime.** The controller reads `Flezha.Config`; the
      Elixir SDK has an environment reader of its own; the crates read
      `flz_transport::env`; Java reads `FlezhaEnv`. They share the names in
      this table, never a process — a runner reads its own environment, and the controller reads the
      application env its boot filled.

  `Flezha.Config.Durations` is the typed view over the `:ms` rows: it keeps the
  derivation vocabulary (`:factor`, `:headroom`) and answers milliseconds, and
  its rows live here so a duration is not a second table.
  """

  alias Flezha.Config.DurationRows

  @type name :: atom()
  @type key :: atom() | {atom(), atom()} | {atom(), atom(), atom()} | nil
  @type type ::
          :string
          | :path
          | :int
          | :positive_int
          | :ms
          | :bool
          | :atom
          | :address
          | :term
          | {:enum, [atom()]}
          | {:list, type()}
          | {:custom, atom()}
  @type derivation ::
          {:factor, name(), number()}
          | {:headroom, name(), integer()}
          | {:one_of, name()}
          | {:join, name(), binary()}
  @type row :: %{
          name: name(),
          type: type(),
          default: term(),
          key: key(),
          env: binary() | nil,
          env_default: keyword(),
          per_run: atom() | nil,
          derived: derivation() | nil,
          runtimes: [atom()],
          surface: term() | nil,
          owner: atom() | nil,
          on_bad_value: atom() | nil,
          blank: :value | nil,
          min: integer() | nil
        }

  # {name, type, default, opts}.
  #
  # `key: true` is the convention: the application-env key is the row's own
  # name, so a row states a key only when it differs, and a row with no key at
  # all is one no controller process reads (a harness, bench or SDK-side name).
  # A variable is always written out, never derived from the name: it is a
  # PUBLISHED name, and `scripts/check-docs-citations.sh` is what holds the
  # documentation to it — a name no source spells is a name the gate cannot
  # judge.
  #
  # A default may be a value, `{:late, {module, function, args}}` evaluated on
  # read, `{:env, var, fallback}` read from another variable while the boot
  # config is being built, or a per-reader keyword list (`[controller: …,
  # runner: …]`). `env_default:` is the ONLY place a `:test`/`:dev`/`:prod`
  # default lives — what `config/test.exs` used to hold.
  #
  # `min:` is the least value the boot scan (`Environment.check!/1`) accepts,
  # for a number whose zero has no meaning, such as a sweep's interval.
  @declared [
    # ---- the run token and the six resources one controller owns alone ------
    #
    # Two suites on one checkout must not collide. The controller claims its
    # state root exclusively and binds its data-plane port once, so a second
    # session taking the same defaults cannot boot at all. In `:test` each of
    # these derives from the OS process running the suite, so a session
    # isolates itself by existing rather than by remembering the variables. An
    # explicit variable still wins for every one of them.
    {:run_token, :string, nil,
     key: true, runtimes: [:harness], env_default: [test: {:late, {System, :pid, []}}]},
    {:state_root, :path, "storage/app/state",
     key: true,
     env: "FLEZHA_STATE_DIR",
     per_run: :state_root,
     runtimes: [:controller, :shell],
     surface: {:status, :state_root}},
    # The UDS socket directory. Unset by default: `Flezha.Socket.Support`
    # resolves the ladder every SDK shares. A path that is set — a user's,
    # a per-run derivation, or a test's — is used as it stands.
    {:socket_files_path, :path, nil,
     key: true,
     env: "FLEZHA_SOCKET_FILES_PATH",
     per_run: :socket_dir,
     runtimes: [:controller, :elixir_sdk, :rust],
     surface: {:disk, :socket_dir}},
    {:quic_listen_address, :address, "0.0.0.0:4433",
     key: true, env: "FLEZHA_QUIC_LISTEN_ADDRESS", per_run: :quic_address},
    # The WS testnet daemon (`test/apps/testnet`, started in `:dev` and `:test`)
    # binds one port too.
    {:testnet_ws_port, :int, 4500,
     key: true, env: "FLEZHA_TESTNET_WS_PORT", per_run: :ws_port, runtimes: [:shell, :harness]},
    # The release names its node `FLEZHA_NODE_NAME@FLEZHA_NODE_HOST`
    # (`rel/env.sh.eex`) and the test harness starts distribution under the same
    # two keys.
    {:node_base_name, :string, "flezha",
     key: true, env: "FLEZHA_NODE_NAME", runtimes: [:controller, :shell], per_run: :node_name},
    {:node_host_name, :string, "127.0.0.1",
     key: true, env: "FLEZHA_NODE_HOST", runtimes: [:shell, :harness]},
    {:process_hub_registry_root, :path, "storage/app/process_hub",
     key: true, env: "FLEZHA_PROCESS_HUB_REGISTRY_ROOT", per_run: :hub_root},

    # ---- cluster ------------------------------------------------------------
    # Available handlers: "manual_discovery" (default), "k8s_pod_discovery".
    {:cluster_discovery, :string, "manual_discovery", key: true, env: "FLEZHA_CLUSTER_DISCOVERY"},
    {:reconnect_interval, :ms, 1_000, key: true, env: "FLEZHA_RECONNECT_INTERVAL"},
    # The maximum number of jittered reconnect attempts before falling back to
    # the periodic discovery sweep. Greater than 1 so a single transient failure
    # does not strand a peer until the next sweep.
    {:max_reconnects, :positive_int, 5, key: true, env: "FLEZHA_MAX_RECONNECTS"},
    {:cluster_update_interval, :ms, 5_000, key: true, env: "FLEZHA_CLUSTER_UPDATE_INTERVAL"},
    {:initial_nodes, {:list, :atom}, [], key: true, env: "FLEZHA_INITIAL_NODES"},
    {:static_nodes, {:list, :atom}, [], key: true},
    {:concurrent_channel_count, :positive_int, {:late, {System, :schedulers_online, []}},
     key: true, env: "FLEZHA_CONCURRENT_CHANNEL_COUNT"},

    # ---- Kubernetes ---------------------------------------------------------
    {:k8s_service_account_path, :path, "/var/run/secrets/kubernetes.io/serviceaccount",
     key: true, env: "FLEZHA_K8S_SERVICE_ACCOUNT_PATH"},
    {:k8s_cluster_name, :string, "cluster", key: true, env: "FLEZHA_K8S_CLUSTER_NAME"},
    # Not the cluster NAME row's default: an unset cluster name must leave the
    # domain empty rather than take the word "cluster" into every DNS name.
    {:k8s_cluster_domain, :string, {:env, "FLEZHA_K8S_CLUSTER_NAME", ""},
     key: true, env: "FLEZHA_K8S_CLUSTER_DOMAIN"},
    {:k8s_host_identifier, {:enum, [:ip, :hostname, :dns]}, :ip,
     key: true, env: "FLEZHA_K8S_HOST_IDENTIFIER"},
    {:k8s_master_port, :int, 6443, key: true, env: "FLEZHA_K8S_MASTER_PORT"},
    {:k8s_master_name, :string, "", key: true, env: "FLEZHA_K8S_MASTER_NAME"},
    {:k8s_namespace, :string, "flezha", key: true, env: "FLEZHA_K8S_NAMESPACE"},
    {:k8s_ip_lookup_mode, {:enum, [:endpoints, :pods]}, :pods,
     key: true, env: "FLEZHA_K8S_IP_LOOKUP_MODE"},
    {:k8s_service_name, :string, "flezha", key: true, env: "FLEZHA_K8S_SERVICE_NAME"},
    {:k8s_selector, :string, "app=flezha", key: true, env: "FLEZHA_K8S_SELECTOR"},

    # ---- transports ---------------------------------------------------------
    {:transport, {:enum, [:uds, :quic]}, :uds,
     key: true, env: "FLEZHA_TRANSPORT", runtimes: [:controller, :elixir_sdk, :rust, :cli]},
    # Both UDS and QUIC in `:test` so every scenario runs under `mix test`.
    {:transports, {:list, {:enum, [:uds, :quic]}}, nil,
     key: true,
     env: "FLEZHA_TRANSPORTS",
     env_default: [test: [:uds, :quic]],
     derived: {:one_of, :transport}},
    {:controller_enabled, :bool, true, key: true, env: "FLEZHA_CONTROLLER_ENABLED"},
    {:ca_cert_path, :path, "storage/app/certs/ca.pem",
     key: true, env: "FLEZHA_CA_CERT_PATH", runtimes: [:controller, :elixir_sdk, :rust]},
    {:server_cert_path, :path, "storage/app/certs/server.pem",
     key: true, env: "FLEZHA_SERVER_CERT_PATH"},
    {:server_key_path, :path, "storage/app/certs/server.key",
     key: true, env: "FLEZHA_SERVER_KEY_PATH"},
    {:client_cert_path, :path, "storage/app/certs/client.pem",
     key: true, env: "FLEZHA_CLIENT_CERT_PATH"},
    {:client_key_path, :path, "storage/app/certs/client.key",
     key: true, env: "FLEZHA_CLIENT_KEY_PATH"},
    # Peer authentication posture: required by default; the explicit wide-open
    # opt-out is warned on every boot while it is set.
    {:transport_auth_required, :bool, true, key: true, env: "FLEZHA_TRANSPORT_AUTH"},
    # A second trust anchor on purpose: two files a user may point apart.
    {:quic_client_ca_path, :path, "storage/app/certs/ca.pem",
     key: true, env: "FLEZHA_QUIC_CLIENT_CA"},
    # The addresses a runner dials, read by the NIF's diagnosis rather than the
    # controller's own boot.
    {:quic_addresses, :string, "127.0.0.1:4433",
     key: true, env: "FLEZHA_QUIC_ADDRESSES", runtimes: [:elixir_sdk, :rust]},

    # ---- processes the controller spawns ------------------------------------
    {:external_processes, {:list, :string}, [],
     key: true,
     env: "FLEZHA_EXTERNAL_PROCESSES",
     env_default: [test: {:custom, :test_runner_commands}]},
    {:testnet_enabled, :bool, false, key: true, env_default: [test: true, dev: true]},
    # Three keys another config file fills with something that is not a value:
    # the boot hooks' own function bodies, and the environment the build was
    # compiled for. The boot translation reads them and never writes them.
    {:pre_hooks, :term, [], key: true, owner: :config},
    {:post_hooks, :term, [], key: true, owner: :config},
    {:env, :atom, nil, key: true, owner: :config},

    # ---- runners ------------------------------------------------------------
    # The minimum wait before a disconnected runner is removed, so a restart or
    # an update does not evict one. `0` disables the hold.
    {:runner_removal_grace_ms, :ms, 30_000, key: true, env: "FLEZHA_RUNNER_REMOVAL_GRACE_MS"},
    {:bind_restore_concurrency, :positive_int, 16, key: true},
    # The fairness cap on runner selection, as a percentage of the fair share of
    # the candidate set. At `100` the cap is exactly the fair share and the
    # spread is even at every relay count; a value high enough that no
    # candidate ever reaches the cap is plain rendezvous hashing, which is where
    # placement was before the cap existed.
    {:placement_load_cap_percent, :positive_int, 100,
     key: true, env: "FLEZHA_PLACEMENT_LOAD_CAP_PERCENT"},
    {:overwatch_ping_enabled, :bool, true, key: true},
    {:overwatch_ping_fn, :term, nil, key: true},
    {:commit_stall_fn, :term, nil, key: true},
    {:state_resync_fun, :term, nil, key: true},

    # ---- shutdown -----------------------------------------------------------
    # Off under `:test` so the suite's own BEAM keeps kernel's signal handling
    # and `Application.stop/1` at teardown stays fast.
    {:graceful_shutdown_enabled, :bool, true,
     key: true, env: "FLEZHA_GRACEFUL_SHUTDOWN_ENABLED", env_default: [test: false]},

    # ---- migration consent --------------------------------------------------
    # `nil` derives the ceiling from the graceful-shutdown budget.
    {:migration_max_defer_ms, :ms, nil,
     key: {:migration_consent, :max_defer_ms}, env: "FLEZHA_MIGRATION_MAX_DEFER_MS"},

    # ---- observability foundation -------------------------------------------
    # `enabled` is the master flag — when false the supervisor starts no
    # children, and the service API answers `{:error, :observability_disabled}`.
    {:observability_enabled, :bool, true,
     key: {:observability, :enabled}, env: "FLEZHA_OBSERVABILITY_ENABLED"},
    {:observability_ring_buffer_retention_ms, :ms, 900_000,
     key: {:observability, :ring_buffer_retention_ms}},
    {:observability_ring_buffer_max_entries_per_source, :positive_int, 10_000,
     key: {:observability, :ring_buffer_max_entries_per_source}},
    {:observability_cardinality_budget_per_source, :positive_int, 1_000,
     key: {:observability, :cardinality_budget_per_source}},
    {:observability_pubsub_sink_enabled, :bool, true,
     key: {:observability, :pubsub_sink_enabled}},
    # The sources whose signals `Flezha.Observability.LogSink` writes to the log.
    {:observability_log_sources, {:list, :atom}, [],
     key: {:observability, :log_sources}, env: "FLEZHA_OBSERVABILITY_LOG_SOURCES"},
    # Data-plane liveness instrumentation: always-on atomic counters plus the
    # ~1 s driver poll and per-tick stall classifier.
    {:observability_data_plane_liveness, :bool, true,
     key: {:observability, :data_plane_liveness}, env: "FLEZHA_DATA_PLANE_LIVENESS"},
    # Built-in pollers off in tests so unrelated modules pick up no cross-test
    # BEAM or host samples.
    {:observability_otp_backend_enabled, :bool, true,
     key: {:observability, :otp_backend_enabled}, env_default: [test: false]},
    {:observability_poll_intervals, :term,
     %{
       beam_vm: 1_000,
       host: 5_000,
       processhub: 1_000,
       flezha_transport: 1_000
     }, key: {:observability, :otp_backend, :poll_intervals}},

    # ---- observability export -----------------------------------------------
    # Both exporters are OFF by default; turning OTLP on adopts the `:block`
    # overflow policy, bounded by the configured retry window.
    {:prometheus_enabled, :bool, false,
     key: {:observability_export, :prometheus_enabled}, env: "FLEZHA_PROMETHEUS_ENABLED"},
    {:prometheus_port, :int, 9568,
     key: {:observability_export, :prometheus_port},
     env: "FLEZHA_PROMETHEUS_PORT",
     surface: {:observe, :prometheus}},
    {:prometheus_bind, :string, "127.0.0.1",
     key: {:observability_export, :prometheus_bind}, env: "FLEZHA_PROMETHEUS_BIND"},
    {:otlp_enabled, :bool, false,
     key: {:observability_export, :otlp_enabled}, env: "FLEZHA_OTLP_ENABLED"},
    {:otlp_endpoint, :string, nil,
     key: {:observability_export, :otlp_endpoint}, env: "FLEZHA_OTLP_ENDPOINT"},
    {:otlp_headers, :term, %{}, key: {:observability_export, :otlp_headers}},
    {:otlp_flush_interval_ms, :ms, 10_000, key: {:observability_export, :otlp_flush_interval_ms}},
    {:otlp_batch_max, :positive_int, 512, key: {:observability_export, :otlp_batch_max}},

    # ---- state: compaction, watermarks, credit ------------------------------
    # A Cell whose un-snapshotted WAL exceeds this is compacted. A class that
    # declares `compact_after_bytes()` ships it in the `AddRelay` metadata
    # and that per-class value is resolved ahead of this default.
    {:default_compact_after_bytes, :int, 8_388_608, key: true, env: "FLEZHA_COMPACT_AFTER_BYTES"},
    {:wal_high_watermark_bytes, :int, 16_777_216, key: true},
    # The bytes a source's input log admits before its writers wait.
    {:source_credit_window_bytes, :int, nil,
     key: true, derived: {:factor, :wal_high_watermark_bytes, 1}},
    {:wal_low_watermark_bytes, :int, nil,
     key: true, derived: {:factor, :wal_high_watermark_bytes, 0.5}},
    {:wal_hard_watermark_bytes, :int, nil,
     key: true, derived: {:factor, :wal_high_watermark_bytes, 4}},
    {:wal_pending_msg_cap, :positive_int, 5_000, key: true},
    {:outbox_inflight_max, :positive_int, 1_000, key: true},
    # The answers one callee relay owes the callers waiting on it, outside
    # callers and relays alike; a call past it is refused `call.busy`
    # before anything is ingested. A relay's own outbound calls never
    # count against it, its own or a callee's.
    {:call_pending_max, :positive_int, 10_000, key: true, env: "FLEZHA_CALL_PENDING_MAX"},
    # What one listener holds that its handler has not taken: parts are dropped
    # oldest first to stay inside both, and an output or a notice that still
    # does not fit ends the listener `listener.overrun`.
    {:listener_pending_bytes, :positive_int, 4_194_304,
     key: true, env: "FLEZHA_LISTENER_PENDING_BYTES"},
    {:listener_pending_frames, :positive_int, 4_096,
     key: true, env: "FLEZHA_LISTENER_PENDING_FRAMES"},
    # The hook events one copy of a controller plugin has not run yet: an event
    # that does not fit drops the oldest waiting ones.
    {:plugin_backlog_bytes, :positive_int, 4_194_304,
     key: true, env: "FLEZHA_PLUGIN_BACKLOG_BYTES"},
    {:plugin_backlog_frames, :positive_int, 4_096,
     key: true, env: "FLEZHA_PLUGIN_BACKLOG_FRAMES"},
    # The ingest requests one source batch keeps in flight to its relay
    # (`Host.inject_many/4`), so a large fetch is not one mailbox flood.
    {:plugin_ingest_window, :positive_int, 64, key: true, env: "FLEZHA_PLUGIN_INGEST_WINDOW"},
    {:record_log_cut_after_bytes, :int, 65_536, key: true},
    {:default_time_travel_retention_hours, :int, 6, key: true},
    # Unset means half the relay's credit window, which is not a row: the
    # window is resolved per relay, so the site holds the halving.
    {:credit_low_watermark_bytes, :int, nil, key: true},
    # Kill switch: `false` skips the CreditWindowInit offer entirely, reverting
    # every session to the legacy CellMailboxStatus and shed path.
    {:credit_flow_control_enabled, :bool, true, key: true, env: "FLEZHA_CREDIT_FLOW_CONTROL"},
    # The `%{class => hours}` store `Steps.set_retention/3` writes, read ahead
    # of the global default.
    {:time_travel_retention_hours, :term, %{}, key: true},
    {:default_relay_completed_retention_hours, :int, 72,
     key: true, env: "FLEZHA_STEPS_RETENTION_HOURS"},

    # ---- remote durability tier ---------------------------------------------
    # `backend: :none` wires nothing into the supervision tree and leaves every
    # ack path gating on local fsync. An unset variable resolves to `:unset` in
    # production, which is `none` with a warning: `DurabilityTier.children/0`
    # says the decision was never made and boots anyway.
    #
    # `on_bad_value: :keep_raw` is what keeps a typo out of the boot loop. The
    # tier resolves an unrecognised name to `none` and warns naming it, so the
    # text has to survive the parse rather than be refused at translation.
    {:durability_backend, {:enum, [:none, :postgres, :s3, :local_path, :unset]}, :none,
     key: {:durability_tier, :backend},
     env: "FLEZHA_DURABILITY_BACKEND",
     env_default: [prod: :unset],
     on_bad_value: :keep_raw,
     runtimes: [:controller, :bench]},
    # STABLE across restarts of the same controller, and NOT surviving the loss
    # of its disk. `node()` satisfies both under a StatefulSet or a fixed host.
    {:durability_writer_id, :string, {:late, {Kernel, :node, []}},
     key: true, env: "FLEZHA_DURABILITY_WRITER_ID"},
    {:durability_pg_host, :string, "localhost",
     key: {:durability_tier, :backend_opts, :hostname}, env: "FLEZHA_DURABILITY_PG_HOST"},
    {:durability_pg_port, :int, 5432,
     key: {:durability_tier, :backend_opts, :port}, env: "FLEZHA_DURABILITY_PG_PORT"},
    {:durability_pg_database, :string, "flezha",
     key: {:durability_tier, :backend_opts, :database}, env: "FLEZHA_DURABILITY_PG_DATABASE"},
    {:durability_pg_username, :string, "flezha",
     key: {:durability_tier, :backend_opts, :username}, env: "FLEZHA_DURABILITY_PG_USERNAME"},
    # The password itself is never a row: the value names ANOTHER variable, read
    # at use so it lands in no config, worker state or log line.
    {:durability_pg_password_env, :string, nil,
     key: {:durability_tier, :backend_opts, :password_env},
     env: "FLEZHA_DURABILITY_PG_PASSWORD_ENV"},
    {:durability_pg_ssl, :bool, false,
     key: {:durability_tier, :backend_opts, :ssl}, env: "FLEZHA_DURABILITY_PG_SSL"},
    {:durability_pg_table, :string, "flezha_durability_artifacts",
     key: {:durability_tier, :backend_opts, :table}, env: "FLEZHA_DURABILITY_PG_TABLE"},
    {:durability_pg_auto_create, :bool, true,
     key: {:durability_tier, :backend_opts, :auto_create}, env: "FLEZHA_DURABILITY_PG_AUTO_CREATE"},
    {:durability_s3_endpoint, :string, nil,
     key: {:durability_tier, :backend_opts, :endpoint}, env: "FLEZHA_DURABILITY_S3_ENDPOINT"},
    {:durability_s3_bucket, :string, nil,
     key: {:durability_tier, :backend_opts, :bucket}, env: "FLEZHA_DURABILITY_S3_BUCKET"},
    {:durability_s3_region, :string, "us-east-1",
     key: {:durability_tier, :backend_opts, :region}, env: "FLEZHA_DURABILITY_S3_REGION"},
    {:durability_s3_access_key_id, :string, nil,
     key: {:durability_tier, :backend_opts, :access_key_id},
     env: "FLEZHA_DURABILITY_S3_ACCESS_KEY_ID"},
    {:durability_s3_secret_access_key_env, :string, nil,
     key: {:durability_tier, :backend_opts, :secret_access_key_env},
     env: "FLEZHA_DURABILITY_S3_SECRET_ENV"},
    {:durability_s3_prefix, :string, "flezha",
     key: {:durability_tier, :backend_opts, :prefix}, env: "FLEZHA_DURABILITY_S3_PREFIX"},
    # A directory that must outlive the cluster's own disks: a node-local path
    # shares the fate the tier exists to survive.
    {:durability_path, :path, nil,
     key: {:durability_tier, :backend_opts, :path}, env: "FLEZHA_DURABILITY_PATH"},

    # ---- durable audit log --------------------------------------------------
    # User opt-in: default OFF. When enabled the subsystem writes one JSON
    # Lines record per accepted signal to `<dir>/audit-current.jsonl`.
    {:audit_log_enabled, :bool, false, key: {:audit_log, :enabled}, env: "FLEZHA_AUDIT_ENABLED"},
    {:audit_log_dir, :path, "storage/app/audit",
     key: {:audit_log, :dir}, env: "FLEZHA_AUDIT_DIR", surface: {:disk, :audit_dir}},
    {:audit_log_max_file_bytes, :int, 67_108_864, key: {:audit_log, :max_file_bytes}},
    {:audit_log_max_file_age_seconds, :int, 86_400, key: {:audit_log, :max_file_age_seconds}},
    # Retention deletes already-parked data, so the caps default generous.
    {:audit_log_retention_seconds, {:custom, :days_to_seconds}, 2_592_000,
     key: {:audit_log, :retention_seconds}, env: "FLEZHA_AUDIT_RETENTION_DAYS"},
    {:audit_log_max_total_bytes, :int, 4_294_967_296, key: {:audit_log, :max_total_bytes}},
    {:audit_log_fsync_mode, {:enum, [:per_record, :interval]}, :per_record,
     key: {:audit_log, :fsync_mode}},
    {:audit_log_fsync_interval_ms, :ms, 100, key: {:audit_log, :fsync_interval_ms}},
    {:audit_log_rotation_check_interval_ms, :ms, 60_000,
     key: {:audit_log, :rotation_check_interval_ms}},
    {:audit_log_retention_check_interval_ms, :ms, 3_600_000,
     key: {:audit_log, :retention_check_interval_ms}},
    {:audit_log_extra_subscriptions, :term, [], key: {:audit_log, :extra_subscriptions}},

    # ---- dead-letter queue --------------------------------------------------
    {:dlq_dir, :path, "storage/app/dlq",
     key: {:dlq, :dir}, env: "FLEZHA_DLQ_DIR", surface: {:disk, :dlq_dir}},
    {:dlq_retention_seconds, {:custom, :days_to_seconds}, 2_592_000,
     key: {:dlq, :retention_seconds}, env: "FLEZHA_DLQ_RETENTION_DAYS"},
    {:dlq_max_total_bytes, :int, 4_294_967_296, key: {:dlq, :max_total_bytes}},

    # ---- authentication and authorization -----------------------------------
    # Enforcement is the DEFAULT posture in prod. Until `flz auth bootstrap` is
    # claimed, gated verbs refuse while dataflow continues; the explicit opt-out
    # emits a `warn` on every boot.
    {:authz_enabled, :bool, false,
     key: {:authn_authz, :enabled}, env: "FLEZHA_AUTHZ_ENABLED", env_default: [prod: true]},
    # Tests bring up their own stores against a per-test tmp `auth_dir`, so the
    # global tree must start no default supervisor.
    {:authz_application_managed, :bool, true,
     key: {:authn_authz, :application_managed}, env_default: [test: false]},
    {:authz_auth_dir, :path, "storage/app/auth",
     key: {:authn_authz, :auth_dir}, env: "FLEZHA_AUTHZ_DIR", runtimes: [:controller, :cli]},
    {:authz_bootstrap_token_path, :path, nil,
     key: {:authn_authz, :bootstrap_token_path},
     env: "FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH",
     derived: {:join, :authz_auth_dir, "bootstrap.token"}},
    {:authz_token_ttl_default_seconds, :int, 86_400,
     key: {:authn_authz, :token_ttl_default_seconds}},
    {:authz_namespaces, :term, %{}, key: {:authn_authz, :namespaces}},

    # ---- scheduled steps spawns ------------------------------------------
    {:schedule_dir, :path, "storage/app/schedules",
     key: {:schedules, :dir}, env: "FLEZHA_SCHEDULE_DIR"},
    # Same posture as authn_authz: no test gets a stray `:flz_schedules` hub it
    # did not ask for.
    {:schedule_application_managed, :bool, true,
     key: {:schedules, :application_managed}, env_default: [test: false]},

    # ---- plugin packages -------------------------------------------------------
    # The folder of installed `*.fpz` archives, loaded at boot. An archive there
    # is the opt-in: no setting gates loading.
    {:plugin_root, :path, "storage/app/plugins",
     key: {:plugins, :plugin_root}, env: "FLEZHA_PLUGIN_ROOT"},
    # Ed25519 public keys verifying `signature.sig` over `CHECKSUMS.sha256`; the
    # variable names PEM files on disk, comma separated.
    {:plugin_trusted_keys, {:custom, :pem_files}, [],
     key: {:plugins, :trusted_keys}, env: "FLEZHA_PLUGIN_TRUSTED_KEYS"},
    {:plugin_require_signature, :bool, false,
     key: {:plugins, :require_signature}, env: "FLEZHA_PLUGIN_REQUIRE_SIGNATURE"},
    # Bounds an archive's unpacked size and a download alike.
    {:plugin_max_package_bytes, :int, 50_000_000,
     key: {:plugins, :max_package_bytes}, env: "FLEZHA_PLUGIN_MAX_PACKAGE_BYTES"},
    # The declared package list `flz plugin sync` makes the plugin root hold;
    # while it is set, install and uninstall are refused. Set and empty is a
    # list of no packages, which `blank: :value` keeps apart from unset.
    {:plugin_list, :term, nil, key: {:plugins, :list}, env: "FLEZHA_PLUGINS", blank: :value},

    # ---- file plugins ---------------------------------------------------------
    # The one folder or bucket prefix a file plugin may read from or write to.
    # The default is a folder of its own beside the controller's data, never a
    # parent of it, so no plugin path reaches the state, certificates or tokens.
    {:file_plugin_root, :path, "storage/app/files",
     key: true, env: "FLEZHA_FILE_PLUGIN_ROOT", surface: {:status, :file_plugin_root}},
    # An `s3://<bucket>/<prefix>` root is reached with these and never with the
    # durability tier's S3 settings, so credentials a plugin holds cannot reach
    # durable state. The secret is never a row: the value names ANOTHER variable.
    {:file_plugin_s3_endpoint, :string, nil,
     key: {:file_plugin_s3, :endpoint}, env: "FLEZHA_FILE_PLUGIN_S3_ENDPOINT"},
    {:file_plugin_s3_region, :string, "us-east-1",
     key: {:file_plugin_s3, :region}, env: "FLEZHA_FILE_PLUGIN_S3_REGION"},
    {:file_plugin_s3_access_key_id, :string, nil,
     key: {:file_plugin_s3, :access_key_id}, env: "FLEZHA_FILE_PLUGIN_S3_ACCESS_KEY_ID"},
    {:file_plugin_s3_secret_access_key_env, :string, nil,
     key: {:file_plugin_s3, :secret_access_key_env}, env: "FLEZHA_FILE_PLUGIN_S3_SECRET_ENV"},

    # ---- boot and recovery --------------------------------------------------
    # Tests bring up their own runners and relays, so at-boot replay is off
    # under `:test`; the persistent registry still records mutations.
    {:auto_restore_enabled, :bool, true,
     key: true, env: "FLEZHA_AUTO_RESTORE_ENABLED", env_default: [test: false]},

    # ---- convergence --------------------------------------------------------
    # OFF in `:test` so global ticks do not race per-test fixtures.
    {:convergence_health_enabled, :bool, true,
     key: true, env: "FLEZHA_USE_CONVERGENCE_HEALTH", env_default: [test: false]},
    # Consecutive classifier polls reporting a controller-fabric wedge before
    # `/healthz` answers 503. `0` disables the fabric 503 — the escape hatch and
    # the knob are the same setting.
    {:convergence_fabric_503_polls, :int, 3, key: true, env: "FLEZHA_FABRIC_503_POLLS"},

    # ---- /healthz -----------------------------------------------------------
    # Opt-in everywhere; a listener that is up binds `0.0.0.0` so a kubelet
    # httpGet probe reaches the pod IP.
    {:healthz_enabled, :bool, false, key: true, env: "FLEZHA_HEALTHZ_ENABLED"},
    {:healthz_port, :int, 9569,
     key: true, env: "FLEZHA_HEALTHZ_PORT", surface: {:healthz, :port}},
    {:healthz_bind, :string, "0.0.0.0",
     key: true, env: "FLEZHA_HEALTHZ_BIND", surface: {:healthz, :bind}},

    # ---- the file log -------------------------------------------------------
    # `:logger`'s keys, not `:flezha`'s: the names are Flezha's, so the rows
    # live here and `config/runtime.exs` writes them into the logger's own
    # application env. A controller's file log records at `info`; a runner's
    # stdout is quiet at `warn` — one name, two readers, one spelling each.
    {:log_path, :path, "storage/logs/flezha.log",
     env: "FLEZHA_LOG_PATH", runtimes: [:controller]},
    {:log_level, {:enum, [:debug, :info, :warn, :error]}, [controller: :info, runner: :warn],
     env: "FLEZHA_LOG_LEVEL",
     runtimes: [:controller, :elixir_sdk, :rust, :java, :node, :python, :ruby, :php]},
    {:log_max_bytes, :int, 10_485_760, env: "FLEZHA_LOG_MAX_BYTES", runtimes: [:controller]},
    {:log_keep_files, :positive_int, 5, env: "FLEZHA_LOG_KEEP_FILES", runtimes: [:controller]},

    # ---- names read in another process --------------------------------------
    #
    # A runner reads these in ITS OWN process: the names are shared, the readers
    # are not. `Flezha.Config` never answers them for the controller.
    {:mailbox_capacity, :positive_int, 1_024,
     runtimes: [:elixir_sdk, :rust, :java, :node, :python, :ruby, :php],
     env: "FLEZHA_MAILBOX_CAPACITY"},
    {:worker_pool_size, :positive_int, nil,
     runtimes: [:elixir_sdk, :rust, :java, :node, :python, :ruby, :php],
     env: "FLEZHA_WORKER_POOL_SIZE"},
    {:worker_pool_max, :positive_int, nil,
     runtimes: [:elixir_sdk, :rust, :java, :node, :python, :ruby, :php],
     env: "FLEZHA_WORKER_POOL_MAX"},
    {:node_workers, :positive_int, nil, runtimes: [:node], env: "FLEZHA_NODE_WORKERS"},
    # The cadence at which a keyed host sweeps idle cells. The default is
    # the caller's: a quarter of the cell TTL, so a shorter-lived cell is
    # swept sooner without a second row.
    {:reap_interval_ms, :ms, nil, runtimes: [:rust], env: "FLEZHA_REAP_INTERVAL_MS"},
    {:runner_id_path, :path, "/var/lib/flezha-runner/id",
     runtimes: [:rust], env: "FLEZHA_RUNNER_ID_PATH"},
    # The credential a runner proves its namespace with, presented on the
    # control channel at connect. Read by every SDK, because the namespace a
    # runner claims is checked against what its own credential entitles; a
    # deployment whose peers authenticate with a SPIFFE certificate needs none.
    {:namespace_secret, :string, nil,
     runtimes: [:elixir_sdk, :rust, :java, :node, :python, :ruby, :php],
     env: "FLEZHA_NAMESPACE_SECRET"},
    {:runner_start_wait_ms, :ms, 10_000,
     runtimes: [:elixir_sdk, :rust, :java, :node, :python, :ruby, :php],
     env: "FLEZHA_RUNNER_START_WAIT_MS"},
    {:token, :string, nil, runtimes: [:cli], env: "FLEZHA_TOKEN"},
    {:reconnect_ring_capacity, :positive_int, 1_024,
     runtimes: [:rust], env: "FLEZHA_RECONNECT_RING_CAPACITY"},
    {:max_frame_bytes, :int, 67_108_864, runtimes: [:rust], env: "FLEZHA_MAX_FRAME_BYTES"},
    {:effect_max_bytes, :int, 4_194_304,
     key: true, runtimes: [:controller, :rust], env: "FLEZHA_EFFECT_MAX_BYTES"},
    {:max_write_buffer_bytes, :int, 8_388_608,
     runtimes: [:rust], env: "FLEZHA_MAX_WRITE_BUFFER_BYTES"},
    {:send_gate_high_water, :int, nil, runtimes: [:rust], env: "FLEZHA_SEND_GATE_HIGH_WATER"},
    {:send_gate_low_water, :int, nil, runtimes: [:rust], env: "FLEZHA_SEND_GATE_LOW_WATER"},
    {:max_activations, :positive_int, nil, runtimes: [:php], env: "FLEZHA_MAX_ACTIVATIONS"},
    {:max_inflight_activations, :positive_int, 1_024,
     runtimes: [:rust, :python, :ruby], env: "FLEZHA_MAX_INFLIGHT_ACTIVATIONS"},
    {:push_holdback_max, :positive_int, 4_096,
     runtimes: [:rust], env: "FLEZHA_PUSH_HOLDBACK_MAX"},
    {:flush_before_emit, :bool, true, runtimes: [:rust], env: "FLEZHA_FLUSH_BEFORE_EMIT"},
    {:flush_linger_ms, :ms, 5, runtimes: [:rust], env: "FLEZHA_FLUSH_LINGER_MS"},
    {:drop_nothing_nonparking, :bool, false,
     runtimes: [:rust], env: "FLEZHA_DROP_NOTHING_NONPARKING"},
    {:broken_pipe_window_ms, :ms, 5_000, runtimes: [:rust], env: "FLEZHA_BROKEN_PIPE_WINDOW_MS"},
    {:broken_pipe_reconnect_threshold, :positive_int, 10,
     runtimes: [:rust], env: "FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD"},
    {:reconnect_request_retry_ms, :ms, 30_000,
     runtimes: [:rust], env: "FLEZHA_RECONNECT_REQUEST_RETRY_MS"},
    {:credit_park_interval_ms, :ms, 50, runtimes: [:rust], env: "FLEZHA_CREDIT_PARK_INTERVAL_MS"},
    {:control_ack_timeout_ms, :ms, 10_000,
     runtimes: [:rust], env: "FLEZHA_CONTROL_ACK_TIMEOUT_MS"},
    {:control_idle_timeout_ms, :ms, 45_000,
     env: "FLEZHA_CONTROL_IDLE_TIMEOUT_MS", runtimes: [:rust]},
    {:native_binary, :path, nil, runtimes: [:node], env: "FLEZHA_NATIVE_BINARY"},
    {:native_lib_dir, :path, nil, runtimes: [:java, :php], env: "FLEZHA_NATIVE_LIB_DIR"},
    {:socket_dir_group, :string, nil,
     key: true, env: "FLEZHA_SOCKET_DIR_GROUP", runtimes: [:controller, :rust]},
    {:storage_root, :path, "storage", runtimes: [:shell, :cli], env: "FLEZHA_STORAGE_ROOT"},
    {:container_user, :string, nil, runtimes: [:shell], env: "FLEZHA_CONTAINER_USER"},
    {:restore_wait_timeout_s, :int, 60, runtimes: [:shell], env: "FLEZHA_RESTORE_WAIT_TIMEOUT_S"},
    {:shutdown_token_file, :path, nil, runtimes: [:shell], env: "FLEZHA_SHUTDOWN_TOKEN_FILE"},
    {:dist_tls_optfile, :path, nil, runtimes: [:shell], env: "FLEZHA_DIST_TLS_OPTFILE"},
    # Where a release keeps the cookie it minted; unset is `cookie` under the
    # state directory.
    {:cookie_file, :path, nil, runtimes: [:shell], env: "FLEZHA_COOKIE_FILE"},
    {:namespace, :string, nil, runtimes: [:harness], env: "FLEZHA_NAMESPACE"},
    {:demo_namespace, :string, nil, runtimes: [:harness], env: "FLEZHA_DEMO_NAMESPACE"},
    {:quic_address, :address, nil, runtimes: [:harness, :rust], env: "FLEZHA_QUIC_ADDRESS"},
    {:quic_client_cert, :path, "storage/app/certs/client.pem",
     runtimes: [:harness, :elixir_sdk, :rust], env: "FLEZHA_QUIC_CLIENT_CERT"},
    {:quic_client_key, :path, "storage/app/certs/client.key",
     runtimes: [:harness, :elixir_sdk, :rust], env: "FLEZHA_QUIC_CLIENT_KEY"},
    {:testnet_config, :path, nil, runtimes: [:harness], env: "FLEZHA_TESTNET_CONFIG"},
    {:testnet_enabled_env, :bool, nil, env: "FLEZHA_TESTNET_ENABLED", runtimes: [:harness]},
    {:harness_paths, :string, nil, runtimes: [:harness], env: "FLEZHA_HARNESS_PATHS"},
    {:harness_wait_socket, :string, nil, runtimes: [:harness], env: "FLEZHA_HARNESS_WAIT_SOCKET"},
    {:e2e_runtime, :string, "rust", runtimes: [:harness], env: "FLEZHA_E2E_RUNTIME"},
    {:e2e_snapshot, :string, nil, runtimes: [:harness], env: "FLEZHA_E2E_SNAPSHOT"},
    {:steps_e2e_release, :string, nil, runtimes: [:harness], env: "FLEZHA_STEPS_E2E_RELEASE"},
    {:conformance_report, :path, nil, runtimes: [:harness], env: "FLEZHA_CONFORMANCE_REPORT"},
    {:refusal_fixture, :string, nil, runtimes: [:harness], env: "FLEZHA_REFUSAL_FIXTURE"},
    {:branch_fail, :string, nil, runtimes: [:harness], env: "FLEZHA_BRANCH_FAIL"},
    {:steps_retry_fails, :int, nil, runtimes: [:harness], env: "FLEZHA_STEPS_RETRY_FAILS"},
    {:fixture_sleep_after_ms, :ms, nil,
     runtimes: [:harness], env: "FLEZHA_FIXTURE_SLEEP_AFTER_MS"},
    {:reset_probe_override, :string, nil,
     runtimes: [:harness, :cli], env: "FLEZHA_RESET_PROBE_OVERRIDE"},
    {:persistence_relay, :string, nil, runtimes: [:harness], env: "FLEZHA_PERSISTENCE_RELAY"},
    {:persistence_processor, :string, nil,
     runtimes: [:harness], env: "FLEZHA_PERSISTENCE_PROCESSOR"},
    {:keep_stack, :bool, false, runtimes: [:harness], env: "FLEZHA_KEEP_STACK"},
    {:test_s3_secret, :string, nil, runtimes: [:harness], env: "FLEZHA_TEST_S3_SECRET"},
    {:python, :path, nil, runtimes: [:harness], env: "FLEZHA_PYTHON"},
    {:python_executor, :string, nil,
     runtimes: [:harness, :python], env: "FLEZHA_PYTHON_EXECUTOR"},
    {:ruby_executor, :string, nil, runtimes: [:harness, :ruby], env: "FLEZHA_RUBY_EXECUTOR"},
    {:php_executor, :string, nil, runtimes: [:harness, :php], env: "FLEZHA_PHP_EXECUTOR"},
    {:yjit, :bool, false, runtimes: [:harness, :ruby], env: "FLEZHA_YJIT"},
    {:image, :string, nil, runtimes: [:build], env: "FLEZHA_IMAGE"},
    {:rebuild_image, :bool, false, runtimes: [:build], env: "FLEZHA_REBUILD_IMAGE"},
    {:release_base_url, :string, nil, runtimes: [:php], env: "FLEZHA_RELEASE_BASE_URL"},
    {:ui_path, :path, nil, runtimes: [:build], env: "FLEZHA_UI_PATH"},
    # A directory of bare repositories a distribution build fetches from in
    # place of the organisation's remotes.
    {:git_mirror, :path, nil, runtimes: [:build], env: "FLEZHA_GIT_MIRROR"},
    # The Elixir SDK's two build-time names: where its compile reads the
    # contract set, and whether its socket client builds from source.
    {:contract_dir, :path, nil, runtimes: [:elixir_sdk], env: "FLEZHA_CONTRACT_DIR"},
    {:sdk_build_nif, :bool, false, runtimes: [:elixir_sdk], env: "FLEZHA_SDK_BUILD_NIF"},

    # ---- benchmarks ---------------------------------------------------------
    {:bench_log, :path, nil, runtimes: [:bench], env: "FLEZHA_BENCH_LOG"},
    {:bench_skip_build, :bool, false,
     key: true, runtimes: [:bench], env: "FLEZHA_BENCH_SKIP_BUILD"},
    {:bench_checkpoint_every, :positive_int, 500,
     runtimes: [:bench], env: "FLEZHA_BENCH_CHECKPOINT_EVERY"},
    {:bench_checkpoint_outputs, :string, nil,
     runtimes: [:bench], env: "FLEZHA_BENCH_CHECKPOINT_OUTPUTS"},
    {:bench_checkpoint_delay_ms, :ms, 50,
     runtimes: [:bench], env: "FLEZHA_BENCH_CHECKPOINT_DELAY_MS"},
    {:bench_wake_after_ms, :ms, nil, runtimes: [:bench], env: "FLEZHA_BENCH_WAKE_AFTER_MS"},
    {:bench_dyn_msgs, :int, nil, runtimes: [:bench], env: "FLEZHA_BENCH_DYN_MSGS"},
    {:bench_msgs_per_relay, :int, nil, runtimes: [:bench], env: "FLEZHA_BENCH_MSGS_PER_RELAY"},
    {:bench_measurement_secs, :int, 30,
     runtimes: [:bench, :rust], env: "FLEZHA_BENCH_MEASUREMENT_SECS"},
    {:scale_n, :int, nil, runtimes: [:bench], env: "FLEZHA_SCALE_N"},
    {:scale_pool, :int, nil, runtimes: [:bench], env: "FLEZHA_SCALE_POOL"},
    {:scale_shape, :string, nil, runtimes: [:bench], env: "FLEZHA_SCALE_SHAPE"},
    {:scale_hold_ms, :ms, nil, runtimes: [:bench], env: "FLEZHA_SCALE_HOLD_MS"},
    {:scale_max_duration_ms, :ms, nil, runtimes: [:bench], env: "FLEZHA_SCALE_MAX_DURATION_MS"}
  ]

  # Every duration is a row of this table too, declared next door: a
  # millisecond has a vocabulary of its own — a derivation over another row,
  # and a reader that answers only durations.
  @rows Map.new(@declared ++ DurationRows.declared(), fn {name, type, default, opts} ->
          {name,
           %{
             name: name,
             type: type,
             default: default,
             key: if(opts[:key] == true, do: name, else: opts[:key]),
             env: opts[:env],
             env_default: opts[:env_default] || [],
             per_run: opts[:per_run],
             derived: opts[:derived],
             runtimes: opts[:runtimes] || [:controller],
             surface: opts[:surface],
             owner: opts[:owner],
             on_bad_value: opts[:on_bad_value],
             blank: opts[:blank],
             min: opts[:min]
           }}
        end)

  # A name declared twice, or two rows claiming one variable or one key, is a
  # table that answers two things for one question — refused where it is
  # written rather than where it is read.
  @duplicates for {field, values} <- [
                    name: Enum.map(@declared ++ DurationRows.declared(), &elem(&1, 0)),
                    env: for(r <- Map.values(@rows), r.env, do: r.env),
                    key: for(r <- Map.values(@rows), r.key, do: r.key)
                  ],
                  repeated = values -- Enum.uniq(values),
                  repeated != [],
                  do: {field, repeated}

  if @duplicates != [] do
    raise "Flezha.Config.Keys declares the same thing twice: #{inspect(@duplicates)}"
  end

  # A variable is written out, never derived from the row's name, so a name a
  # document publishes is a name a grep finds.
  @unwritten for {name, row} <- @rows, row.env != nil and not is_binary(row.env), do: name

  if @unwritten != [] do
    raise "Flezha.Config.Keys must write each variable out: #{inspect(@unwritten)}"
  end

  @by_env Map.new(for r <- Map.values(@rows), r.env, do: {r.env, r})
  @by_key Map.new(for r <- Map.values(@rows), r.key, do: {r.key, r})

  @doc "The whole table, one row per name."
  @spec rows() :: %{name() => row()}
  def rows(), do: @rows

  @doc "The row `name` declares, or `nil` when it declares none."
  @spec row(name()) :: row() | nil
  def row(name), do: Map.get(@rows, name)

  @doc "The row `name` declares. Raises naming the table when there is none."
  @spec fetch!(name()) :: row()
  def fetch!(name) do
    Map.get(@rows, name) ||
      raise ArgumentError, "no configuration row named #{inspect(name)}"
  end

  @doc """
  The row filling application-env key `key`, or `nil`.

  A subsystem holds the option keys it takes, not the row names that fill
  them, so this is how one asks the table about a setting it knows by key.
  """
  @spec by_key(key()) :: row() | nil
  def by_key(key), do: Map.get(@by_key, key)

  @doc "The row declaring environment variable `var`, or `nil`."
  @spec by_env(binary()) :: row() | nil
  def by_env(var), do: Map.get(@by_env, var)

  @doc """
  The variable `name` is set with, or `nil` when it has none. A knob may be
  addressed by its row name or by the key it fills.
  """
  @spec env_name(name() | key()) :: binary() | nil
  def env_name(key) when is_tuple(key) do
    case by_key(key) do
      nil -> nil
      row -> row.env
    end
  end

  def env_name(name), do: fetch!(name).env
end
