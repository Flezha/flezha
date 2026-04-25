import Config

config :flezha,
  # The environment the application is running in.
  env: Mix.env(),

  # ------------------------------------------------------------------------------------------------
  # Validator hooks to run before and after boot.
  pre_hooks: [],
  post_hooks: [],

  # ------------------------------------------------------------------------------------------------
  # Failure-recovery subsystem defaults. See `lib/flezha/recovery/` and
  # `openspec/changes/add-failure-recovery-mode` for the full design.
  #
  # - `recovery_window_ms`: time spent in `:bootstrap`/`:recover` before transitioning to
  #   `:normal`. During this window the agent exchanges SYSMODE with peers; seeing a peer
  #   in `:normal` short-circuits local recovery.
  # - `recovery_data_dir`: directory housing the DETS file (`recovery.dets`) of locally
  #   hosted processor child specs. Must be a persistent volume in containerized envs.
  # - `recovery_enabled`: master switch. When `false`, SysMode jumps straight to `:normal`.
  recovery_window_ms: 10_000,
  recovery_data_dir: "app_data/recovery",
  recovery_enabled: true,

  # ------------------------------------------------------------------------------------------------
  # Cluster snapshot/restore subsystem (see docs/cluster-snapshot.md).
  #
  # - `prefer_explicit_restore?`: when true (default), `flz cluster restore`
  #   suppresses the SYSMODE bootstrap-window timer for that boot so the
  #   deprecated DETS-replay loop does not fire. Set to false to keep the
  #   legacy timer behaviour during migration.
  # - `snapshot_state_cap_bytes`: per-processor cap on captured-state size
  #   in the snapshot file. Processors above the cap are written without
  #   their state binary and a WARN line names them; restore primes them
  #   with empty default state.
  prefer_explicit_restore?: true,
  snapshot_state_cap_bytes: 8_388_608,

  # TODO: we need to dynamically load the plugins from .env.
  # ------------------------------------------------------------------------------------------------
  # Enabled plugins.
  enabled_plugins: %{
    "processor_kafka_producer" => Flezha.Plugin.Provider.Kafka.Producer,
    "processor_websocket" => Flezha.Plugin.Provider.Websocket,
    "processor_beam_msg" => Flezha.Plugin.Provider.BeamMsgForward,
    "processor_stdout" => Flezha.Plugin.Provider.StdOut,
    "processor_io_filter" => Flezha.Plugin.Provider.Filter,
    "monitored_io_forward" => Flezha.Plugin.Provider.MonitoredIOForward,
    # `:on_message` provider that forwards a `{:flezha_request, key, payload,
    # reply_to, ref}` envelope to the SDK-side processor via the
    # `ProcessorMessage` control command. Attach at processor start with a
    # subscription keyed on the handler name, e.g. `["on_message",
    # {"name": "fetch_init_state"}]`.
    "processor_message_forward" => Flezha.Plugin.Provider.MessageForward
  },

  # ------------------------------------------------------------------------------------------------
  # Installable plugin subsystem (Jenkins-style runtime plugin lifecycle).
  # See `lib/flezha/plugin/installer/` and `docs/plugin_installation.md`.
  #
  # - `plugin_installer_enabled`: master feature flag. When `false`, the
  #   `Flezha.Plugin.Installer.Supervisor` subtree is not started and the
  #   `Recovery.Coordinator` falls back to its pre-existing behaviour.
  # - `:plugin_installer` keyword list:
  #   - `plugin_root`: filesystem root for staging + installed plugins.
  #     `staging/<uuid>/` holds in-flight extractions;
  #     `installed/<id>/<version>/` holds the canonical layout per plugin.
  #   - `trusted_keys`: list of Ed25519 public keys (raw 32-byte binaries or
  #     PEM-encoded strings) used to verify `signature.sig` over
  #     `CHECKSUMS.sha256` bytes.
  #   - `require_signature`: when `true`, packages without `signature.sig`
  #     are rejected. Implies `allow_unsigned: false`.
  #   - `allow_unsigned`: when `false`, packages without `signature.sig` are
  #     rejected even if `require_signature: false`.
  #   - `max_package_bytes`: uncompressed-size ceiling for `.fpz` archives.
  #   - `max_source_files`: upper bound on files under `src/` per package.
  plugin_installer_enabled: false,
  plugin_installer: [
    plugin_root: "app_data/plugins",
    trusted_keys: [],
    require_signature: false,
    allow_unsigned: true,
    max_package_bytes: 50_000_000,
    max_source_files: 200
  ]

config :logger,
  handle_sasl_reports: true,
  handle_otp_reports: true,
  level: :warning

config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :module, :function]

config :logger, :file_log,
  path: "logs/flezha.log",
  level: :info,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :module, :function, :line],
  rotate: %{max_bytes: 10_485_760, keep: 5}

config :kafka_ex,
  # A list of brokers to connect to. This can be in either of the following formats
  #
  #  * [{"HOST", port}...]
  #  * CSV - `"HOST:PORT,HOST:PORT[,...]"`
  #  * {mod, fun, args}
  #  * &arity_zero_fun/0
  #  * fn -> ... end
  #
  # If you receive :leader_not_available
  # errors when producing messages, it may be necessary to modify "advertised.host.name" in the
  # server.properties file.
  # In the case below you would set "advertised.host.name=localhost"
  # brokers: [
  #   {"kafka", 9092},
  # ],
  #
  # OR:
  # brokers: "localhost:9092,localhost:9093,localhost:9094"
  #
  # It may be useful to configure your brokers at runtime, for example if you use
  # service discovery instead of storing your broker hostnames in a config file.
  # To do this, you can use `{mod, fun, args}` or a zero-arity function, and `KafkaEx`
  # will invoke your callback when fetching the `:brokers` configuration.
  # Note that when using this approach you must return a list of host/port pairs.
  #
  # the default consumer namespace for worker processes, must be a binary (string)
  #    NOTE if you are on Kafka < 0.8.2 or if you want to disable the use of
  #    consumer namespaces, set this to :no_consumer_namespace (this is the
  #    only exception to the requirement that this value be a binary)
  # consumer_namespace: "kafka_ex",

  # The client_id is the logical namespaceing of a set of kafka clients.
  # client_id: "kafka_ex",

  # Set this value to true if you do not want the default
  # `KafkaEx.Server` worker to start during application start-up -
  # i.e., if you want to start your own set of named workers
  disable_default_worker: true,

  # Timeout value, in msec, for synchronous operations (e.g., network calls).
  # If this value is greater than GenServer's default timeout of 5000, it will also
  # be used as the timeout for work dispatched via KafkaEx.Server.call (e.g., KafkaEx.metadata).
  # In those cases, it should be considered a 'total timeout', encompassing both network calls and
  # wait time for the genservers.
  # sync_timeout: 3000,

  # Supervision max_restarts - the maximum amount of restarts allowed in a time frame
  # max_restarts: 10,

  # Supervision max_seconds -  the time frame in which :max_restarts applies
  # max_seconds: 60,

  # Interval in milliseconds that GenConsumer waits to commit offsets.
  # commit_interval: 5_000,

  # Threshold number of messages consumed for GenConsumer to commit offsets
  # to the broker.
  # commit_threshold: 100,

  # The policy for resetting offsets when an :offset_out_of_range error occurs
  # Options:
  # - `:earliest` - Will move to the offset to the oldest available
  # - `:latest` - Will move the offset to the most recent.
  # - `:none` - The error will simply be raised
  # auto_offset_reset: :none,

  # Interval in milliseconds to wait before reconnect to kafka
  # sleep_for_reconnect: 400,

  # This is the flag that enables use of ssl
  # use_ssl: true,

  # see SSL OPTION DESCRIPTIONS - CLIENT SIDE at http://erlang.org/doc/man/ssl.html
  # for supported options
  # ssl_options: [
  # Fix warnings. More at https://github.com/erlang/otp/issues/5352
  # verify: :verify_none,
  # cacertfile: File.cwd!() <> "/ssl/ca-cert",
  # certfile: File.cwd!() <> "/ssl/cert.pem",
  # keyfile: File.cwd!() <> "/ssl/key.pem"
  # ],

  # snappy_module: :snappyer,

  # set this to the version of the kafka broker that you are using
  # include only major.minor.patch versions.  must be at least 0.8.0
  # use "kayrock" for the new client
  kafka_version: "kayrock"

import_config "#{Mix.env()}.exs"
