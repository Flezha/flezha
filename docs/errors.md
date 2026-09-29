# Errors

<!-- Generated from flz_transport/errors.json by `mix flezha.errors_doc`; do not edit. -->

Every failure Flezha reports carries one shape, on every surface: the `flz` CLI, the
published consumer API, the SDK administration door, spawn, a runner connect and every
SDK. A program acts on the `code` and the `class`; the `message` is for a person and may
be reworded.

| Field | Meaning |
|---|---|
| `code` | Stable, spelled `family.snake_case`. |
| `class` | What kind of failure it is, and so what a caller can do about it. |
| `message` | One plain sentence naming what it is about. |
| `details` | An object carrying the facts the message names. |
| `hint` | The next command to run, or `null`. |

## Classes and exit codes

A `flz` verb exits with the code of its failure's class. Success is 0.

| Class | Exit code | Meaning |
|---|---|---|
| `internal` | 1 | A defect or an unexpected failure inside Flezha. |
| `invalid` | 2 | The request itself is wrong; fix it and ask again. |
| `unreachable` | 3 | The controller could not be reached. |
| `denied` | 4 | The caller's identity or role does not allow it. |
| `not_found` | 5 | What the request names does not exist. |
| `conflict` | 6 | The current state refuses it; change the state first. |
| `unavailable` | 7 | Not possible right now; it may succeed if retried. |

## Forms

In text, a failure is written on stderr as:

```
error: <message> (<code>)
hint: <hint>
```

Under `--json` or `--output json`, it is one line on stderr:

```json
{"error": {"code": "namespace.not_found", "class": "not_found", "message": "namespace 'ghost' does not exist", "details": {"namespace": "ghost"}, "hint": "flz namespace list"}}
```

The consumer API answers `{:error, %Flezha.Error{}}` with the same five fields, and each
SDK raises or returns its one error type carrying them.

## Codes

### admin

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `admin.args_not_array` | invalid | 2 | the arguments must be a JSON array |  |
| `admin.invalid_args_json` | invalid | 2 | the arguments are not valid JSON: {reason} |  |
| `admin.no_control_plane` | unavailable | 7 | there is no control plane to administer over: the runner is closed or has no controller |  |
| `admin.timeout` | unavailable | 7 | the controller did not answer the administration call within {seconds}s |  |
| `admin.unknown_function` | not_found | 5 | no published function is named '{function}' |  |
| `admin.unknown_parameter` | invalid | 2 | unknown parameter '{parameter}' |  |
| `admin.unreadable_answer` | internal | 1 | the controller's answer to '{verb}' could not be read |  |
| `admin.wrong_arity` | invalid | 2 | '{function}' takes no argument list of this length |  |

### audit

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `audit.closed` | unavailable | 7 | the audit log writer is closed |  |
| `audit.disabled` | unavailable | 7 | the audit log is disabled; enable it with FLEZHA_AUDIT_ENABLED=true, or :audit_log, :enabled in the configuration |  |
| `audit.file_not_found` | not_found | 5 | the audit log has no file '{file}' | `flz audit list` |
| `audit.writer_unavailable` | unavailable | 7 | the audit log writer is not running; try again |  |

### authz

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `authz.bootstrap_claimed` | conflict | 6 | this controller's admin is already claimed, so there is no bootstrap token to claim | `flz auth rebootstrap` |
| `authz.bootstrap_pending` | denied | 4 | authorization is enforced and no admin has claimed the bootstrap yet; its token is in {path} on the controller | `flz auth bootstrap <token>` |
| `authz.bootstrap_token_mismatch` | denied | 4 | the token does not match the one in the bootstrap token file |  |
| `authz.bootstrap_token_missing` | not_found | 5 | no bootstrap token at {path}; a controller writes one at boot while no admin is claimed, relative to the directory it was started from |  |
| `authz.bootstrap_token_read_failed` | internal | 1 | the bootstrap token at {path} could not be read |  |
| `authz.bootstrap_token_write_failed` | internal | 1 | the bootstrap token could not be written to {path} |  |
| `authz.bound_elsewhere` | conflict | 6 | '{principal}' holds roles in namespace '{namespace}'; unbind it there before binding it elsewhere | `flz role unbind {principal}` |
| `authz.cross_namespace_admin_denied` | denied | 4 | an admin of one namespace cannot act in another | `flz auth whoami` |
| `authz.invalid_caller_context` | invalid | 2 | the caller context is not valid: it needs a source of cli, ui, sdk or internal |  |
| `authz.invalid_scope` | invalid | 2 | the call names no valid namespace to be judged in |  |
| `authz.invalid_ttl` | invalid | 2 | a token's lifetime must be a positive number of seconds or never, got {ttl} |  |
| `authz.namespace_mismatch` | denied | 4 | the caller's namespace is not the namespace this acts in | `flz auth whoami` |
| `authz.no_role_binding` | denied | 4 | the caller's identity holds no role | `flz role bind <principal> <role> <namespace>` |
| `authz.permissive_auth_directory` | invalid | 2 | the auth directory {path} is writable by its group or by others, so no bootstrap token is written there; remove those permission bits |  |
| `authz.principal_not_bound` | denied | 4 | the call carries no verifiable identity | `export FLEZHA_TOKEN=<token>` |
| `authz.registry_unavailable` | unavailable | 7 | the token registry is not open yet; try again |  |
| `authz.role_insufficient` | denied | 4 | the caller's role does not allow this | `flz auth whoami` |
| `authz.runner_token_expiry` | invalid | 2 | a runner credential cannot expire | `flz token issue <principal> <namespace> runner --ttl never` |
| `authz.runner_token_system` | invalid | 2 | a runner credential cannot be issued in the _system namespace |  |
| `authz.token_ambiguous` | invalid | 2 | the id '{token}' matches more than one token; give more of it | `flz token list` |
| `authz.token_expired` | denied | 4 | the token has expired | `flz token issue <principal> <namespace> <role>` |
| `authz.token_malformed` | invalid | 2 | the token is not a Flezha token, which starts flzt_ |  |
| `authz.token_not_found` | not_found | 5 | no token has the id '{token}' | `flz token list` |
| `authz.token_revoked` | denied | 4 | the token has been revoked |  |
| `authz.token_unknown` | denied | 4 | the token is not one this controller issued |  |
| `authz.unknown_action` | internal | 1 | the authorization policy knows no action for this call |  |
| `authz.unknown_role` | invalid | 2 | '{role}' is not a role; the roles are viewer, app, ops, admin, runner, spawner and stopper |  |

### boot

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `boot.not_ready` | unavailable | 7 | the controller is not ready: its readiness is {verdict}, with {unmet} unmet | `flz boot status` |

### cli

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `cli.auth_dir_missing` | not_found | 5 | the auth directory {path} does not exist, so there is nothing to recover |  |
| `cli.entropy_unavailable` | internal | 1 | could not read OS entropy for a bootstrap token: {reason} |  |
| `cli.flzvm_not_runnable` | internal | 1 | flz could not run {path}: {reason} |  |
| `cli.runner_check_failed` | unreachable | 3 | a runner on this host cannot reach a controller: {reason} |  |
| `cli.write_failed` | internal | 1 | could not write {path}: {reason} |  |

### commit

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `commit.not_owner` | conflict | 6 | this controller does not own operator '{operator}', so its commit is refused |  |
| `commit.oversized_handoff` | invalid | 2 | a tasklet's handoff of {size} bytes is over the {max}-byte limit |  |
| `commit.record_not_durable` | unavailable | 7 | the record at seq {seq} of Cell '{cell}' did not become durable in time |  |

### controller

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `controller.peer_unreachable` | unavailable | 7 | a peer controller node did not answer the call routed to it |  |
| `controller.process_gone` | unavailable | 7 | the process serving this request is gone; try again |  |
| `controller.shutting_down` | unavailable | 7 | the controller is shutting down |  |
| `controller.stop_unconfirmed` | internal | 1 | the controller answered the stop without saying how it ended |  |
| `controller.timeout` | unavailable | 7 | a part of the controller did not answer in time; try again |  |
| `controller.unreachable` | unreachable | 3 | the controller could not be reached (its control socket is {socket}): it is not up yet or it stopped after it was reached, this shell's cookie (RELEASE_COOKIE, FLEZHA_COOKIE_FILE, FLEZHA_STATE_DIR) is not the controller's, or its node name (FLEZHA_NODE_NAME, FLEZHA_NODE_HOST) is not | `flz controller start` |

### dlq

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `dlq.bad_entry_ref` | invalid | 2 | '{entry}' is not an entry reference; write it as &lt;operator&gt;/&lt;tasklet&gt;/&lt;entry-id&gt; | `flz dlq list` |
| `dlq.not_found` | not_found | 5 | no parked entry '{entry}' | `flz dlq list` |
| `dlq.payload_not_replayable` | conflict | 6 | entry '{entry}' carries no payload bytes (an input between tasklets), so it cannot be replayed; show it or drop it | `flz dlq drop {entry}` |

### durability

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `durability.sync_ship_failed` | unavailable | 7 | operator '{operator}' declares synchronous durability, and shipping its state to the durability backend failed: {reason} |  |

### internal

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `internal.uncatalogued` | internal | 1 | an unexpected failure: {reason} |  |

### namespace

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `namespace.already_exists` | conflict | 6 | namespace '{namespace}' already exists | `flz namespace show {namespace}` |
| `namespace.invalid_name` | invalid | 2 | '{namespace}' is not a valid namespace name: a lowercase letter, then up to 62 lowercase letters, digits, '_' or '-' |  |
| `namespace.invalid_quota_value` | invalid | 2 | quota {field} must be a whole number of zero or more, got {value} |  |
| `namespace.not_found` | not_found | 5 | namespace '{namespace}' does not exist | `flz namespace list` |
| `namespace.not_running` | unavailable | 7 | no runner has connected to namespace '{namespace}', so it runs nothing yet | `flz runner list --namespace {namespace}` |
| `namespace.recovering` | unavailable | 7 | the namespace's recovery did not finish in time; try again | `flz boot status` |
| `namespace.system_undeletable` | invalid | 2 | the _system namespace cannot be deleted |  |
| `namespace.unknown_quota_field` | invalid | 2 | '{field}' is not a quota field; the fields are max_operators, max_cells_per_operator, max_mailbox_bytes_total, max_message_rate_per_second, max_state_bytes and max_runners |  |

### observability

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `observability.disabled` | unavailable | 7 | observability is disabled |  |
| `observability.missing_source` | invalid | 2 | the query names no source | `flz observe sources` |
| `observability.pubsub_disabled` | unavailable | 7 | the PubSub sink is disabled, so no source can be tailed; enable :observability, :pubsub_sink_enabled |  |

### operator

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `operator.already_running` | conflict | 6 | operator '{operator}' is already running | `flz operator list` |
| `operator.busy` | conflict | 6 | the operator is busy moving to another runner; try again |  |
| `operator.channel_without_text` | invalid | 2 | --channel names the entry channel of a first message, and no --text was given |  |
| `operator.factory_kind_ambiguous` | conflict | 6 | factory '{factory}' in namespace '{namespace}' is announced as both unbounded and bounded, and neither is established; run runners of one kind for it |  |
| `operator.factory_kind_undeclared` | unavailable | 7 | factory '{factory}' in namespace '{namespace}' is served only by runners that announce no kind for it, and such a runner is never placed on |  |
| `operator.factory_kind_unserved` | unavailable | 7 | factory '{factory}' in namespace '{namespace}' is established as {kind}, and every runner serving it announces the other kind |  |
| `operator.factory_language_ambiguous` | conflict | 6 | factory '{factory}' in namespace '{namespace}' is served in more than one SDK language and none is established; run one language's runners, or clear the factory's durable state |  |
| `operator.factory_language_undeclared` | unavailable | 7 | factory '{factory}' in namespace '{namespace}' is served only by runners that declare no SDK language, and such a runner is never placed on |  |
| `operator.factory_language_unserved` | unavailable | 7 | factory '{factory}' in namespace '{namespace}' is established in SDK language {language}, and no connected runner of that language serves it; start one, or clear the factory's durable state |  |
| `operator.factory_not_bounded` | invalid | 2 | factory '{factory}' is an unbounded operator, and only a bounded one can be spawned or scheduled |  |
| `operator.factory_not_unbounded` | invalid | 2 | factory '{factory}' is a bounded operator, and only an unbounded one is started here |  |
| `operator.factory_unknown` | not_found | 5 | no runner in namespace '{namespace}' serves factory '{factory}' | `flz runner list --namespace {namespace}` |
| `operator.first_message_not_text` | invalid | 2 | a first message and its channel must be text |  |
| `operator.group_base_taken` | conflict | 6 | '{operator}' names a partition group of {members} live members; start it with --partitions {partitions} to converge it, or stop the group first |  |
| `operator.group_id_too_long` | invalid | 2 | group id '{operator}' is too long for {partitions} partitions: the member suffix leaves at most {max} bytes for it |  |
| `operator.group_not_found` | not_found | 5 | no partition group '{group}' in namespace '{namespace}' | `flz operator list --namespace {namespace}` |
| `operator.group_partition_mismatch` | conflict | 6 | group '{group}' cannot converge to {partitions} partitions: its live member '{member}' declares {declared}, and a start never moves Cell state | `flz operator scale {group} {partitions}` |
| `operator.group_stop_partial` | unavailable | 7 | partition group '{group}' did not stop completely: {failed} did not stop |  |
| `operator.id_too_long` | invalid | 2 | operator id '{operator}' is longer than {limit} bytes |  |
| `operator.input_refused` | unavailable | 7 | operator '{operator}' refused the input because its durable append failed |  |
| `operator.iteration_not_committed` | invalid | 2 | iteration {iteration} has not committed, so there is no boundary to rewind to there |  |
| `operator.member_unreadable` | unavailable | 7 | partition member '{member}' could not be read to rescale from |  |
| `operator.missing_fields` | invalid | 2 | an operator start needs {fields} |  |
| `operator.no_free_id` | conflict | 6 | no free operator id was found for factory '{factory}' in {draws} draws | `flz operator start {factory} --id <id>` |
| `operator.no_runner` | unavailable | 7 | the operator has no runner bound to it yet |  |
| `operator.no_runners` | unavailable | 7 | no runner is connected in namespace '{namespace}' | `flz runner list --namespace {namespace}` |
| `operator.not_accepting_input` | unavailable | 7 | operator '{operator}' is not ready to accept input yet |  |
| `operator.not_found` | not_found | 5 | operator '{operator}' is not running in namespace '{namespace}' | `flz operator list --namespace {namespace}` |
| `operator.not_running` | unavailable | 7 | operator '{operator}' is not running; start it first | `flz operator list` |
| `operator.outside_retention` | invalid | 2 | seq {target} of Cell '{cell}' is older than its retention window; the earliest point kept is seq {earliest} |  |
| `operator.partitioned_first_message` | invalid | 2 | a partitioned start carries no first message, because a group's message reaches its member by key; send it through a keyed forward, or to a member id such as '{member}' once the group runs |  |
| `operator.partitions_out_of_range` | invalid | 2 | partitions must be between 2 and {max}, one per key group; got {partitions} |  |
| `operator.replay_bounded` | invalid | 2 | '{cell}' is a Cell of a bounded operator, whose baseline is a committed iteration across every Cell rather than one Cell's WAL seq, so it cannot be replayed; take the loop back with a rewind | `flz operator rewind <operator> --to-iteration <n>` |
| `operator.rescale_id_held` | unavailable | 7 | the rescale stopped before anything moved: '{member}' still holds its id {timeout_ms} ms after it was stopped; retry once it has gone |  |
| `operator.rescale_move_failed` | internal | 1 | the rescale stopped: a Cell move failed, so the group is stopped with no member started; run the scale again to finish the remaining moves |  |
| `operator.rescale_parked_entries` | conflict | 6 | the group holds parked DLQ entries ({entries}); replay or drop them before a rescale | `flz dlq list` |
| `operator.rescale_quiesce_timeout` | unavailable | 7 | the rescale stopped before anything moved: '{member}' has not consumed its input log within the timeout; retry with a longer timeout, or let a checkpoint or a pass commit advance it |  |
| `operator.rescale_running` | conflict | 6 | a rescale of '{group}' in namespace '{namespace}' is already running; wait for it to finish |  |
| `operator.rewind_uncovered` | conflict | 6 | a rewind to iteration {iteration} would unwind output on {channels}, written by a tasklet that declares no compensate hook, so it is refused |  |
| `operator.start_failed` | internal | 1 | operator '{operator}' did not start; the controller's log names the cause |  |
| `operator.state_claimed` | conflict | 6 | the durable state of '{operator}' belongs to namespace '{owner}' |  |
| `operator.stop_failed` | internal | 1 | operator '{operator}' did not stop: its {step} step failed; the controller's log names the cause |  |
| `operator.stop_timeout` | unavailable | 7 | operator '{operator}' did not finish stopping in time; the steps {skipped} were skipped |  |
| `operator.unknown_fields` | invalid | 2 | an operator start does not read {fields} |  |
| `operator.unknown_opts` | invalid | 2 | the operator options {opts} are not ones Flezha reads |  |
| `operator.unpartitioned_running` | conflict | 6 | '{operator}' is already running as an unpartitioned operator, which a partitioned start cannot converge; stop it first or choose another group id | `flz operator stop {operator}` |
| `operator.unresponsive` | unavailable | 7 | the operator did not answer; try again |  |

### plugin

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `plugin.ack_needs_correlation` | invalid | 2 | sink_ack_mode required needs a sink_template containing the {placeholder} placeholder, because a frame the upstream cannot correlate cannot be acknowledged |  |
| `plugin.activation_failed` | unavailable | 7 | the plugin was prepared but could not start; the controller's log names the cause |  |
| `plugin.already_attached` | conflict | 6 | plugin '{plugin}' is already attached to this operator | `flz plugin list` |
| `plugin.already_enabled` | conflict | 6 | plugin '{plugin}' is already enabled |  |
| `plugin.already_installed` | conflict | 6 | plugin '{plugin}' is already installed | `flz plugin upgrade {plugin} <path>` |
| `plugin.channel_spelled_twice` | invalid | 2 | a {provider} plugin was given channel together with {options}; channel already names both directions, so write it alone or write the pair instead |  |
| `plugin.channel_unused` | invalid | 2 | option '{option}' names a channel this {provider} plugin never uses: it neither feeds the operator on it nor reads it through an on_input or on_output hook |  |
| `plugin.channel_written_twice` | invalid | 2 | payload_metadata names channel '{metadata_channel}' and a channel option names '{channel}'; write one of the two |  |
| `plugin.checksum_file_missing` | invalid | 2 | CHECKSUMS.sha256 lists '{path}', which the package does not contain |  |
| `plugin.checksum_line_missing` | invalid | 2 | package file '{path}' has no line in CHECKSUMS.sha256 |  |
| `plugin.checksum_mismatch` | invalid | 2 | package file '{path}' does not match its line in CHECKSUMS.sha256 |  |
| `plugin.checksum_unreadable` | internal | 1 | package file '{path}' could not be read to check it |  |
| `plugin.checksums_malformed` | invalid | 2 | line {line_number} of CHECKSUMS.sha256 is not a SHA-256 digest followed by a path |  |
| `plugin.compile_failed` | invalid | 2 | package source '{file}' does not compile |  |
| `plugin.dependency_cycle` | invalid | 2 | installing the plugin would make a dependency cycle: {cycle} |  |
| `plugin.dependency_missing` | not_found | 5 | the plugin needs '{dependency}' {constraint}, which is not installed | `flz plugin list --installed` |
| `plugin.dependency_unsatisfied` | conflict | 6 | installed '{dependency}' is version {version}, which does not satisfy {constraint} |  |
| `plugin.dependency_version_invalid` | invalid | 2 | a dependency version or constraint is malformed |  |
| `plugin.dependents_block` | conflict | 6 | installed plugins depend on this one and would break: {dependents} |  |
| `plugin.enabling` | conflict | 6 | plugin '{plugin}' is still being enabled; try again once it is |  |
| `plugin.entry_channel_required` | invalid | 2 | a {provider} plugin needs an entry channel: write channel or channel_in |  |
| `plugin.entry_channel_set` | invalid | 2 | a {provider} plugin injects on exactly one channel, and option '{option}' names several |  |
| `plugin.fields_missing` | invalid | 2 | a plugin needs {fields} |  |
| `plugin.fields_unknown` | invalid | 2 | a plugin does not read {fields} |  |
| `plugin.file_commit_unsupported` | invalid | 2 | file plugin path '{path}' is an S3 object, which is never appended to, so commit {commit} cannot grow it; set commit rename |  |
| `plugin.file_directory_missing` | not_found | 5 | file plugin path '{path}': the directory '{directory}' does not exist |  |
| `plugin.file_directory_unmakeable` | unavailable | 7 | the directory '{path}' that commit rename writes into cannot be made |  |
| `plugin.file_path_not_directory` | invalid | 2 | file plugin path '{path}' is not a directory, and commit rename writes its parts into one |  |
| `plugin.file_path_outside_root` | invalid | 2 | file plugin path '{path}' resolves outside the root '{root}' |  |
| `plugin.file_path_too_many_links` | invalid | 2 | file plugin path '{path}' crosses more than {max} symbolic links |  |
| `plugin.file_path_unreadable` | unavailable | 7 | file plugin path '{path}' cannot be read |  |
| `plugin.file_root_invalid` | unavailable | 7 | the file plugin root '{root}' names no bucket, or climbs out of it |  |
| `plugin.file_root_missing` | unavailable | 7 | the file plugin root folder '{root}' does not exist or is not a directory |  |
| `plugin.file_root_overlaps_durability` | conflict | 6 | the file plugin root '{root}' overlaps the durability tier's objects in bucket '{bucket}': the prefixes {tier_prefix} and {prefix} contain one another |  |
| `plugin.file_root_setting_missing` | unavailable | 7 | the file plugin root '{root}' needs {setting}, which is not set |  |
| `plugin.forward_across_namespaces` | denied | 4 | target_operator_id '{target}' runs in namespace '{target_namespace}', across the namespace boundary |  |
| `plugin.forward_to_self` | invalid | 2 | target_operator_id '{target}' is the operator the plugin is attached to |  |
| `plugin.group_disable_partial` | unavailable | 7 | the plugin could not be disabled on {failed} |  |
| `plugin.group_enable_partial` | unavailable | 7 | the plugin was enabled on {enabled} of {total} partitions and failed on {failed} |  |
| `plugin.headers_invalid` | invalid | 2 | option '{option}' must be a JSON object mapping header names to text or number values |  |
| `plugin.help_failed` | internal | 1 | the help of plugin provider '{provider}' failed to render |  |
| `plugin.hook_invalid` | invalid | 2 | hook {hook} is not a name, a [name, opts] pair, or an object with a name and an opts object |  |
| `plugin.hook_key_missing` | invalid | 2 | an on_message hook needs its key option |  |
| `plugin.hook_key_taken` | conflict | 6 | another plugin already answers key '{key}' on hook '{hook}' |  |
| `plugin.hook_not_implemented` | invalid | 2 | the plugin's provider does not implement hook '{hook}' | `flz plugin help <provider_id>` |
| `plugin.hook_option_invalid` | invalid | 2 | hook option '{option}' does not accept the value {value} |  |
| `plugin.hook_options_not_object` | invalid | 2 | a hook's options must be an object of names and values |  |
| `plugin.hook_position_refused` | invalid | 2 | this hook does not take a position; only a transform or fold hook does |  |
| `plugin.hooks_missing` | invalid | 2 | a plugin needs a list of at least one hook | `flz plugin help <provider_id>` |
| `plugin.id_mismatch` | invalid | 2 | the package is plugin '{package_plugin}', not '{plugin}' |  |
| `plugin.id_taken` | conflict | 6 | plugin id '{plugin}' is already registered for operator '{operator}' in namespace '{namespace}' | `flz plugin list` |
| `plugin.init_after_start` | invalid | 2 | an on_operator_init plugin can only be attached when its operator starts | `flz operator start <factory> --plugins '<json-array>'` |
| `plugin.invalid_regex` | invalid | 2 | option 'regex_pattern' is not a valid regular expression |  |
| `plugin.load_failed` | unavailable | 7 | plugin '{plugin}' failed to load when the controller started, so it cannot be enabled; upgrade or reinstall it | `flz plugin info {plugin}` |
| `plugin.manifest_invalid` | invalid | 2 | the package's manifest.exs is refused: {problem} ({value}) |  |
| `plugin.manifest_not_literal` | invalid | 2 | the package's manifest.exs must be a literal map, with nothing to evaluate |  |
| `plugin.manifest_unparsable` | invalid | 2 | the package's manifest.exs is not valid Elixir |  |
| `plugin.module_collision` | conflict | 6 | the package defines module {module}, which the controller or another plugin already has |  |
| `plugin.module_load_failed` | internal | 1 | module {module} of the package could not be loaded |  |
| `plugin.module_mismatch` | invalid | 2 | the package's sources define {produced}, and its manifest.exs declares {expected} |  |
| `plugin.no_trusted_keys` | denied | 4 | the package is signed, but the controller trusts no key to verify it with |  |
| `plugin.not_found` | not_found | 5 | plugin '{plugin}' is not enabled | `flz plugin list` |
| `plugin.not_installed` | not_found | 5 | plugin '{plugin}' is not installed | `flz plugin list --installed` |
| `plugin.operator_not_declared` | not_found | 5 | the plugin's operator is not declared in its namespace, so nothing records the plugin |  |
| `plugin.option_above_maximum` | invalid | 2 | option '{option}' is {value}, above its maximum of {max} | `flz plugin help <provider_id>` |
| `plugin.option_below_minimum` | invalid | 2 | option '{option}' is {value}, below its minimum of {min} | `flz plugin help <provider_id>` |
| `plugin.option_invalid` | invalid | 2 | option '{option}' does not accept the value {value} | `flz plugin help <provider_id>` |
| `plugin.option_requires` | invalid | 2 | option '{option}' needs option '{required}' as well |  |
| `plugin.options_exclusive` | invalid | 2 | options '{option}' and '{other}' cannot be set together |  |
| `plugin.options_required` | invalid | 2 | a {provider} plugin needs the option {options} | `flz plugin help <provider_id>` |
| `plugin.package_layout_invalid` | invalid | 2 | the package layout is refused ({problem}): a package holds manifest.exs, CHECKSUMS.sha256, a src folder of .ex and .erl files, and optionally signature.sig |  |
| `plugin.package_too_large` | invalid | 2 | the package unpacks to {bytes} bytes, above the limit of {max} |  |
| `plugin.package_too_many_files` | invalid | 2 | the package has {count} source files, above the limit of {max} |  |
| `plugin.package_unreadable` | invalid | 2 | the package is not a readable gzip tar archive |  |
| `plugin.package_unsafe_entry` | invalid | 2 | package entry '{entry}' is refused ({problem}) |  |
| `plugin.partition_placeholder_unfilled` | invalid | 2 | option '{option}' carries {placeholder}, but this plugin has no partition identity; the placeholder is filled only on a partitioned start |  |
| `plugin.prepare_failed` | invalid | 2 | plugin '{plugin}' was refused while its operator started, so the operator did not start |  |
| `plugin.provider_bad_answer` | internal | 1 | the plugin's provider answered with a value it may not return |  |
| `plugin.provider_crashed` | internal | 1 | provider {provider} crashed while preparing the plugin; the controller's log names the cause |  |
| `plugin.provider_not_loaded` | not_found | 5 | provider module {provider} is not loaded; its package may not be installed | `flz plugin list --installed` |
| `plugin.registry_unavailable` | unavailable | 7 | the operator registry of the plugin's namespace did not answer; try again |  |
| `plugin.service_failed` | internal | 1 | the plugin service failed; the controller's log names the cause |  |
| `plugin.service_unavailable` | unavailable | 7 | the plugin service did not answer; try again |  |
| `plugin.signature_invalid` | invalid | 2 | the package signature does not verify against any trusted key |  |
| `plugin.signature_unreadable` | internal | 1 | the package's signature.sig could not be read |  |
| `plugin.spec_field_missing` | invalid | 2 | a plugin spec needs a non-empty '{field}' string |  |
| `plugin.spec_field_not_object` | invalid | 2 | a plugin spec's '{field}' must be a JSON object |  |
| `plugin.spec_invalid_json` | invalid | 2 | the plugins spec is not a JSON array of plugin objects |  |
| `plugin.spec_not_object` | invalid | 2 | each plugin spec must be a JSON object |  |
| `plugin.target_missing` | invalid | 2 | a plugin needs the namespace and the operator it attaches to |  |
| `plugin.template_missing_placeholder` | invalid | 2 | option '{option}' must contain the {placeholder} placeholder |  |
| `plugin.timeout_not_below_interval` | invalid | 2 | timeout_ms {timeout_ms} must be below interval_ms {interval_ms}, so that a poll cannot outlast its own interval |  |
| `plugin.uninstall_enabled` | conflict | 6 | plugin '{plugin}' is enabled; disable it before uninstalling it | `flz plugin disable {plugin}` |
| `plugin.unknown_hook` | not_found | 5 | no hook is named '{hook}' | `flz plugin help <provider_id>` |
| `plugin.unknown_hook_option` | invalid | 2 | a {kind} hook has no option '{option}' |  |
| `plugin.unknown_options` | invalid | 2 | a {provider} plugin has no option {options} | `flz plugin help <provider_id>` |
| `plugin.unknown_provider` | not_found | 5 | no plugin provider is named '{provider}' | `flz plugin list --installed` |
| `plugin.unread_options` | invalid | 2 | flz plugin enable does not read {options}; provider settings are written --X&lt;key&gt;=&lt;value&gt;, and hooks --hook=&lt;name&gt; | `flz plugin help <provider>` |
| `plugin.unsigned_refused` | denied | 4 | the controller accepts only signed plugin packages, and this one carries no signature.sig |  |
| `plugin.upgrade_failed` | internal | 1 | the upgrade failed while swapping the plugin's modules, and the previous version was restored |  |
| `plugin.url_has_credentials` | invalid | 2 | option '{option}' must not embed credentials; put a secret in '{headers_option}' |  |
| `plugin.url_scheme_unsupported` | invalid | 2 | option '{option}' must be an http or https URL, got {url} |  |
| `plugin.url_without_host` | invalid | 2 | option '{option}' names {url}, which has no host |  |
| `plugin.window_needs_idempotence` | invalid | 2 | max_in_flight {max_in_flight} needs idempotent set to true: only the broker's per-partition sequence check keeps a retry inside the window ordered |  |
| `plugin.write_failed` | internal | 1 | the plugin could not be recorded on its operator; the controller's log names the cause |  |

### quota

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `quota.exceeded` | conflict | 6 | the namespace's {kind} quota is reached ({current} of {limit}) | `flz namespace show` |

### reset

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `reset.confirmation_required` | invalid | 2 | flz reset printed its plan and cleared nothing: pass --yes to apply it, or --dry-run to preview without an error | `flz reset --yes` |
| `reset.controller_running` | conflict | 6 | flz reset refuses to run while the controller is up | `flz controller stop` |
| `reset.partial` | internal | 1 | the reset stopped at {path} after clearing some roots: {reason} |  |
| `reset.path_refused` | invalid | 2 | refusing to clear {path}: it is, or overlaps, {collides_with}; --app-data-root or a symlink may alias the two |  |
| `reset.path_unreadable` | internal | 1 | {path} could not be read before anything was cleared: {reason} |  |

### runner

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `runner.bad_reply` | internal | 1 | the runner answered with a reply the controller could not read |  |
| `runner.command_failed` | internal | 1 | the runner refused the command without saying why |  |
| `runner.controller_unreachable` | unreachable | 3 | the runner could not reach a controller: {reason} |  |
| `runner.data_streams_failed` | unavailable | 7 | the runner could not open its data streams: {reason} |  |
| `runner.hub_unavailable` | unavailable | 7 | the operator hub of namespace '{namespace}' could not be started for the runner; try again |  |
| `runner.identity_taken` | conflict | 6 | another live runner already holds identity '{runner}' |  |
| `runner.namespace_not_entitled` | denied | 4 | the runner asked for namespace '{claimed}', and its credential entitles it only to '{proven}' |  |
| `runner.namespace_not_proven` | denied | 4 | the runner proved no namespace: it presented no runner credential, or one this controller does not know | `flz token issue runner@<namespace> <namespace> runner --ttl never` |
| `runner.not_admitted` | unavailable | 7 | the runner's session ended before it was admitted |  |
| `runner.not_connected` | unavailable | 7 | the runner this request needs is not connected |  |
| `runner.quic_addresses_invalid` | invalid | 2 | FLEZHA_QUIC_ADDRESSES cannot be read: {reason} |  |
| `runner.quic_addresses_unresolved` | unreachable | 3 | no FLEZHA_QUIC_ADDRESSES entry resolved: {reason} |  |
| `runner.session_unavailable` | unavailable | 7 | the controller could not start the runner's session; try again |  |
| `runner.socket_dir_refused` | invalid | 2 | the runner could not settle on a controller socket directory: {reason} |  |
| `runner.system_namespace_refused` | denied | 4 | no runner can connect in the _system namespace |  |
| `runner.uds_unavailable` | invalid | 2 | the UDS transport is not available on this platform | `FLEZHA_TRANSPORT=quic` |

### runtime

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `runtime.argument_not_utf8` | invalid | 2 | {argument} is not valid UTF-8 |  |
| `runtime.change_without_apply_change` | invalid | 2 | tasklet '{tasklet}' recorded a change but declares no applyChange, so the change cannot rebuild the state it describes |  |
| `runtime.channel_not_produced` | invalid | 2 | tasklet '{tasklet}' emits on channel '{channel}', which it does not produce on |  |
| `runtime.channel_undeclared` | not_found | 5 | operator '{operator}' has no {kind} channel '{channel}'; it declares {channels} |  |
| `runtime.channel_wrong_kind` | invalid | 2 | operator '{operator}' channel '{channel}' is a {declared} channel, not a {kind} one |  |
| `runtime.class_not_registered` | not_found | 5 | no operator class is registered as '{factory}' on this runner |  |
| `runtime.command_crashed` | internal | 1 | the runner failed while answering the command: {reason} |  |
| `runtime.complete_outside_bounded` | invalid | 2 | only a tasklet running inside a bounded operator can complete it; an unbounded operator never completes |  |
| `runtime.completion_too_many_effects` | invalid | 2 | the completion carries more effects than the activation had items |  |
| `runtime.connect_unconfirmed` | unreachable | 3 | the controller connect was not confirmed within {seconds}s |  |
| `runtime.edge_closure_panicked` | internal | 1 | the edge {closure} panicked: {reason} |  |
| `runtime.event_time_timer_without_event_time_clock` | invalid | 2 | tasklet '{tasklet}' declared clock '{clock}'; an event-time timer needs an event-time clock |  |
| `runtime.executor_unavailable` | unavailable | 7 | the {executor} executor cannot run here: {reason} |  |
| `runtime.factory_in_service` | conflict | 6 | '{module}' cannot be unregistered, because a running runner cannot stop serving a started declaration |  |
| `runtime.factory_kind_mismatch` | conflict | 6 | factory '{factory}' is registered as a {declared} class, and the controller started it as {kind} |  |
| `runtime.forked_process` | invalid | 2 | the core was started in process {owner} and this is process {pid}; fork does not carry the core's threads, so start the core in each child |  |
| `runtime.hook_missing` | invalid | 2 | tasklet '{tasklet}' {action}, and declares no {hook} to handle it |  |
| `runtime.host_command_failed` | internal | 1 | the host failed the {command} activation: {reason} |  |
| `runtime.host_dropped_activation` | internal | 1 | the host context dropped the {activation} activation |  |
| `runtime.host_state_in_process` | internal | 1 | a host-run Cell's state is reached only through its host activation, never in-process |  |
| `runtime.host_state_unreadable` | internal | 1 | the host's state JSON failed to parse: {reason} |  |
| `runtime.host_timeout` | unavailable | 7 | the host did not complete the {activation} activation within {seconds}s |  |
| `runtime.host_wrong_outcome` | internal | 1 | the host answered the {command} activation with an outcome of another kind |  |
| `runtime.identity_too_long` | invalid | 2 | the {part} '{value}' is longer than {limit} bytes |  |
| `runtime.invalid_if_running` | invalid | 2 | if_running must be deliver or error, not '{value}' |  |
| `runtime.invalid_transport` | invalid | 2 | '{transport}' is not a transport; use native, uds or quic |  |
| `runtime.key_not_allowed_on_non_keyed_channel` | invalid | 2 | channel '{channel}' is not keyed, so an emit on it cannot carry a key |  |
| `runtime.key_outside_partition` | invalid | 2 | key '{key}' hashes to key group {group}, which partition {partition} of {partitions} does not own (it owns groups {from} to {to}); the routing item's key must equal the key_by result |  |
| `runtime.key_required_on_keyed_channel` | invalid | 2 | channel '{channel}' is keyed and this tasklet is single-cell, so there is no Cell key to emit under; emit with a key |  |
| `runtime.local_needs_controller` | invalid | 2 | class '{factory}' declares {capability}, which requires a controller; a local run cannot serve it |  |
| `runtime.local_run_busy` | unavailable | 7 | operator '{operator}' was still working after {budget_ms} ms |  |
| `runtime.local_run_unanswered` | unavailable | 7 | the local run did not answer within {budget_ms} ms |  |
| `runtime.max_cells_reached` | unavailable | 7 | the tasklet's max_cells cap is reached, so an input for a new key is dropped |  |
| `runtime.metadata_on_inter_tasklet_channel` | invalid | 2 | channel '{channel}' is between two tasklets, and only a plugin-bound channel carries metadata |  |
| `runtime.no_local_run` | conflict | 6 | there is no local run on this context: start one before feeding it |  |
| `runtime.no_operators` | invalid | 2 | the runner declares no operator; register one before starting it |  |
| `runtime.no_output_channel` | invalid | 2 | tasklet '{tasklet}' emits but declares no output channel |  |
| `runtime.not_a_consumer` | internal | 1 | an edge output names tasklet {tasklet}, which does not consume channel '{channel}' |  |
| `runtime.not_host_tasklet` | internal | 1 | the Cell is not run by a host, so it has no host crossing |  |
| `runtime.operator_busy` | unavailable | 7 | operator '{operator}' did not answer in time; try again |  |
| `runtime.operator_message_unsupported` | unavailable | 7 | operator '{operator}' does not handle direct messages |  |
| `runtime.operator_not_found` | not_found | 5 | no instance of operator '{operator}' runs on this runner |  |
| `runtime.operator_type_in_use` | conflict | 6 | factory '{factory_id}' has {live_instances} live operator instance(s); stop them before removing the type |  |
| `runtime.plugin_serializer_panicked` | internal | 1 | the plugin serializer of channel '{channel}' panicked: {reason} |  |
| `runtime.prepare_unknown_channel` | invalid | 2 | the prepare answer names a channel the operator does not declare |  |
| `runtime.reserved_metadata_key` | invalid | 2 | metadata key '{key}' is in the reserved 'flz.' namespace, which only the runtime stamps |  |
| `runtime.result_undecodable` | invalid | 2 | the activation result failed to decode: {reason} |  |
| `runtime.runner_already_running` | conflict | 6 | the runner on this context is already started |  |
| `runtime.runner_closed` | unavailable | 7 | the runner of namespace '{namespace}' is closed |  |
| `runtime.runner_not_started` | conflict | 6 | the runner is not started; start it first |  |
| `runtime.runner_shutting_down` | unavailable | 7 | the runner is shutting down, so the {activation} activation was not handed to the host |  |
| `runtime.runner_unavailable` | unavailable | 7 | the runner thread is gone or answered out of turn: {reason} |  |
| `runtime.schema_mismatch` | invalid | 2 | activation schema version mismatch: the shim speaks {shim}, the core speaks {core} |  |
| `runtime.spawn_handle_released` | not_found | 5 | spawn handle {handle} was released: the activation that made it was completed or abandoned |  |
| `runtime.state_mutated` | invalid | 2 | tasklet '{tasklet}' assigned state.{path} directly, which a restart loses; record it with a change instead |  |
| `runtime.state_unencodable` | internal | 1 | the state could not be encoded as JSON: {reason} |  |
| `runtime.tasklet_failed` | internal | 1 | {error} |  |
| `runtime.timer_cap_exceeded` | conflict | 6 | tasklet '{tasklet}' reached its timer cap ({max}) |  |
| `runtime.timer_without_on_timer` | invalid | 2 | tasklet '{tasklet}' registered a timer but declares no onTimer |  |
| `runtime.timers_unavailable` | invalid | 2 | timers are not available here; only a tasklet's run or on_timer may register one |  |
| `runtime.unknown_activation_token` | invalid | 2 | activation token {token} is unknown or already answered |  |
| `runtime.unknown_channel` | not_found | 5 | channel '{channel}' is not declared on this operator |  |
| `runtime.unknown_channel_id` | internal | 1 | an edge output names unknown channel {channel_id} |  |
| `runtime.unreadable_reply` | internal | 1 | the controller's reply could not be read: {reply} |  |
| `runtime.value_not_json` | invalid | 2 | {owner} sends a {type} that has no JSON form: {reason} |  |
| `runtime.value_wrong_for_codec` | invalid | 2 | channel '{channel}' names the {codec} codec and cannot send a {type} |  |

### schedule

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `schedule.already_exists` | conflict | 6 | a schedule named '{schedule}' already exists | `flz schedule list` |
| `schedule.cron_field_count` | invalid | 2 | cron expression '{cron}' must have 5 fields (minute hour day month weekday), got {fields} |  |
| `schedule.cron_invalid_field` | invalid | 2 | cron expression '{cron}': the {field} field has an invalid element '{element}' (allowed {min}-{max}) |  |
| `schedule.cron_required` | invalid | 2 | a schedule needs a cron expression |  |
| `schedule.factory_required` | invalid | 2 | a schedule needs the factory it starts |  |
| `schedule.init_values_not_object` | invalid | 2 | a schedule's init values must be a JSON object |  |
| `schedule.invalid_name` | invalid | 2 | schedule name '{schedule}' must match [A-Za-z0-9][A-Za-z0-9_-]* and be at most {max} bytes, so that each firing's '&lt;name&gt;@&lt;slot&gt;' fits an operator id |  |
| `schedule.invalid_timezone` | invalid | 2 | '{timezone}' is not an IANA time zone |  |
| `schedule.name_required` | invalid | 2 | a schedule needs a name |  |
| `schedule.not_found` | not_found | 5 | schedule '{schedule}' does not exist | `flz schedule list` |
| `schedule.plugins_not_array` | invalid | 2 | a schedule's plugins must be a JSON array of plugin specs |  |
| `schedule.timezone_required` | invalid | 2 | a schedule needs a time zone |  |

### shutdown

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `shutdown.coordinator_exited` | internal | 1 | the shutdown coordinator stopped before reporting an outcome; the controller's log names the cause |  |
| `shutdown.partial_drain` | unavailable | 7 | the shutdown drained partially: operators were still on this node when its timeout ran out |  |

### spawn

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `spawn.timeout` | unavailable | 7 | no spawn result arrived within {seconds}s |  |
| `spawn.unavailable` | unavailable | 7 | there is no control plane to request a spawn on: the runner is closed or has no controller |  |

### state

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `state.apply_failed` | invalid | 2 | the state could not be applied: {reason} |  |
| `state.apply_not_supported` | unavailable | 7 | this runner does not apply state to a live operator |  |
| `state.bad_cell_path` | invalid | 2 | '{path}' is not a Cell path; write it as &lt;operator&gt;/&lt;tasklet&gt;/&lt;cell_key&gt; |  |
| `state.bad_live_path` | invalid | 2 | '{path}' is not a live state path; write it as &lt;operator&gt;/&lt;tasklet&gt;[/&lt;cell_key&gt;] |  |
| `state.cell_busy` | unavailable | 7 | Cell '{cell}' is inside an activation older than {seconds}s |  |
| `state.cell_evicted` | not_found | 5 | Cell '{cell_key}' of tasklet '{tasklet}' was idle-evicted; eviction is destructive, so there is no durable value to serve | `flz state at <operator>/{tasklet}/{cell_key} <seq\|ts>` |
| `state.cell_key_without_tasklet` | invalid | 2 | a Cell key is unique only within its tasklet, so name the tasklet as well |  |
| `state.cell_not_found` | not_found | 5 | tasklet '{tasklet}' holds no Cell keyed '{cell_key}' | `flz state at <operator>/{tasklet}/{cell_key} <seq\|ts>` |
| `state.cell_not_live` | not_found | 5 | no live Cell matches cell id '{cell_id}' |  |
| `state.inbox_unreadable` | unavailable | 7 | the input log of '{instance}' did not answer |  |
| `state.init_values_invalid_json` | invalid | 2 | init_values is not valid JSON: {reason} |  |
| `state.init_values_not_object` | invalid | 2 | init_values must be a JSON object, got {kind} |  |
| `state.migration_unreadable` | invalid | 2 | the migrated state is not UTF-8 text: {reason} |  |
| `state.no_operator` | not_found | 5 | no running operator '{operator}' in any namespace | `flz operator list` |
| `state.no_owning_runner` | unavailable | 7 | no connected runner owns operator '{operator}', so nothing can compact its Cell |  |
| `state.no_persisted_state` | not_found | 5 | Cell '{cell}' has no persisted state | `flz state list <operator>` |
| `state.not_resident` | unavailable | 7 | operator '{operator}' is {state}, so no Cell of it is held in memory; read its durable state instead | `flz state at <operator>/<tasklet>/<cell_key> <seq>` |
| `state.oversized_read` | invalid | 2 | the selected state is {bytes} bytes, over the {limit}-byte read ceiling; narrow the read to one cell key | `flz state get <operator>/<tasklet>/<cell_key>` |
| `state.persistence_off` | conflict | 6 | operator '{operator}' declares persistence off, so its Cells hold no durable state and live reads are refused |  |
| `state.restore_failed` | internal | 1 | the Cell's state could not be restored: {reason} |  |
| `state.serialize_failed` | internal | 1 | the state of tasklet '{tasklet}' failed to serialize: {reason} |  |
| `state.snapshot_failed` | internal | 1 | the Cell's snapshot could not be taken: {reason} |  |
| `state.tasklet_not_found` | not_found | 5 | operator '{operator}' has no tasklet named '{tasklet}' |  |

### topology

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `topology.ambiguous_operator_kind` | invalid | 2 | '{module}' declares both an unbounded and a bounded operator; an operator is one kind |  |
| `topology.at_checkpoint_without_max_delay` | invalid | 2 | checkpoint_outputs at_checkpoint requires checkpoint_max_delay_ms: an interval that never closes would hold its outputs forever on a quiet Cell |  |
| `topology.bounded_setting_on_unbounded` | invalid | 2 | unbounded operator '{factory_id}' declares {settings}, which only a bounded operator carries |  |
| `topology.branch_too_few_arms` | invalid | 2 | branch requires at least two arms; {arms} given |  |
| `topology.branching_chained_loop` | invalid | 2 | a handoff-chained loop needs exactly one terminal tasklet per chain; the chain of entry channel '{channel}' has {leaves} |  |
| `topology.cell_keys_without_static_mode` | invalid | 2 | tasklet '{tasklet}' declares cell_keys without static mode |  |
| `topology.cells_for_missing` | invalid | 2 | tasklet '{name}' declares non-empty cell_keys but does not override cells_for; write its cells_for (a Rust tasklet also sets CELLS_FOR_OVERRIDDEN) |  |
| `topology.chained_loop_terminal_without_handoff` | invalid | 2 | a durable handoff-chained loop's terminal tasklet '{tasklet}' releases no inter-tasklet handoff; iteration N+1 is seeded from iteration N's terminal batch, and a tasklet that owns no output channel, or whose only output is plugin-bound, has nothing the journal carries. Take the sink off the terminal's own channel, or declare the loop per_input; wiring another tasklet behind the sink only moves the rule onto that one |  |
| `topology.change_without_state` | invalid | 2 | tasklet '{tasklet}' declares applyChange but no initial state, so there is nothing for a change to fold into |  |
| `topology.channel_name_too_long` | invalid | 2 | channel '{channel}' has a {len}-byte name; the name rides every frame this channel produces, under a one-byte length prefix, so it may be at most {max} bytes |  |
| `topology.channel_named_twice` | invalid | 2 | channel '{channel}' is named twice: named('{named}') and sink('{sink}') both name it. A sink names the channel it terminates, so write the name once: sink('{named}') to keep the channel's name, or named('{named}') with a sink that takes no name |  |
| `topology.checkpoint_ack_without_tier` | invalid | 2 | the operator declares checkpoint_ack without effectively_once; the checkpoint is where the tier's consumed offset advances, and an at-least-once operator has no durable offset to hold back (nor a durable input log to replay the interval from) |  |
| `topology.checkpoint_every_zero` | invalid | 2 | the operator declares checkpoint_every(0); an interval that closes every zero inputs is not an interval, so omit it to close only on an explicit checkpoint call |  |
| `topology.checkpoint_max_delay_zero` | invalid | 2 | checkpoint_max_delay_ms must be greater than zero |  |
| `topology.checkpoint_on_bounded` | invalid | 2 | a bounded class cannot declare a checkpoint (checkpoint_ack, checkpoint_every, checkpoint_outputs, checkpoint_max_delay_ms); a bounded commits at its stage boundary |  |
| `topology.class_not_constructible` | invalid | 2 | class '{class}' could not be constructed through a public no-argument constructor: {reason} |  |
| `topology.codec_with_callable` | invalid | 2 | channel '{channel}' names both a codec and its own callable; give one |  |
| `topology.consumer_out_of_range` | invalid | 2 | channel '{channel}' names consumer {consumer} but the class declares {tasklets} tasklet(s) |  |
| `topology.correction_window_on_bounded` | invalid | 2 | a bounded class cannot declare correction_window_ms |  |
| `topology.correction_window_zero` | invalid | 2 | the operator declares correction_window(0); a zero window can correct nothing |  |
| `topology.correction_without_event_time` | invalid | 2 | the operator declares correction_window but tasklet '{tasklet}' declares clock '{clock_type}'; correction orders a replayed slice by event time, which only an event-time clock yields |  |
| `topology.correction_without_tier` | invalid | 2 | the operator declares correction_window without effectively_once; correction replays the input log against a rewound baseline, and only the tier journals inputs before forwarding them (an at-least-once operator has no durable input to replay) |  |
| `topology.cycle` | invalid | 2 | tasklet '{tasklet}' is on a cycle; an operator topology is a DAG, so a node's output may never reach its own input |  |
| `topology.dangling_transform_chain` | invalid | 2 | channel '{channel}' declares a transform or key_by that is never closed by into; a transform chain is stored on the consumer edge it terminates in, so a dangling one would silently do nothing |  |
| `topology.declaration_unreadable` | invalid | 2 | the class declaration is not valid JSON for the schema: {reason} |  |
| `topology.deferred_builder_surface` | invalid | 2 | operator '{module}' uses a builder call this SDK does not have yet: {reason} |  |
| `topology.dropping_policy_under_effectively_once` | invalid | 2 | channel '{channel}' declares on_full({policy}) on an operator that declares effectively_once; a shed input's offset is still advanced past by the tier's watermark, so the loss is permanent and reported nowhere. Declare one or the other, not both (a correction window implies the tier, so it is refused here too) |  |
| `topology.duplicate_channel` | invalid | 2 | channel '{channel}' is declared more than once on this operator |  |
| `topology.duplicate_factory_id` | invalid | 2 | factory '{factory_id}' is already declared on this runner |  |
| `topology.duplicate_key_by` | invalid | 2 | channel '{channel}' declares key_by twice before one into |  |
| `topology.duplicate_merge_source` | invalid | 2 | merge subscribes channel '{channel}' twice; each merge source must be a distinct channel (one mapping per channel into the merged input) |  |
| `topology.duplicate_persistent_tasklet` | invalid | 2 | tasklet '{tasklet}' journals changes and is wired as more than one node; a changeful tasklet may appear once per class |  |
| `topology.duplicate_plugin_sink` | invalid | 2 | channel '{channel}' was declared with sink more than once; only one plugin sink per channel is allowed |  |
| `topology.duplicate_sink_name` | invalid | 2 | channel '{channel}' is named by two plugin sinks; a sink names the channel it terminates, so two channels cannot name one sink. Give each its own name; a plugin still reaches both by attaching to the operator with no channel filter |  |
| `topology.edge_already_wired` | invalid | 2 | channel '{channel}' already feeds tasklet '{tasklet}'; an edge carries exactly one transform and one key or neither, so a re-declaration where either side carries one is refused rather than silently keeping the first |  |
| `topology.effectively_once_on_bounded` | invalid | 2 | a bounded class cannot declare effectively_once |  |
| `topology.effectively_once_without_persistence` | invalid | 2 | effectively_once requires persistent: the tier is durable state |  |
| `topology.entry_channel_with_producer` | invalid | 2 | entry channel '{channel}' cannot name a producer |  |
| `topology.entry_channel_without_consumer` | invalid | 2 | entry channel '{channel}' names no consumer |  |
| `topology.external_stop_only` | invalid | 2 | bounded class '{factory_id}' can only end by an external stop: let its chain complete, have a tasklet call complete, or declare max_duration |  |
| `topology.inter_task_channel_without_producer` | invalid | 2 | inter_task channel '{channel}' names no producer |  |
| `topology.invalid_mode` | invalid | 2 | operator '{factory_id}' sets {knob} to {value}, which is not one of its modes |  |
| `topology.invalid_setting` | invalid | 2 | {declaration} sets {setting} to {value}, and it must be {expected} |  |
| `topology.invalid_tasklet` | invalid | 2 | tasklet '{tasklet}' cannot be declared: {reason} |  |
| `topology.key_by_on_unkeyable_target` | invalid | 2 | channel '{channel}' declares key_by into tasklet '{tasklet}', which declares non-empty cell_keys (static mode); static keying routes by cells_for and may name several Cells, edge keying names exactly one, so declare one or the other, not both |  |
| `topology.keyed_channel_without_plugin_wire` | invalid | 2 | channel '{channel}' is declared keyed but has no plugin wire; output identity stamps only exist on plugin-bound frames, so add a sink |  |
| `topology.keyed_on_entry` | invalid | 2 | entry channel '{channel}' is declared keyed; only a plugin-bound output channel carries a natural key |  |
| `topology.mailbox_capacity_too_large` | invalid | 2 | tasklet '{tasklet}' declared mailbox_capacity {value}; capacity must be between 1 and 1048576 |  |
| `topology.mailbox_capacity_zero` | invalid | 2 | tasklet '{tasklet}' declared mailbox_capacity 0; capacity must be between 1 and 1048576 |  |
| `topology.max_cells_zero` | invalid | 2 | dynamic tasklet '{tasklet}' declares max_cells 0 |  |
| `topology.merge_too_few_sources` | invalid | 2 | merge into tasklet '{tasklet}' declares {sources} source channel(s); a merge consumes two or more, so wire a single source with into instead |  |
| `topology.missing_body` | invalid | 2 | {declaration} declares no body; it needs {body} |  |
| `topology.missing_factory_id` | invalid | 2 | operator '{operator}' declares no factory id, and every operator needs one |  |
| `topology.multiple_output_channels` | invalid | 2 | tasklet '{tasklet}' produces more than one channel; a tasklet emits on exactly one output channel |  |
| `topology.named_on_entry` | invalid | 2 | entry channel '{channel}' cannot be renamed with named; an entry channel is named where it is declared |  |
| `topology.negative_setting` | invalid | 2 | tasklet '{tasklet}' sets {setting} to {value}, and it must not be negative |  |
| `topology.no_bounded_body` | invalid | 2 | bounded class '{factory_id}' declares no channels body, so it has no tasklets to run |  |
| `topology.no_entry_channel` | invalid | 2 | the bounded class declares no entry channel for its messages to arrive on |  |
| `topology.no_operator_exported` | invalid | 2 | module '{module}' exports no operator declaration |  |
| `topology.no_tasklets` | invalid | 2 | the class declares no tasklet; a host class declares at least one |  |
| `topology.node_identity_collision` | invalid | 2 | node identity '{name}' is declared by two distinct nodes; a node name must be unique per operator and distinct from every tasklet name |  |
| `topology.operator_field_on_tasklet` | invalid | 2 | tasklet '{tasklet}' declares {fields}, which an operator declaration carries; declare the short form with {instead} |  |
| `topology.partitions_without_keyed_tasklet` | invalid | 2 | factory '{factory}' has no keyed entry-fed tasklet, so each partition would fold a slice of the input into its own copy of the same state; declare key_by on the entry edge before partitioning |  |
| `topology.partitions_without_per_input_loop` | invalid | 2 | factory '{factory}' reads one message (loop_mode {loop_mode}), so its {partitions} partitions would each wait for a message that reaches only one of them; declare loop_mode per_input before partitioning |  |
| `topology.payload_type_mismatch` | invalid | 2 | channel '{channel}' delivers {delivered}, which does not match the input type {input} of tasklet '{tasklet}' |  |
| `topology.plugin_channel_with_consumers` | invalid | 2 | plugin channel '{channel}' cannot name consumers |  |
| `topology.producer_out_of_range` | invalid | 2 | channel '{channel}' names producer {producer} but the class declares {tasklets} tasklet(s) |  |
| `topology.retired_callback` | invalid | 2 | tasklet '{tasklet}' defines {callback}, which no longer exists; use {instead} |  |
| `topology.retry_max_zero` | invalid | 2 | channel '{channel}' declared retry_max 0; at least one attempt is required (fail an input as non-retryable to park it without retries) |  |
| `topology.schema_floor_above_version` | invalid | 2 | tasklet '{tasklet}' reads back to schema version {floor}, above its own schema version {version} |  |
| `topology.schema_floor_without_migration` | invalid | 2 | tasklet '{tasklet}' reads back to schema version {floor} but supplies no migration, so the first older record would fail instead of the start |  |
| `topology.setting_on_short_form` | invalid | 2 | short form '{factory_id}' declares {fields}, which a short form does not carry: its kind is its declaration's, its tasklet is named after its id, and every class-level setting keeps its default |  |
| `topology.short_form_field_on_full_form` | invalid | 2 | operator '{factory_id}' declares {fields} beside its channels body; a full form wires entry channels, codecs and sinks in channels and declares each tasklet's fields on the tasklet |  |
| `topology.short_form_name_mismatch` | invalid | 2 | '{class}' declares operator id '{factory_id}' and tasklet name '{name}'; a short form's tasklet is named after its id, so the two must be equal |  |
| `topology.short_form_not_a_tasklet` | invalid | 2 | '{class}' is declared a short-form operator with {annotation} but is not a tasklet |  |
| `topology.short_form_with_topology` | invalid | 2 | operator '{factory_id}' is a single-tasklet short form and also declares a channels body; declare one or the other |  |
| `topology.sink_on_entry` | invalid | 2 | entry channel '{channel}' is given a plugin sink, and a sink is fed by a tasklet; wire the entry channel into one first |  |
| `topology.sink_without_output` | invalid | 2 | operator '{factory_id}' names a sink, and its tasklet declares no output to feed it |  |
| `topology.sleep_without_persistence` | invalid | 2 | class '{factory_id}' declares sleep_when_idle with persistence off: a sleeping instance is evicted and restored from its durable state, and this class keeps none |  |
| `topology.state_not_constructible` | invalid | 2 | tasklet '{tasklet}' has state class '{state}' with no accessible no-argument constructor; add one, or write initState |  |
| `topology.state_type_id_collision` | invalid | 2 | tasklet '{tasklet}' has state types '{first}' and '{second}' that derive the same encoding id {id}; rename one, or list the types to own their ids |  |
| `topology.state_unrepresentable` | invalid | 2 | {path} cannot be encoded for durable state: {reason} |  |
| `topology.static_tasklet_without_cell_keys` | invalid | 2 | static tasklet '{tasklet}' declares no cell_keys |  |
| `topology.sync_durability_on_unbounded` | invalid | 2 | class '{factory_id}' declares sync_durability, which only a bounded class may: a stream class acknowledges at input rates, and a remote round trip per acknowledgement is not a guarantee it can keep |  |
| `topology.sync_durability_without_persistence` | invalid | 2 | sync_durability requires persistent: there is no commit barrier to ship for a class that journals nothing |  |
| `topology.tasklet_name_missing` | invalid | 2 | tasklet class '{tasklet}' declares no name, and the name is where its state is saved |  |
| `topology.transform_after_key_by` | invalid | 2 | channel '{channel}' declares a transform after key_by; the key is evaluated on the final post-transform value, so it must be the last link before into |  |
| `topology.transform_after_terminal` | invalid | 2 | channel '{channel}' declares a transform or key_by after a plugin sink; the sink serializes the channel's own value, so nothing downstream of it can be reshaped |  |
| `topology.two_clocks` | invalid | 2 | tasklet '{tasklet}' declares two clocks: it exports event_time and its logical_clock answers true; declare one |  |
| `topology.unkeyed_channel_under_effectively_once` | invalid | 2 | channel '{channel}' has a plugin wire but is not declared keyed; the effectively-once tier deduplicates by output identity, so every plugin-bound channel of an effectively_once operator must opt in with keyed |  |
| `topology.unkeyed_edge_into_dynamic_tasklet` | invalid | 2 | channel '{channel}' feeds tasklet '{tasklet}' without a key_by, but another edge keys it into dynamic mode; every edge into a dynamic-mode tasklet must name the Cell its inputs reach |  |
| `topology.unknown_codec` | invalid | 2 | channel '{channel}' names codec '{codec}', which is not one of {codecs} |  |
| `topology.unknown_field` | invalid | 2 | {declaration} declares {fields}, which it does not carry |  |
| `topology.unknown_operator_kind` | invalid | 2 | '{module}' declares neither an unbounded nor a bounded operator |  |
| `topology.unknown_sdk_lang` | invalid | 2 | sdk_lang {sdk_lang} is not an allocated SDK language; every class declares the byte its SDK holds in the allocation table, because the controller places an operator only on a runner of its factory's language |  |
| `topology.unreachable_tasklet` | invalid | 2 | tasklet '{tasklet}' is not reachable from any entry channel |  |
| `topology.unresolved_type_argument` | invalid | 2 | tasklet class '{tasklet}' leaves its {parameter} type argument unresolved; declare it concretely where the class implements the tasklet |  |
| `topology.unservable_clock` | invalid | 2 | tasklet '{tasklet}' declares clock '{clock_type}' with no change type (NoChange); the runtime cannot serve it, because logical time is the durable change seq, which never advances without persistent changes |  |
| `topology.unusable_sdk_version` | invalid | 2 | class '{factory_id}' declares no usable SDK version ('{sdk_version}'); every class declares the released version of the SDK that writes its records, because the durable header names the writer and a veneer releases apart from the core it links against |  |

### usage

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `usage.command_required` | invalid | 2 | {command} needs a command | `{command} help` |
| `usage.invalid_json` | invalid | 2 | {option} is not valid JSON: {reason} |  |
| `usage.invalid_number` | invalid | 2 | invalid {option} value '{value}' |  |
| `usage.invalid_plugins` | invalid | 2 | --plugins is not a plugin list: {reason} |  |
| `usage.json_not_object` | invalid | 2 | {option} must be a JSON object |  |
| `usage.missing_argument` | invalid | 2 | the command is missing an argument | `{usage}` |
| `usage.negative` | invalid | 2 | {option} must be zero or more, got {value} |  |
| `usage.not_positive` | invalid | 2 | {option} must be a whole number of one or more, got {value} |  |
| `usage.option_required` | invalid | 2 | {option} is required | `{usage}` |
| `usage.option_requires_value` | invalid | 2 | {option} requires a value |  |
| `usage.option_takes_no_value` | invalid | 2 | {option} takes no value |  |
| `usage.options_conflict` | invalid | 2 | {option} and {other} cannot be given together |  |
| `usage.text_only` | invalid | 2 | {what} is rendered as text only | `{alternative}` |
| `usage.unexpected_argument` | invalid | 2 | unexpected argument '{argument}' | `{usage}` |
| `usage.unknown_command` | invalid | 2 | '{word}' is not a {command} command; use one of {allowed} | `{command} help` |
| `usage.unknown_option` | invalid | 2 | unknown option '{option}' | `{usage}` |
| `usage.unknown_word` | invalid | 2 | unknown {noun} '{value}'; use one of {allowed} |  |
