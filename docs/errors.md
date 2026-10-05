# Errors

<!-- Generated from flz_transport/errors.json by `mix flezha.errors_doc`; do not edit. -->

Every failure Flezha reports carries one shape, on every surface: the `flz` CLI, the
published consumer API, the SDK administration door, spawn, a runner connect and every
SDK. A program acts on the `code` and the `class`.

| Field | Meaning |
|---|---|
| `code` | Stable, spelled `family.snake_case`. |
| `class` | What kind of failure it is, and so what a caller can do about it. |
| `message` | One plain sentence naming what it is about. |
| `details` | An object carrying the facts the message names. |
| `hint` | The next command to run, or `null`. |

**Contents**

1. [Classes and exit codes](#classes-and-exit-codes)
2. [Forms](#forms)
3. [Codes](#codes)
   1. [audit](#audit)
   1. [authz](#authz)
   1. [boot](#boot)
   1. [call](#call)
   1. [cli](#cli)
   1. [commit](#commit)
   1. [control](#control)
   1. [controller](#controller)
   1. [dlq](#dlq)
   1. [durability](#durability)
   1. [effect](#effect)
   1. [http](#http)
   1. [http_push](#http_push)
   1. [input](#input)
   1. [internal](#internal)
   1. [listener](#listener)
   1. [mcp](#mcp)
   1. [namespace](#namespace)
   1. [observability](#observability)
   1. [part](#part)
   1. [plugin](#plugin)
   1. [postgres_cdc](#postgres_cdc)
   1. [quota](#quota)
   1. [rabbitmq](#rabbitmq)
   1. [relay](#relay)
   1. [reset](#reset)
   1. [runner](#runner)
   1. [runtime](#runtime)
   1. [schedule](#schedule)
   1. [shutdown](#shutdown)
   1. [spawn](#spawn)
   1. [state](#state)
   1. [topology](#topology)
   1. [usage](#usage)

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
| `authz.invalid_caller_context` | invalid | 2 | the caller context is not valid: it needs a source of cli, ui, sdk, http or internal |  |
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

### call

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `call.absorbed` | conflict | 6 | relay '{relay}' took the call's input without running it: it was a duplicate, or an input hook filtered it out, so no answer will come |  |
| `call.busy` | unavailable | 7 | relay '{relay}' already owes answers to {pending_max} waiting callers; nothing was sent, try again later | `raise FLEZHA_CALL_PENDING_MAX if the relay is meant to hold more waiting callers` |
| `call.callee_ended` | unavailable | 7 | relay '{relay}' ended ({status}) before it answered the call |  |
| `call.failed` | unavailable | 7 | relay '{relay}' dead-lettered the call's input as '{dlq_ref}': {error} | `flz dlq list {relay}` |
| `call.not_accepted` | conflict | 6 | relay '{relay}' runs once over one input and has already taken it, so it cannot take the call's input |  |
| `call.target_unkeyed` | invalid | 2 | '{relay}' is a partition group; name the key whose member should take the input, or one of its members: {members} | `pass --key <key>` |
| `call.timeout` | unavailable | 7 | relay '{relay}' did not answer within {deadline_ms} ms; the input stays accepted and its turn still runs |  |
| `call.unavailable` | unavailable | 7 | relay '{relay}' cannot answer the call: {reason} |  |

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
| `commit.not_owner` | conflict | 6 | this controller does not own relay '{relay}', so its commit is refused |  |
| `commit.oversized_handoff` | invalid | 2 | a tasklet's handoff of {size} bytes is over the {max}-byte limit |  |
| `commit.record_not_durable` | unavailable | 7 | the record at seq {seq} of Cell '{cell}' did not become durable in time |  |
| `commit.turn_not_parked` | unavailable | 7 | the failed turn could not be parked in the dead-letter queue: {reason} |  |

### control

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `control.args_not_array` | invalid | 2 | the arguments must be a JSON array |  |
| `control.invalid_args_json` | invalid | 2 | the arguments are not valid JSON: {reason} |  |
| `control.no_control_plane` | unavailable | 7 | there is no control plane to administer over: the runner is closed or has no controller |  |
| `control.timeout` | unavailable | 7 | the controller did not answer the administration call within {seconds}s |  |
| `control.unknown_function` | not_found | 5 | no published function is named '{function}' |  |
| `control.unknown_parameter` | invalid | 2 | unknown parameter '{parameter}' |  |
| `control.unreadable_answer` | internal | 1 | the controller's answer to '{verb}' could not be read |  |
| `control.wrong_arity` | invalid | 2 | '{function}' takes no argument list of this length |  |

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
| `dlq.bad_entry_ref` | invalid | 2 | '{entry}' is not an entry reference; write it as `<relay>/<tasklet>/<entry-id>` | `flz dlq list` |
| `dlq.not_found` | not_found | 5 | no parked entry '{entry}' | `flz dlq list` |
| `dlq.payload_not_replayable` | conflict | 6 | entry '{entry}' carries no payload bytes (an input between tasklets), so it cannot be replayed; show it or drop it | `flz dlq drop {entry}` |

### durability

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `durability.sync_ship_failed` | unavailable | 7 | relay '{relay}' declares durable: remote, and shipping its state to the durability backend failed: {reason} |  |

### effect

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `effect.bad_name` | invalid | 2 | an effect's name must be a non-empty string of at most 255 bytes, and {name} is not | `name the call after what it reaches, for example "model" or "search"` |
| `effect.nested` | invalid | 2 | effect {name} was called inside the function of another effect | `call ctx.effect once for the whole outside call, or one after the other` |
| `effect.not_recorded` | unavailable | 7 | the controller did not keep the result of effect {name}: {reason} | `the invocation runs again; give the outside service ctx.stable_id() as an idempotency key` |
| `effect.outside_invocation` | invalid | 2 | effect {name} was called through a context whose invocation has already returned | `call ctx.effect from run or compensate, before it returns` |
| `effect.refused` | invalid | 2 | the controller keeps no result of {bytes} bytes for input {offset}: it is over the size cap, past the input log's last input, or one result too many for that input | `keep a reference to the result rather than the result, or raise FLEZHA_EFFECT_MAX_BYTES` |
| `effect.too_large` | invalid | 2 | the result of effect {name} is {bytes} bytes, over the {limit}-byte limit | `keep a reference to the result rather than the result, or raise FLEZHA_EFFECT_MAX_BYTES` |

### http

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `http.answer_invalid` | invalid | 2 | relay '{relay}' answered the request with something that is not an HTTP answer envelope: {reason} |  |
| `http.body_timeout` | invalid | 2 | the client stopped sending the request body for {timeout_ms} ms |  |
| `http.body_too_large` | invalid | 2 | the request body is larger than the limit of {limit} bytes |  |
| `http.busy` | unavailable | 7 | plugin '{plugin}' already has {max_waiting} requests waiting for an answer; try again |  |
| `http.length_required` | invalid | 2 | a request body must carry a content-length: a body sent without one is refused before it is read |  |
| `http.listen_failed` | unavailable | 7 | the HTTP server of plugin '{plugin}' cannot listen on {address} port {port}: {reason} |  |
| `http.origin_refused` | denied | 4 | the request's Origin is not one of the HTTP API's allowed_origins | `flz plugin help http_api` |
| `http.streams_full` | unavailable | 7 | relay '{relay}' already has {max_streams} streams open; try again |  |
| `http.tls_required` | invalid | 2 | address '{address}' is not loopback, so the HTTP API needs tls_certfile and tls_keyfile, or plain_http set to true | `flz plugin help http_api` |

### http_push

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `http_push.exhausted` | unavailable | 7 | the output was not delivered to its HTTP endpoint: {reason} |  |
| `http_push.rejected` | invalid | 2 | the HTTP endpoint refused the output with status {status} |  |

### input

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `input.dead_lettered` | unavailable | 7 | an input of relay '{relay}' on channel '{channel}' was dead-lettered as '{entry}' after {attempts} attempt(s): {error} | `flz dlq show {entry}` |

### internal

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `internal.uncatalogued` | internal | 1 | an unexpected failure: {reason} |  |

### listener

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `listener.detached` | conflict | 6 | the listener on relay '{relay}' was disabled by another caller |  |
| `listener.overrun` | unavailable | 7 | the listener on relay '{relay}' fell more than {bytes} bytes or {frames} frames behind and was ended | `flz relay listen {relay} again, or raise FLEZHA_LISTENER_PENDING_BYTES` |

### mcp

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `mcp.unknown_code` | not_found | 5 | no error code is named '{requested}' | `call flezha_docs with page errors for every code` |
| `mcp.unknown_language` | invalid | 2 | there is no SDK example for the language '{language}' | `use one of: {languages}` |
| `mcp.unknown_page` | not_found | 5 | no guide page is named '{page}' | `call flezha_guide for the pages it lists` |

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
| `namespace.unknown_quota_field` | invalid | 2 | '{field}' is not a quota field; the fields are max_relays, max_cells_per_relay, max_mailbox_bytes_total, max_message_rate_per_second, max_state_bytes and max_runners |  |

### observability

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `observability.disabled` | unavailable | 7 | observability is disabled |  |
| `observability.missing_source` | invalid | 2 | the query names no source | `flz observe sources` |
| `observability.pubsub_disabled` | unavailable | 7 | the PubSub sink is disabled, so no source can be tailed; enable :observability, :pubsub_sink_enabled |  |

### part

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `part.no_output` | invalid | 2 | tasklet '{tasklet}' of relay '{relay}' sent a part, but its output channel '{channel}' has no output | `declare an output on the tasklet's channel` |
| `part.outside_invocation` | invalid | 2 | a part was sent through a context whose invocation has already returned |  |

### plugin

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `plugin.ack_needs_correlation` | invalid | 2 | sink_ack_mode required needs a sink_template containing the {placeholder} placeholder, because a frame the upstream cannot correlate cannot be acknowledged |  |
| `plugin.activation_failed` | unavailable | 7 | the plugin was prepared but could not start; the controller's log names the cause |  |
| `plugin.already_attached` | conflict | 6 | plugin '{plugin}' is already attached to this relay | `flz plugin list` |
| `plugin.already_enabled` | conflict | 6 | plugin '{plugin}' is already enabled |  |
| `plugin.app_callback` | invalid | 2 | application {application} depends on flezha and declares an application callback; a plugin's processes are its providers' declared workers |  |
| `plugin.channel_spelled_twice` | invalid | 2 | a {provider} plugin was given channel together with {options}; channel already names both directions, so write it alone or write the pair instead |  |
| `plugin.channel_unused` | invalid | 2 | option '{option}' names a channel this {provider} plugin never uses: it neither feeds the relay on it nor reads it through an on_input or on_output hook |  |
| `plugin.channel_written_twice` | invalid | 2 | payload_metadata names channel '{metadata_channel}' and a channel option names '{channel}'; write one of the two |  |
| `plugin.digest_mismatch` | invalid | 2 | the archive from {source} has SHA-256 {actual}, not the pinned {expected} |  |
| `plugin.duplicate_id` | conflict | 6 | the archives {files} in the plugin root all declare package '{plugin}', so none of them is loaded | `flz plugin uninstall {plugin}` |
| `plugin.enable_failed` | unavailable | 7 | plugin '{plugin}' could not be enabled in namespace '{namespace}': its namespace's hub did not start it |  |
| `plugin.entry_channel_required` | invalid | 2 | a {provider} plugin needs an entry channel: write channel or channel_in |  |
| `plugin.entry_channel_set` | invalid | 2 | a {provider} plugin injects on exactly one channel, and option '{option}' names several |  |
| `plugin.fetch_failed` | unavailable | 7 | the package could not be fetched from {source}: {reason} |  |
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
| `plugin.forward_across_namespaces` | denied | 4 | target_relay_id '{target}' runs in namespace '{target_namespace}', across the namespace boundary |  |
| `plugin.forward_to_self` | invalid | 2 | target_relay_id '{target}' is the relay the plugin is attached to |  |
| `plugin.group_disable_partial` | unavailable | 7 | the plugin could not be disabled on {failed} |  |
| `plugin.group_enable_partial` | unavailable | 7 | the plugin was enabled on {enabled} of {total} partitions and failed on {failed} |  |
| `plugin.headers_invalid` | invalid | 2 | option '{option}' must be a JSON object mapping header names to text or number values, without a header the plugin sets itself or one a request cannot carry |  |
| `plugin.help_failed` | internal | 1 | the help of plugin provider '{provider}' failed to render |  |
| `plugin.hook_invalid` | invalid | 2 | hook {hook} is not a name, a [name, opts] pair, or an object with a name and an opts object |  |
| `plugin.hook_key_missing` | invalid | 2 | an on_message hook needs its key option |  |
| `plugin.hook_key_taken` | conflict | 6 | another plugin already answers key '{key}' on hook '{hook}' |  |
| `plugin.hook_not_implemented` | invalid | 2 | the plugin's provider does not implement hook '{hook}' | `flz plugin help <provider_id>` |
| `plugin.hook_option_invalid` | invalid | 2 | hook option '{option}' does not accept the value {value} |  |
| `plugin.hook_options_not_object` | invalid | 2 | a hook's options must be an object of names and values |  |
| `plugin.hook_position_refused` | invalid | 2 | this hook does not take a position; only a transform or fold hook does |  |
| `plugin.hooks_missing` | invalid | 2 | a plugin needs a list of at least one hook | `flz plugin help <provider_id>` |
| `plugin.id_taken` | conflict | 6 | plugin '{plugin}' is already enabled in namespace '{namespace}' | `flz plugin list --namespace {namespace}` |
| `plugin.init_after_start` | invalid | 2 | an on_relay_init plugin can only be attached when its relay starts | `flz relay start <factory> --plugins '<json-array>'` |
| `plugin.invalid_regex` | invalid | 2 | option 'regex_pattern' is not a valid regular expression |  |
| `plugin.library_conflict` | conflict | 6 | the package bundles {application} {version}, and {holder} already holds {application} {loaded_version}; a controller holds one version of each library | `pin {application} to {loaded_version} in the plugin project and package it again` |
| `plugin.list_declared` | conflict | 6 | this node declares its plugins in {variable}, so install and uninstall are refused; change the list and restart the controller |  |
| `plugin.list_not_declared` | invalid | 2 | {variable} is not set, so there is no plugin list to sync |  |
| `plugin.load_failed` | internal | 1 | the package could not be loaded: {what} failed ({reason}) |  |
| `plugin.module_conflict` | conflict | 6 | the package's module {module} is already held by {holder} |  |
| `plugin.namespace_refused` | invalid | 2 | provider '{provider}' can be enabled only in namespace '{namespace}' | `flz plugin enable <name> {provider} --namespace {namespace}` |
| `plugin.native_code` | invalid | 2 | the package carries native code ({entry}); a plugin package holds only BEAM code |  |
| `plugin.not_bindable` | invalid | 2 | plugin '{plugin}' is a controller plugin, which is bound to no relay | `flz plugin update <name> --hook <hook>` |
| `plugin.not_bound` | not_found | 5 | relay '{relay}' is not bound to plugin '{plugin}' | `flz plugin list` |
| `plugin.not_found` | not_found | 5 | plugin '{plugin}' is not enabled | `flz plugin list` |
| `plugin.not_installed` | not_found | 5 | plugin '{plugin}' is not installed | `flz plugin list --installed` |
| `plugin.not_running` | unavailable | 7 | plugin '{plugin}' runs as one copy in the cluster, and no copy of it is running yet |  |
| `plugin.not_shareable` | conflict | 6 | plugin '{plugin}' cannot be shared, and relay '{relay}' is already bound to it | `enable another plugin of the same provider for this relay` |
| `plugin.option_above_maximum` | invalid | 2 | option '{option}' is {value}, above its maximum of {max} | `flz plugin help <provider_id>` |
| `plugin.option_below_minimum` | invalid | 2 | option '{option}' is {value}, below its minimum of {min} | `flz plugin help <provider_id>` |
| `plugin.option_invalid` | invalid | 2 | option '{option}' does not accept the value {value} | `flz plugin help <provider_id>` |
| `plugin.option_requires` | invalid | 2 | option '{option}' needs option '{required}' as well |  |
| `plugin.options_exclusive` | invalid | 2 | options '{option}' and '{other}' cannot be set together |  |
| `plugin.options_required` | invalid | 2 | a {provider} plugin needs the option {options} | `flz plugin help <provider_id>` |
| `plugin.package_invalid` | invalid | 2 | the plugin package is refused: {problem} | `package it again with mix flezha.plugin.package` |
| `plugin.partition_placeholder_unfilled` | invalid | 2 | option '{option}' carries {placeholder}, but this plugin has no partition identity; the placeholder is filled only on a partitioned start |  |
| `plugin.partitioned_host` | invalid | 2 | a {provider} plugin cannot be enabled on '{relay}', a member of a partition group: it cannot tell which member a record's key belongs to | `enable it on a relay that is not partitioned` |
| `plugin.prepare_failed` | invalid | 2 | plugin '{plugin}' was refused while its relay started, so the relay did not start |  |
| `plugin.protocol_consolidation_failed` | internal | 1 | protocol {protocol} could not be consolidated again with the package's implementations |  |
| `plugin.provider_bad_answer` | internal | 1 | the plugin's provider answered with a value it may not return |  |
| `plugin.provider_conflict` | conflict | 6 | provider id '{provider}' is already taken by {holder} |  |
| `plugin.provider_crashed` | internal | 1 | provider {provider} crashed while preparing the plugin; the controller's log names the cause |  |
| `plugin.provider_failed` | internal | 1 | provider '{provider}' of package '{plugin}' failed to start its worker: {reason} |  |
| `plugin.provider_not_loaded` | not_found | 5 | provider module {provider} is not loaded on {controller}, and no plugin package installed there provides it | `flz plugin list --installed` |
| `plugin.provider_refused` | invalid | 2 | provider '{provider}' of package '{plugin}' refused its options: {reason} |  |
| `plugin.provider_unavailable` | unavailable | 7 | provider {provider} belongs to package '{plugin}', which is {status} on this controller | `flz plugin list --installed` |
| `plugin.registry_unavailable` | unavailable | 7 | the relay registry of the plugin's namespace did not answer; try again |  |
| `plugin.relay_not_declared` | not_found | 5 | the plugin's relay is not declared in its namespace, so nothing records the plugin |  |
| `plugin.runtime_mismatch` | conflict | 6 | {why}; the package was built on OTP {package_otp}, Elixir {package_elixir} and Flezha {package_flezha}; this controller runs OTP {otp}, Elixir {elixir} and Flezha {flezha} | `package it again in flezha/plugin-builder:{flezha}` |
| `plugin.service_failed` | internal | 1 | the plugin service failed; the controller's log names the cause |  |
| `plugin.service_unavailable` | unavailable | 7 | the plugin service did not answer; try again |  |
| `plugin.signature_invalid` | denied | 4 | the package signature does not verify against any trusted key |  |
| `plugin.signature_required` | denied | 4 | the controller requires signed plugin packages, and this one carries no signature.sig | `package it with mix flezha.plugin.package --sign <key.pem>` |
| `plugin.single_output` | invalid | 2 | plugin '{plugin}' runs as one copy in the cluster, so it takes no on_output hook |  |
| `plugin.source_unsupported` | invalid | 2 | '{source}' is neither a local path nor an https:// URL |  |
| `plugin.spec_field_missing` | invalid | 2 | a plugin spec needs a non-empty '{field}' string |  |
| `plugin.spec_field_not_object` | invalid | 2 | a plugin spec's '{field}' must be a JSON object |  |
| `plugin.spec_invalid_json` | invalid | 2 | the plugins spec is not a JSON array of plugin objects |  |
| `plugin.spec_not_object` | invalid | 2 | each plugin spec must be a JSON object |  |
| `plugin.target_missing` | invalid | 2 | a plugin needs the namespace and the relay it attaches to |  |
| `plugin.template_missing_placeholder` | invalid | 2 | option '{option}' must contain the {placeholder} placeholder |  |
| `plugin.timeout_not_below_interval` | invalid | 2 | timeout_ms {timeout_ms} must be below interval_ms {interval_ms}, so that a poll cannot outlast its own interval |  |
| `plugin.trust_store_empty` | invalid | 2 | signed packages are required and no trusted key is configured, so no package can be verified | `set FLEZHA_PLUGIN_TRUSTED_KEYS to the public keys packages are signed with` |
| `plugin.unknown_hook` | not_found | 5 | no hook is named '{hook}'; this plugin takes {allowed} | `flz plugin help <provider_id>` |
| `plugin.unknown_hook_option` | invalid | 2 | a {kind} hook has no option '{option}' |  |
| `plugin.unknown_options` | invalid | 2 | a {provider} plugin has no option {options} | `flz plugin help <provider_id>` |
| `plugin.unknown_provider` | not_found | 5 | no plugin provider is named '{provider}' | `flz plugin list --installed` |
| `plugin.unread_options` | invalid | 2 | flz plugin enable does not read {options}; provider settings are written `--X<key>=<value>`, and hooks `--hook=<name>` | `flz plugin help <provider>` |
| `plugin.url_has_credentials` | invalid | 2 | option '{option}' must not embed credentials; put a secret in '{headers_option}' |  |
| `plugin.url_scheme_unsupported` | invalid | 2 | option '{option}' must be an http or https URL, got {url} |  |
| `plugin.url_without_host` | invalid | 2 | option '{option}' names {url}, which has no host |  |
| `plugin.window_needs_idempotence` | invalid | 2 | max_in_flight {max_in_flight} needs idempotent set to true: only the broker's per-partition sequence check keeps a retry inside the window ordered |  |
| `plugin.write_failed` | internal | 1 | the plugin could not be recorded on its relay; the controller's log names the cause |  |

### postgres_cdc

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `postgres_cdc.publication_missing` | not_found | 5 | the publication '{publication}' does not exist, and the plugin names no tables to create it from |  |
| `postgres_cdc.replica_identity_missing` | invalid | 2 | the published table {table} has no usable replica identity: give it a primary key or set REPLICA IDENTITY FULL |  |
| `postgres_cdc.replication_failed` | unavailable | 7 | the plugin cannot read changes from {where}: {reason} |  |
| `postgres_cdc.server_too_old` | invalid | 2 | the database server is version {version}; change capture needs 14 or newer |  |
| `postgres_cdc.slot_lag` | unavailable | 7 | the replication slot {slot} holds back {lag_bytes} bytes of write-ahead log, past max_lag_bytes {max_lag_bytes} |  |
| `postgres_cdc.wal_level_not_logical` | invalid | 2 | the database server's wal_level is {wal_level}; change capture needs logical |  |

### quota

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `quota.exceeded` | conflict | 6 | the namespace's {kind} quota is reached ({current} of {limit}) | `flz namespace show` |

### rabbitmq

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `rabbitmq.channel_closed` | unavailable | 7 | the RabbitMQ broker closed the plugin's channel with {code}: {text} |  |
| `rabbitmq.connection_failed` | unavailable | 7 | the plugin has no connection to the RabbitMQ broker at {host}: {reason} |  |
| `rabbitmq.exhausted` | unavailable | 7 | the output was not confirmed by the RabbitMQ broker: {reason} |  |
| `rabbitmq.unroutable` | invalid | 2 | the RabbitMQ broker returned the output: exchange '{exchange}' routes key '{routing_key}' to no queue |  |

### relay

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `relay.already_running` | conflict | 6 | relay '{relay}' is already running | `flz relay list` |
| `relay.busy` | conflict | 6 | the relay is busy moving to another runner; try again |  |
| `relay.channel_without_text` | invalid | 2 | --channel names the entry channel of a first message, and no --text was given |  |
| `relay.factory_language_ambiguous` | conflict | 6 | factory '{factory}' in namespace '{namespace}' is served in more than one SDK language and none is established; run one language's runners, or clear the factory's durable state |  |
| `relay.factory_language_undeclared` | unavailable | 7 | factory '{factory}' in namespace '{namespace}' is served only by runners that declare no SDK language, and such a runner is never placed on |  |
| `relay.factory_language_unserved` | unavailable | 7 | factory '{factory}' in namespace '{namespace}' is established in SDK language {language}, and no connected runner of that language serves it; start one, or clear the factory's durable state |  |
| `relay.factory_mismatch` | conflict | 6 | '{relay}' in namespace '{namespace}' is a relay of factory '{actual}', not of '{factory}', so a call to factory '{factory}' cannot reach it |  |
| `relay.factory_not_flow` | invalid | 2 | factory '{factory}' is a steps relay, and only a flow one is started here |  |
| `relay.factory_not_steps` | invalid | 2 | factory '{factory}' is a flow relay, and only a steps one can be spawned or scheduled |  |
| `relay.factory_run_ambiguous` | conflict | 6 | factory '{factory}' in namespace '{namespace}' is announced as both flow and steps, and neither is established; serve it from runners that agree on how it runs |  |
| `relay.factory_run_undeclared` | unavailable | 7 | factory '{factory}' in namespace '{namespace}' is served only by runners that announce no run for it, or a run that is neither flow nor steps, and such a runner is never placed on |  |
| `relay.factory_run_unserved` | unavailable | 7 | factory '{factory}' in namespace '{namespace}' is established as {run}, and every runner serving it announces the other run |  |
| `relay.factory_unknown` | not_found | 5 | no runner in namespace '{namespace}' serves factory '{factory}' | `flz runner list --namespace {namespace}` |
| `relay.first_message_not_text` | invalid | 2 | a first message and its channel must be text |  |
| `relay.group_base_taken` | conflict | 6 | '{relay}' names a partition group of {members} live members; start it with --partitions {partitions} to converge it, or stop the group first |  |
| `relay.group_id_too_long` | invalid | 2 | group id '{relay}' is too long for {partitions} partitions: the member suffix leaves at most {max} bytes for it |  |
| `relay.group_not_found` | not_found | 5 | no partition group '{group}' in namespace '{namespace}' | `flz relay list --namespace {namespace}` |
| `relay.group_partition_mismatch` | conflict | 6 | group '{group}' cannot converge to {partitions} partitions: its live member '{member}' declares {declared}, and a start never moves Cell state | `flz relay scale {group} {partitions}` |
| `relay.group_stop_partial` | unavailable | 7 | partition group '{group}' did not stop completely: {failed} did not stop |  |
| `relay.id_too_long` | invalid | 2 | relay id '{relay}' is longer than {limit} bytes |  |
| `relay.input_refused` | unavailable | 7 | relay '{relay}' refused the input because its durable append failed |  |
| `relay.iteration_not_committed` | invalid | 2 | iteration {iteration} has not committed, so there is no boundary to rewind to there |  |
| `relay.member_unreadable` | unavailable | 7 | partition member '{member}' could not be read to rescale from |  |
| `relay.missing_fields` | invalid | 2 | a relay start needs {fields} |  |
| `relay.no_free_id` | conflict | 6 | no free relay id was found for factory '{factory}' in {draws} draws | `flz relay start {factory} --id <id>` |
| `relay.no_runner` | unavailable | 7 | the relay has no runner bound to it yet |  |
| `relay.no_runners` | unavailable | 7 | no runner is connected in namespace '{namespace}' | `flz runner list --namespace {namespace}` |
| `relay.not_accepting_input` | unavailable | 7 | relay '{relay}' is not ready to accept input yet |  |
| `relay.not_found` | not_found | 5 | relay '{relay}' is not running in namespace '{namespace}' | `flz relay list --namespace {namespace}` |
| `relay.not_running` | unavailable | 7 | relay '{relay}' is not running; start it first | `flz relay list` |
| `relay.outside_retention` | invalid | 2 | seq {target} of Cell '{cell}' is older than its retention window; the earliest point kept is seq {earliest} |  |
| `relay.partitioned_first_message` | invalid | 2 | a partitioned start carries no first message, because a group's message reaches its member by key; send it through a keyed forward, or to a member id such as '{member}' once the group runs |  |
| `relay.partitions_out_of_range` | invalid | 2 | partitions must be between 2 and {max}, one per key group; got {partitions} |  |
| `relay.remote_without_backend` | unavailable | 7 | factory '{factory}' declares durable: remote, and this controller has no durability backend; set FLEZHA_DURABILITY_BACKEND, or declare durable: local |  |
| `relay.replay_steps` | invalid | 2 | '{cell}' is a Cell of a steps relay, whose baseline is a committed iteration across every Cell rather than one Cell's WAL seq, so it cannot be replayed; take the loop back with a rewind | `flz relay rewind <relay> --to-iteration <n>` |
| `relay.rescale_id_held` | unavailable | 7 | the rescale stopped before anything moved: '{member}' still holds its id {timeout_ms} ms after it was stopped; retry once it has gone |  |
| `relay.rescale_move_failed` | internal | 1 | the rescale stopped: a Cell move failed, so the group is stopped with no member started; run the scale again to finish the remaining moves |  |
| `relay.rescale_parked_entries` | conflict | 6 | the group holds parked DLQ entries ({entries}); replay or drop them before a rescale | `flz dlq list` |
| `relay.rescale_quiesce_timeout` | unavailable | 7 | the rescale stopped before anything moved: '{member}' has not consumed its input log within the timeout; retry with a longer timeout, or let a checkpoint or a pass commit advance it |  |
| `relay.rescale_running` | conflict | 6 | a rescale of '{group}' in namespace '{namespace}' is already running; wait for it to finish |  |
| `relay.rewind_uncovered` | conflict | 6 | a rewind to iteration {iteration} would unwind output on {channels}, written by a tasklet that declares no compensate hook, so it is refused |  |
| `relay.start_failed` | internal | 1 | relay '{relay}' did not start; the controller's log names the cause |  |
| `relay.state_claimed` | conflict | 6 | the durable state of '{relay}' belongs to namespace '{owner}' |  |
| `relay.stop_failed` | internal | 1 | relay '{relay}' did not stop: its {step} step failed; the controller's log names the cause |  |
| `relay.stop_timeout` | unavailable | 7 | relay '{relay}' did not finish stopping in time; the steps {skipped} were skipped |  |
| `relay.unknown_fields` | invalid | 2 | a relay start does not read {fields} |  |
| `relay.unknown_opts` | invalid | 2 | the relay options {opts} are not ones Flezha reads |  |
| `relay.unpartitioned_running` | conflict | 6 | '{relay}' is already running as an unpartitioned relay, which a partitioned start cannot converge; stop it first or choose another group id | `flz relay stop {relay}` |
| `relay.unresponsive` | unavailable | 7 | the relay did not answer; try again |  |

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
| `runner.hub_unavailable` | unavailable | 7 | the relay hub of namespace '{namespace}' could not be started for the runner; try again |  |
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
| `runtime.channel_undeclared` | not_found | 5 | relay '{relay}' has no {kind} channel '{channel}'; it declares {channels} |  |
| `runtime.channel_wrong_kind` | invalid | 2 | relay '{relay}' channel '{channel}' is a {declared} channel, not a {kind} one |  |
| `runtime.class_not_registered` | not_found | 5 | no relay class is registered as '{factory}' on this runner |  |
| `runtime.command_crashed` | internal | 1 | the runner failed while answering the command: {reason} |  |
| `runtime.completion_too_many_effects` | invalid | 2 | the completion carries more effects than the activation had items |  |
| `runtime.connect_unconfirmed` | unreachable | 3 | the controller connect was not confirmed within {seconds}s |  |
| `runtime.edge_closure_panicked` | internal | 1 | the edge {closure} panicked: {reason} |  |
| `runtime.event_time_timer_without_event_time_clock` | invalid | 2 | tasklet '{tasklet}' declared clock '{clock}'; an event-time timer needs an event-time clock |  |
| `runtime.executor_unavailable` | unavailable | 7 | the {executor} executor cannot run here: {reason} |  |
| `runtime.factory_in_service` | conflict | 6 | '{module}' cannot be unregistered, because a running runner cannot stop serving a started declaration |  |
| `runtime.factory_run_mismatch` | conflict | 6 | factory '{factory}' is registered as a {declared} class, and the controller started it as {run} |  |
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
| `runtime.local_run_busy` | unavailable | 7 | relay '{relay}' was still working after {budget_ms} ms |  |
| `runtime.local_run_unanswered` | unavailable | 7 | the local run did not answer within {budget_ms} ms |  |
| `runtime.max_cells_reached` | unavailable | 7 | the tasklet's max_cells cap is reached, so an input for a new key is dropped |  |
| `runtime.metadata_on_inter_tasklet_channel` | invalid | 2 | channel '{channel}' is between two tasklets, and only a plugin-bound channel carries metadata |  |
| `runtime.no_local_run` | conflict | 6 | there is no local run on this context: start one before feeding it |  |
| `runtime.no_output_channel` | invalid | 2 | tasklet '{tasklet}' emits but declares no output channel |  |
| `runtime.no_relays` | invalid | 2 | the runner declares no relay; register one before starting it |  |
| `runtime.not_a_consumer` | internal | 1 | an edge output names tasklet {tasklet}, which does not consume channel '{channel}' |  |
| `runtime.not_host_tasklet` | internal | 1 | the Cell is not run by a host, so it has no host crossing |  |
| `runtime.plugin_serializer_panicked` | internal | 1 | the plugin serializer of channel '{channel}' panicked: {reason} |  |
| `runtime.prepare_unknown_channel` | invalid | 2 | the prepare answer names a channel the relay does not declare |  |
| `runtime.relay_busy` | unavailable | 7 | relay '{relay}' did not answer in time; try again |  |
| `runtime.relay_message_unsupported` | unavailable | 7 | relay '{relay}' does not handle direct messages |  |
| `runtime.relay_not_found` | not_found | 5 | no instance of relay '{relay}' runs on this runner |  |
| `runtime.relay_not_local` | not_found | 5 | relay '{relay}' is not bound on this runner | `send without the local option, through the controller` |
| `runtime.relay_type_in_use` | conflict | 6 | factory '{factory_id}' has {live_instances} live relay instance(s); stop them before removing the type |  |
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
| `runtime.unknown_channel` | not_found | 5 | channel '{channel}' is not declared on this relay |  |
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
| `schedule.invalid_name` | invalid | 2 | schedule name '{schedule}' must match [A-Za-z0-9][A-Za-z0-9_-]* and be at most {max} bytes, so that each firing's '`<name>@<slot>`' fits a relay id |  |
| `schedule.invalid_timezone` | invalid | 2 | '{timezone}' is not an IANA time zone |  |
| `schedule.name_required` | invalid | 2 | a schedule needs a name |  |
| `schedule.not_found` | not_found | 5 | schedule '{schedule}' does not exist | `flz schedule list` |
| `schedule.plugins_not_array` | invalid | 2 | a schedule's plugins must be a JSON array of plugin specs |  |
| `schedule.timezone_required` | invalid | 2 | a schedule needs a time zone |  |

### shutdown

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `shutdown.coordinator_exited` | internal | 1 | the shutdown coordinator stopped before reporting an outcome; the controller's log names the cause |  |
| `shutdown.partial_drain` | unavailable | 7 | the shutdown drained partially: relays were still on this node when its timeout ran out |  |

### spawn

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `spawn.timeout` | unavailable | 7 | no spawn result arrived within {seconds}s |  |
| `spawn.unavailable` | unavailable | 7 | there is no control plane to request a spawn on: the runner is closed or has no controller |  |

### state

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `state.apply_failed` | invalid | 2 | the state could not be applied: {reason} |  |
| `state.apply_not_supported` | unavailable | 7 | this runner does not apply state to a live relay |  |
| `state.bad_cell_path` | invalid | 2 | '{path}' is not a Cell path; write it as `<relay>/<tasklet>/<cell_key>` |  |
| `state.bad_live_path` | invalid | 2 | '{path}' is not a live state path; write it as `<relay>/<tasklet>[/<cell_key>]` |  |
| `state.cell_busy` | unavailable | 7 | Cell '{cell}' is inside an activation older than {seconds}s |  |
| `state.cell_evicted` | not_found | 5 | Cell '{cell_key}' of tasklet '{tasklet}' was idle-evicted; eviction is destructive, so there is no durable value to serve | `flz state at <relay>/{tasklet}/{cell_key} <seq\|ts>` |
| `state.cell_key_without_tasklet` | invalid | 2 | a Cell key is unique only within its tasklet, so name the tasklet as well |  |
| `state.cell_not_found` | not_found | 5 | tasklet '{tasklet}' holds no Cell keyed '{cell_key}' | `flz state at <relay>/{tasklet}/{cell_key} <seq\|ts>` |
| `state.cell_not_live` | not_found | 5 | no live Cell matches cell id '{cell_id}' |  |
| `state.inbox_unreadable` | unavailable | 7 | the input log of '{instance}' did not answer |  |
| `state.init_values_invalid_json` | invalid | 2 | init_values is not valid JSON: {reason} |  |
| `state.init_values_not_object` | invalid | 2 | init_values must be a JSON object, got {kind} |  |
| `state.migration_unreadable` | invalid | 2 | the migrated state is not UTF-8 text: {reason} |  |
| `state.no_owning_runner` | unavailable | 7 | no connected runner owns relay '{relay}', so nothing can compact its Cell |  |
| `state.no_persisted_state` | not_found | 5 | Cell '{cell}' has no persisted state | `flz state list <relay>` |
| `state.no_relay` | not_found | 5 | no running relay '{relay}' in any namespace | `flz relay list` |
| `state.not_resident` | unavailable | 7 | relay '{relay}' is {state}, so no Cell of it is held in memory; read its durable state instead | `flz state at <relay>/<tasklet>/<cell_key> <seq>` |
| `state.oversized_read` | invalid | 2 | the selected state is {bytes} bytes, over the {limit}-byte read ceiling; narrow the read to one cell key | `flz state get <relay>/<tasklet>/<cell_key>` |
| `state.persistence_off` | conflict | 6 | relay '{relay}' declares persistence off, so its Cells hold no durable state and live reads are refused |  |
| `state.restore_failed` | internal | 1 | the Cell's state could not be restored: {reason} |  |
| `state.serialize_failed` | internal | 1 | the state of tasklet '{tasklet}' failed to serialize: {reason} |  |
| `state.snapshot_failed` | internal | 1 | the Cell's snapshot could not be taken: {reason} |  |
| `state.tasklet_not_found` | not_found | 5 | relay '{relay}' has no tasklet named '{tasklet}' |  |

### topology

| Code | Class | Exit | Message | Hint |
|---|---|---|---|---|
| `topology.at_checkpoint_without_max_delay` | invalid | 2 | checkpoint_outputs at_checkpoint requires checkpoint_max_delay_ms: an interval that never closes would hold its outputs forever on a quiet Cell |  |
| `topology.branch_too_few_arms` | invalid | 2 | branch requires at least two arms; {arms} given |  |
| `topology.branching_chained_loop` | invalid | 2 | a handoff-chained loop needs exactly one terminal tasklet per chain; the chain of entry channel '{channel}' has {leaves} |  |
| `topology.call_deadline_zero` | invalid | 2 | channel '{channel}' calls with a deadline of 0 ms; a call needs a positive deadline_ms |  |
| `topology.call_outside_per_input_loop` | invalid | 2 | channel '{channel}' ends in a call, and only a :per_input steps relay reads the later message its Response is; declare the class :per_input, or make the call from a flow relay |  |
| `topology.call_response_unread` | invalid | 2 | channel '{channel}' calls another relay and its Response channel '{response}' feeds no tasklet, so every outcome would be journaled and read by nobody; wire the Response channel into a tasklet, or forward with relay_forward when no outcome is wanted |  |
| `topology.call_without_key` | invalid | 2 | channel '{channel}' calls a factory with create: false and no key_by before the call, so nothing names the instance it may reach; key the chain, or let the call create its callee |  |
| `topology.cell_keys_without_static_mode` | invalid | 2 | tasklet '{tasklet}' declares cell_keys without static mode |  |
| `topology.cells_for_missing` | invalid | 2 | tasklet '{name}' declares non-empty cell_keys but does not override cells_for; write its cells_for (a Rust tasklet also sets CELLS_FOR_OVERRIDDEN) |  |
| `topology.chained_loop_terminal_without_handoff` | invalid | 2 | a durable handoff-chained loop's terminal tasklet '{tasklet}' releases no inter-tasklet handoff; iteration N+1 is seeded from iteration N's terminal batch, and a tasklet that owns no output channel, or whose only output is plugin-bound, has nothing the journal carries. Take the output off the terminal's own channel, or declare the loop per_input; wiring another tasklet behind the output only moves the rule onto that one |  |
| `topology.change_without_state` | invalid | 2 | tasklet '{tasklet}' declares applyChange but no initial state, so there is nothing for a change to fold into |  |
| `topology.channel_ends_twice` | invalid | 2 | channel '{channel}' declares two plugin ends; a channel ends in one output or one call, so route the value through a second tasklet to give it both |  |
| `topology.channel_name_too_long` | invalid | 2 | channel '{channel}' has a {len}-byte name; the name rides every frame this channel produces, under a one-byte length prefix, so it may be at most {max} bytes |  |
| `topology.channel_named_twice` | invalid | 2 | channel '{channel}' is named twice: named('{named}') and output('{output}') both name it. An output names the channel it terminates, so write the name once: output('{named}') to keep the channel's name, or named('{named}') with an output that takes no name |  |
| `topology.checkpoint_every_zero` | invalid | 2 | the relay declares checkpoint_every(0); an interval that closes every zero inputs is not an interval, so omit it to close only on an explicit checkpoint call |  |
| `topology.checkpoint_max_delay_zero` | invalid | 2 | checkpoint_max_delay_ms must be greater than zero |  |
| `topology.class_not_constructible` | invalid | 2 | class '{class}' could not be constructed through a public no-argument constructor: {reason} |  |
| `topology.codec_with_callable` | invalid | 2 | channel '{channel}' names both a codec and its own callable; give one |  |
| `topology.consumer_out_of_range` | invalid | 2 | channel '{channel}' names consumer {consumer} but the class declares {tasklets} tasklet(s) |  |
| `topology.correction_window_zero` | invalid | 2 | the relay declares correction_window(0); a zero window can correct nothing |  |
| `topology.correction_without_event_time` | invalid | 2 | the relay declares correction_window but tasklet '{tasklet}' declares clock '{clock_type}'; correction orders a replayed slice by event time, which only an event-time clock yields |  |
| `topology.cycle` | invalid | 2 | tasklet '{tasklet}' is on a cycle; a relay topology is a DAG, so a node's output may never reach its own input |  |
| `topology.dangling_transform_chain` | invalid | 2 | channel '{channel}' declares a transform or key_by that is never closed by into; a transform chain is stored on the consumer edge it terminates in, so a dangling one would silently do nothing |  |
| `topology.declaration_unreadable` | invalid | 2 | the class declaration is not valid JSON for the schema: {reason} |  |
| `topology.deferred_builder_surface` | invalid | 2 | relay '{module}' uses a builder call this SDK does not have yet: {reason} |  |
| `topology.dropping_policy_under_effectively_once` | invalid | 2 | channel '{channel}' declares on_full({policy}) on a relay that declares effectively_once; a shed input's offset is still advanced past by the tier's watermark, so the loss is permanent and reported nowhere. Declare one or the other, not both (a correction window implies the tier, so it is refused here too) |  |
| `topology.duplicate_channel` | invalid | 2 | channel '{channel}' is declared more than once on this relay |  |
| `topology.duplicate_factory_id` | invalid | 2 | factory '{factory_id}' is already declared on this runner |  |
| `topology.duplicate_key_by` | invalid | 2 | channel '{channel}' declares key_by twice before one into |  |
| `topology.duplicate_merge_source` | invalid | 2 | merge subscribes channel '{channel}' twice; each merge source must be a distinct channel (one mapping per channel into the merged input) |  |
| `topology.duplicate_output_name` | invalid | 2 | channel '{channel}' is named by two outputs; an output names the channel it terminates, so two channels cannot name one output. Give each its own name; a plugin still reaches both by attaching to the relay with no channel filter |  |
| `topology.duplicate_persistent_tasklet` | invalid | 2 | tasklet '{tasklet}' journals changes and is wired as more than one node; a changeful tasklet may appear once per class |  |
| `topology.duplicate_plugin_output` | invalid | 2 | channel '{channel}' was declared with output more than once; only one output per channel is allowed |  |
| `topology.edge_already_wired` | invalid | 2 | channel '{channel}' already feeds tasklet '{tasklet}'; an edge carries exactly one transform and one key or neither, so a re-declaration where either side carries one is refused rather than silently keeping the first |  |
| `topology.effectively_once_without_persistence` | invalid | 2 | exactly: effectively_once needs a durable other than false: the tier is durable state |  |
| `topology.entry_channel_with_producer` | invalid | 2 | entry channel '{channel}' cannot name a producer |  |
| `topology.entry_channel_without_consumer` | invalid | 2 | entry channel '{channel}' names no consumer |  |
| `topology.inter_task_channel_without_producer` | invalid | 2 | inter_task channel '{channel}' names no producer |  |
| `topology.invalid_mode` | invalid | 2 | relay '{factory_id}' sets {knob} to {value}, which is not one of its modes |  |
| `topology.invalid_setting` | invalid | 2 | {declaration} sets {setting} to {value}, and it must be {expected} |  |
| `topology.invalid_tasklet` | invalid | 2 | tasklet '{tasklet}' cannot be declared: {reason} |  |
| `topology.key_by_on_unkeyable_target` | invalid | 2 | channel '{channel}' declares key_by into tasklet '{tasklet}', which declares non-empty cell_keys (static mode); static keying routes by cells_for and may name several Cells, edge keying names exactly one, so declare one or the other, not both |  |
| `topology.keyed_channel_without_plugin_wire` | invalid | 2 | channel '{channel}' is declared keyed but has no plugin wire; output identity stamps only exist on plugin-bound frames, so add an output |  |
| `topology.keyed_on_entry` | invalid | 2 | entry channel '{channel}' is declared keyed; only a plugin-bound output channel carries a natural key |  |
| `topology.mailbox_capacity_too_large` | invalid | 2 | tasklet '{tasklet}' declared mailbox_capacity {value}; capacity must be between 1 and 1048576 |  |
| `topology.mailbox_capacity_zero` | invalid | 2 | tasklet '{tasklet}' declared mailbox_capacity 0; capacity must be between 1 and 1048576 |  |
| `topology.max_cells_zero` | invalid | 2 | dynamic tasklet '{tasklet}' declares max_cells 0 |  |
| `topology.merge_too_few_sources` | invalid | 2 | merge into tasklet '{tasklet}' declares {sources} source channel(s); a merge consumes two or more, so wire a single source with into instead |  |
| `topology.missing_body` | invalid | 2 | {declaration} declares no body; it needs {body} |  |
| `topology.missing_factory_id` | invalid | 2 | relay '{relay}' declares no factory id, and every relay needs one |  |
| `topology.multiple_output_channels` | invalid | 2 | tasklet '{tasklet}' produces more than one channel; a tasklet emits on exactly one output channel |  |
| `topology.named_on_entry` | invalid | 2 | entry channel '{channel}' cannot be renamed with named; an entry channel is named where it is declared |  |
| `topology.negative_setting` | invalid | 2 | tasklet '{tasklet}' sets {setting} to {value}, and it must not be negative |  |
| `topology.no_entry_channel` | invalid | 2 | the steps class declares no entry channel for its messages to arrive on |  |
| `topology.no_relay_exported` | invalid | 2 | module '{module}' exports no relay declaration |  |
| `topology.no_steps_body` | invalid | 2 | steps class '{factory_id}' declares no channels body, so it has no tasklets to run |  |
| `topology.no_tasklets` | invalid | 2 | the class declares no tasklet; a host class declares at least one |  |
| `topology.node_identity_collision` | invalid | 2 | node identity '{name}' is declared by two distinct nodes; a node name must be unique per relay and distinct from every tasklet name |  |
| `topology.output_on_entry` | invalid | 2 | entry channel '{channel}' is given an output, and an output is fed by a tasklet; wire the entry channel into one first |  |
| `topology.output_without_type` | invalid | 2 | relay '{factory_id}' names an output, and its tasklet produces no value to feed it |  |
| `topology.partitions_without_keyed_tasklet` | invalid | 2 | factory '{factory}' has no keyed entry-fed tasklet, so each partition would fold a slice of the input into its own copy of the same state; declare key_by on the entry edge before partitioning |  |
| `topology.partitions_without_per_input_loop` | invalid | 2 | factory '{factory}' reads one message (each {each}), so its {partitions} partitions would each wait for a message that reaches only one of them; declare each per_input before partitioning |  |
| `topology.payload_type_mismatch` | invalid | 2 | channel '{channel}' delivers {delivered}, which does not match the input type {input} of tasklet '{tasklet}' |  |
| `topology.plugin_channel_with_consumers` | invalid | 2 | plugin channel '{channel}' cannot name consumers |  |
| `topology.producer_out_of_range` | invalid | 2 | channel '{channel}' names producer {producer} but the class declares {tasklets} tasklet(s) |  |
| `topology.relay_field_on_tasklet` | invalid | 2 | tasklet '{tasklet}' declares {fields}, which a relay declaration carries; declare the short form with {instead} |  |
| `topology.retired_callback` | invalid | 2 | tasklet '{tasklet}' defines {callback}, which no longer exists; use {instead} |  |
| `topology.retry_max_zero` | invalid | 2 | channel '{channel}' declared retry_max 0; at least one attempt is required (fail an input as non-retryable to park it without retries) |  |
| `topology.schema_floor_above_version` | invalid | 2 | tasklet '{tasklet}' reads back to schema version {floor}, above its own schema version {version} |  |
| `topology.schema_floor_without_migration` | invalid | 2 | tasklet '{tasklet}' reads back to schema version {floor} but supplies no migration, so the first older record would fail instead of the start |  |
| `topology.setting_on_short_form` | invalid | 2 | short form '{factory_id}' declares {fields}, which a short form does not carry: its kind is its declaration's, its tasklet is named after its id, and every class-level setting keeps its default |  |
| `topology.short_form_field_on_full_form` | invalid | 2 | relay '{factory_id}' declares {fields} beside its channels body; a full form wires entry channels, codecs and outputs in channels and declares each tasklet's fields on the tasklet |  |
| `topology.short_form_name_mismatch` | invalid | 2 | '{class}' declares relay id '{factory_id}' and tasklet name '{name}'; a short form's tasklet is named after its id, so the two must be equal |  |
| `topology.short_form_not_a_tasklet` | invalid | 2 | '{class}' is declared a short-form relay with {annotation} but is not a tasklet |  |
| `topology.short_form_with_topology` | invalid | 2 | relay '{factory_id}' is a single-tasklet short form and also declares a channels body; declare one or the other |  |
| `topology.state_not_constructible` | invalid | 2 | tasklet '{tasklet}' has state class '{state}' with no accessible no-argument constructor; add one, or write initState |  |
| `topology.state_type_id_collision` | invalid | 2 | tasklet '{tasklet}' has state types '{first}' and '{second}' that derive the same encoding id {id}; rename one, or list the types to own their ids |  |
| `topology.state_unrepresentable` | invalid | 2 | {path} cannot be encoded for durable state: {reason} |  |
| `topology.static_tasklet_without_cell_keys` | invalid | 2 | static tasklet '{tasklet}' declares no cell_keys |  |
| `topology.tasklet_name_missing` | invalid | 2 | tasklet class '{tasklet}' declares no name, and the name is where its state is saved |  |
| `topology.transform_after_key_by` | invalid | 2 | channel '{channel}' declares a transform after key_by; the key is evaluated on the final post-transform value, so it must be the last link before into |  |
| `topology.transform_after_terminal` | invalid | 2 | channel '{channel}' declares a transform or key_by after an output; the output serializes the channel's own value, so nothing downstream of it can be reshaped |  |
| `topology.two_clocks` | invalid | 2 | tasklet '{tasklet}' declares two clocks: it exports event_time and its logical_clock answers true; declare one |  |
| `topology.unkeyed_channel_under_effectively_once` | invalid | 2 | channel '{channel}' has a plugin wire but is not declared keyed; the effectively-once tier deduplicates by output identity, so every plugin-bound channel of an effectively_once relay must opt in with keyed |  |
| `topology.unkeyed_edge_into_dynamic_tasklet` | invalid | 2 | channel '{channel}' feeds tasklet '{tasklet}' without a key_by, but another edge keys it into dynamic mode; every edge into a dynamic-mode tasklet must name the Cell its inputs reach |  |
| `topology.unknown_codec` | invalid | 2 | channel '{channel}' names codec '{codec}', which is not one of {codecs} |  |
| `topology.unknown_field` | invalid | 2 | {declaration} declares {fields}, which it does not carry |  |
| `topology.unknown_sdk_lang` | invalid | 2 | sdk_lang {sdk_lang} is not an allocated SDK language; every class declares the byte its SDK holds in the allocation table, because the controller places a relay only on a runner of its factory's language |  |
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
| `usage.not_text` | invalid | 2 | {option} must be text, not {value} |  |
| `usage.option_required` | invalid | 2 | {option} is required | `{usage}` |
| `usage.option_requires_value` | invalid | 2 | {option} requires a value |  |
| `usage.option_takes_no_value` | invalid | 2 | {option} takes no value |  |
| `usage.options_conflict` | invalid | 2 | {option} and {other} cannot be given together |  |
| `usage.text_only` | invalid | 2 | {what} is rendered as text only | `{alternative}` |
| `usage.unexpected_argument` | invalid | 2 | unexpected argument '{argument}' | `{usage}` |
| `usage.unknown_command` | invalid | 2 | '{word}' is not a {command} command; use one of {allowed} | `{command} help` |
| `usage.unknown_option` | invalid | 2 | unknown option '{option}' | `{usage}` |
| `usage.unknown_word` | invalid | 2 | unknown {noun} '{value}'; use one of {allowed} |  |
