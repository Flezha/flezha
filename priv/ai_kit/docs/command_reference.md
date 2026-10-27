# Command reference

Every `flz` verb with its usage line and what it does, by group. A group's
name leads to its help and examples. The tables are rendered from the same
rows as `flz <group> help`, which prints them on the command line, and
`flz <group> <verb> --help` prints one verb's options.

## [`flz controller`](controller_commands.md#flz-controller)

<!-- flz-reference:begin flz controller -->
| Command | What it does |
| --- | --- |
| `flz controller start [--mode \| -m attached\|daemon\|attached_iex\|daemon_iex]` | Start the flezha controller. |
| `flz controller stop [--timeout-ms <ms>] [--no-wait] [--force]` | Drain this controller and stop it. |
| `flz controller status` | The controller's status: node, clock, shutdown and durability. |
| `flz controller list` | Every connected controller, the one that answered first. |
| `flz controller mem-top` | One snapshot of the controller's memory hot spots. |
<!-- flz-reference:end -->

## [`flz boot`](controller_commands.md#flz-boot)

<!-- flz-reference:begin flz boot -->
| Command | What it does |
| --- | --- |
| `flz boot status` | The readiness verdict and every precondition behind it. |
| `flz boot await [--timeout \| -t <seconds>]` | Wait until the controller is ready, or the timeout passes. |
<!-- flz-reference:end -->

## [`flz cluster`](controller_commands.md#flz-cluster)

<!-- flz-reference:begin flz cluster -->
| Command | What it does |
| --- | --- |
| `flz cluster status [--namespace \| -n <namespace>]` | The cluster as one namespace sees it: nodes, relays and health. |
<!-- flz-reference:end -->

## [`flz runner`](controller_commands.md#flz-runner)

<!-- flz-reference:begin flz runner -->
| Command | What it does |
| --- | --- |
| `flz runner list [--namespace \| -n <namespace>]` | Every runner serving a namespace, anywhere in the cluster. |
| `flz runner check` | Report whether a runner on this host can reach a controller. |
| `flz runner drop <runner_id> [--namespace \| -n <namespace>]` | Drop one runner serving a namespace. |
| `flz runner dropped [--namespace \| -n <namespace>]` | The drop marks standing in a namespace, oldest first. |
| `flz runner readmit <stable_id> [--namespace \| -n <namespace>]` | Lift the drop mark on a runner in a namespace. |
<!-- flz-reference:end -->

## [`flz observe`](controller_commands.md#flz-observe)

<!-- flz-reference:begin flz observe -->
| Command | What it does |
| --- | --- |
| `flz observe sources` | Every registered observability source and what it said recently. |
| `flz observe sinks` | Every registered observability sink. |
| `flz observe tail <source> [--duration-ms <milliseconds>]` | Stream the live signals of one source that the caller may see. |
| `flz observe enable <source>` | Enable an observability source. |
| `flz observe disable <source>` | Disable an observability source. |
<!-- flz-reference:end -->

## [`flz reset`](controller_commands.md#flz-reset), [`flz mcp`](../core/mcp_server.md) and `flz help`

<!-- flz-reference:begin flz -->
| Command | What it does |
| --- | --- |
| `flz reset [--yes] [--dry-run] [--include-certs] [--include-dlq] [--include-schedules] [--include-auth] [--include-audit] [--include-plugins] [--include-sockets] [--all] [--probe-timeout-ms <ms>] [--storage-root <path>]` | Clear Controller durable runtime data on disk (controller must be stopped). |
| `flz mcp` | Serve Flezha to an AI assistant over MCP, on stdin and stdout. |
| `flz help` | Show this help message, or with --json every operation as data. |
<!-- flz-reference:end -->

## [`flz relay`](relay_commands.md#flz-relay)

<!-- flz-reference:begin flz relay -->
| Command | What it does |
| --- | --- |
| `flz relay start <factory_id> [--id \| -i <id>] [--namespace \| -n <namespace>] [--init-values <json>] [--text \| -t <payload>] [--channel \| -c <ch>] [--plugins <json-array>] [--partitions <N>]` | Start a relay in a namespace, answering the id it runs under. |
| `flz relay scale <group> <partitions> [--namespace \| -n <namespace>] [--timeout \| -t <ms>]` | Rescale a partition group in a namespace to a new size. |
| `flz relay stop <relay_id> [--namespace \| -n <namespace>]` | Stop a relay in a namespace, or a whole partition group. |
| `flz relay state <relay_id> [--namespace \| -n <namespace>]` | A relay's live state, as JSON, and the change sequence it was read at. |
| `flz relay list [--namespace \| -n <namespace>]` | The relays in a namespace, one row each. |
| `flz relay info [<relay_id>] [--namespace \| -n <namespace>]` | Every relay in a namespace as its Overwatch reports it. |
| `flz relay diagnose <relay_id> [--namespace \| -n <namespace>]` | What is wrong with a relay and what to do about it. |
| `flz relay graph <relay_id> [--namespace \| -n <namespace>]` | The graph of a relay's factory, as its runner announces it. |
| `flz relay activity <relay_id> [--cells <tasklet>] [--limit <count>] [--namespace \| -n <namespace>]` | What each tasklet of a relay is doing, as its runner counts it. |
| `flz relay listen <relay_id> [--input] [--calls] [--tally] [--duration-ms <milliseconds>] [--namespace \| -n <namespace>]` | Stream every frame a relay emits, or those of one direction. |
| `flz relay send <relay_id> --text \| -t <payload> [--channel \| -c <ch>] [--key <key>] [--msg-id <id>] [--await <ms>] [--namespace \| -n <namespace>]` | Inject a message into a relay's input stream. |
| `flz relay call <relay_id> [--text \| -t <payload>] [--arg <name=value>] [--channel \| -c <ch>] [--answer-channel <ch>] [--key <key>] [--timeout <ms>] [--namespace \| -n <namespace>]` | Call an entry channel of a relay with a value. |
| `flz relay rewind <relay_id> --to-iteration <N> [--namespace \| -n <namespace>]` | Rewind a relay class in a namespace to an earlier iteration. |
| `flz relay replay <relay/tasklet/key> --from \| -f <seq\|timestamp> [--namespace \| -n <namespace>]` | Re-run a Cell's retained inputs from a point in the past. |
<!-- flz-reference:end -->

## [`flz state`](relay_commands.md#flz-state)

<!-- flz-reference:begin flz state -->
| Command | What it does |
| --- | --- |
| `flz state get <relay/tasklet[/cell_key]>` | The CURRENT in-memory state of live Cells, as JSON. |
| `flz state list <relay_class>` | One row per Cell of a relay class with persisted state. |
| `flz state at <relay/tasklet/cell_key> <seq \| iso8601-timestamp>` | One Cell's state at a past point, from its write-ahead log. |
| `flz state rotate <relay/tasklet/cell_key>` | Force a compaction of one Cell. |
| `flz state retention <relay_class> <hours>` | Set how long a relay class keeps its write-ahead history. |
| `flz state delete <relay/tasklet/cell_key>` | Hard-delete one Cell's persisted state. |
| `flz state outbox <relay/tasklet/cell_key>` | One Cell's outbox, with its cursors, epoch and lag. |
| `flz state journal <relay/instance>` | One instance's task journal, in append order, and the run's outcome. |
| `flz state inbox <relay/instance>` | An instance's durable input log, record by record. |
<!-- flz-reference:end -->

## [`flz dlq`](relay_commands.md#flz-dlq)

<!-- flz-reference:begin flz dlq -->
| Command | What it does |
| --- | --- |
| `flz dlq list [<relay_class>]` | Parked entries, nearest-to-expiry first. |
| `flz dlq show <relay/tasklet/entry-id>` | One parked entry, with its payload bytes if it carries any. |
| `flz dlq replay <relay/tasklet/entry-id>` | Re-dispatch a parked payload to its relay. |
| `flz dlq drop <relay/tasklet/entry-id>` | Discard a parked entry and its payload. |
<!-- flz-reference:end -->

## [`flz schedule`](relay_commands.md#flz-schedule)

<!-- flz-reference:begin flz schedule -->
| Command | What it does |
| --- | --- |
| `flz schedule add <name> --cron <expr> --tz <iana> --factory <factory_id> [--namespace \| -n <namespace>] [--init-values <json>] [--text \| -t <payload>] [--channel \| -c <ch>] [--plugins \| -p <json>]` | Store a schedule and assert it on the hub. |
| `flz schedule list` | Every stored schedule, by name, with what the hub holds for it. |
| `flz schedule remove <name>` | Forget a schedule and retract it from the hub. |
| `flz schedule run <name>` | Fire a schedule once, now. |
<!-- flz-reference:end -->

## [`flz plugin`](plugin_commands.md#flz-plugin)

<!-- flz-reference:begin flz plugin -->
| Command | What it does |
| --- | --- |
| `flz plugin enable <name> <provider_id> [X<key>=<value> ...] [--<key> <value> ...] [--single]` | Enable a plugin from its provider and options. |
| `flz plugin bind <name> [X<key>=<value> ...] [--<key> <value> ...]` | Bind a relay to a plugin. |
| `flz plugin unbind <name> [X<key>=<value> ...] [--<key> <value> ...]` | Unbind a relay from a plugin. |
| `flz plugin update <name> [X<key>=<value> ...] [--<key> <value> ...]` | Change the options and hooks of a plugin. |
| `flz plugin disable <name> [X<key>=<value> ...] [--<key> <value> ...]` | Disable a plugin: every running copy stops. |
| `flz plugin help [<provider_id>]` | One plugin provider's help text, type, hooks and options. |
| `flz plugin list [--installed] [--providers] [--namespace \| -n <namespace>]` | Every plugin enabled in a namespace. |
| `flz plugin install <path-or-https-url>` | Place a plugin package (.fpz) from a path or an https:// URL. |
| `flz plugin uninstall <plugin_id>` | Remove an installed plugin package. |
| `flz plugin sync` | Make the plugin root hold exactly the packages FLEZHA_PLUGINS declares. |
<!-- flz-reference:end -->

## [`flz namespace`](access_commands.md#flz-namespace)

<!-- flz-reference:begin flz namespace -->
| Command | What it does |
| --- | --- |
| `flz namespace list` | The namespaces in the durable catalog the caller may be shown. |
| `flz namespace create <name> [X<key>=<value> ...] [--<key> <value> ...]` | Create a namespace. |
| `flz namespace delete <name>` | Delete a namespace and the quota counters keyed on it. |
| `flz namespace show <name>` | One namespace record. |
| `flz namespace set-quota <name> [X<key>=<value> ...] [--<key> <value> ...]` | Replace the namespace's quota overrides. |
| `flz namespace set-migration <name> <cold\|hot>` | Set how the namespace's hub moves its relays, `cold` or `hot`. |
<!-- flz-reference:end -->

## [`flz auth`](access_commands.md#flz-auth)

<!-- flz-reference:begin flz auth -->
| Command | What it does |
| --- | --- |
| `flz auth bootstrap` | Claim the first admin with the one-time bootstrap token. |
| `flz auth whoami` | The identity the caller's credential resolves to. |
| `flz auth rebootstrap [--storage-root <path>]` | Break-glass: recover a lost admin@_system. |
<!-- flz-reference:end -->

## [`flz token`](access_commands.md#flz-token)

<!-- flz-reference:begin flz token -->
| Command | What it does |
| --- | --- |
| `flz token issue <principal> <namespace> <viewer\|ops\|admin\|runner> [--ttl <seconds>\|never]` | Issue a token for a principal holding a role in a namespace. |
| `flz token revoke <token-id>` | Revoke the token whose id starts with the given prefix. |
| `flz token list` | The tokens the caller may see, one row each. |
<!-- flz-reference:end -->

## [`flz role`](access_commands.md#flz-role)

<!-- flz-reference:begin flz role -->
| Command | What it does |
| --- | --- |
| `flz role bind <principal> <viewer\|app\|ops\|admin\|runner\|spawner\|stopper> <namespace>` | Bind a principal to a role in a namespace. |
| `flz role unbind <principal> [<viewer\|app\|ops\|admin\|runner\|spawner\|stopper>]` | Unbind a role from a principal, or the principal's whole binding. |
| `flz role list` | The role bindings the caller may see, one per identity. |
<!-- flz-reference:end -->

## [`flz audit`](access_commands.md#flz-audit)

<!-- flz-reference:begin flz audit -->
| Command | What it does |
| --- | --- |
| `flz audit list` | The audit log's files, each with its record times and digest. |
| `flz audit read <file> [--source <source>] [--name <name>] [--since <microseconds>] [--until <microseconds>] [--limit <count>]` | The records of one audit file that match the given filters. |
| `flz audit tail [--duration-ms <milliseconds>]` | Stream every audit record the caller may read as it is written. |
| `flz audit stats` | The audit log's operational snapshot. |
| `flz audit rotate` | Close the current audit file and start a new one. |
<!-- flz-reference:end -->
