# Command reference

Every `flz` verb in one line, by group. A group's name leads to its help and
examples. `flz <group> help` and `flz <group> <verb> --help` print the same
on the command line.

## [`flz controller`](controller_commands.md#flz-controller)

| Command | What it does |
| --- | --- |
| `flz controller start [--mode <mode>]` | Starts the controller, in this terminal or as a daemon |
| `flz controller stop [--timeout-ms <n>] [--no-wait] [--force]` | Stops it once its relays have drained |
| `flz controller status` | Prints its settings, and whether a shutdown is running |
| `flz controller mem-top` | Lists what uses the most memory in the controller |

## [`flz boot`](controller_commands.md#flz-boot)

| Command | What it does |
| --- | --- |
| `flz boot status` | Says whether the controller is ready, and which conditions are met |
| `flz boot await [--timeout <secs>]` | Waits until the controller is ready |

## [`flz cluster`](controller_commands.md#flz-cluster)

| Command | What it does |
| --- | --- |
| `flz cluster status` | Lists the cluster's controllers, its leader and its relays' health |

## [`flz runner`](controller_commands.md#flz-runner)

| Command | What it does |
| --- | --- |
| `flz runner list` | Lists the runners connected to a namespace |
| `flz runner check` | Checks that a runner on this machine can reach the controller's socket |

## [`flz observe`](controller_commands.md#flz-observe)

| Command | What it does |
| --- | --- |
| `flz observe sources` | Lists the sources of metrics and events, and whether each is on |
| `flz observe sinks` | Lists where signals are sent |
| `flz observe tail <source>` | Prints a source's signals as they happen |
| `flz observe enable <source>` | Turns a source on |
| `flz observe disable <source>` | Turns a source off |

## [`flz reset`](controller_commands.md#flz-reset)

| Command | What it does |
| --- | --- |
| `flz reset [--dry-run] [--yes] [--include-...]` | Deletes a stopped controller's data on disk |

## [`flz mcp`](../working_with_ai_assistants.md)

| Command | What it does |
| --- | --- |
| `flz mcp` | Serves Flezha to an AI assistant over MCP, on stdin and stdout |

## [`flz template`](template_commands.md)

| Command | What it does |
| --- | --- |
| `flz template list` | Prints the languages and examples the release carries |
| `flz template create <language> <dir> [--example <name>] [--tar]` | Writes a template's files into a new folder, or as a tar stream |

## [`flz relay`](relay_commands.md#flz-relay)

| Command | What it does |
| --- | --- |
| `flz relay start <factory_id>` | Starts a relay from a factory a runner offers |
| `flz relay stop <relay_id>` | Stops a relay |
| `flz relay list` | Lists the relays in a namespace, with their health |
| `flz relay info [<relay_id>]` | Shows each relay's details and plugins, or one relay's API |
| `flz relay diagnose <relay_id>` | Says what is wrong with a relay and what to do |
| `flz relay state <relay_id>` | Prints a relay's state |
| `flz relay send <relay_id> --text <payload>` | Sends an input |
| `flz relay call <relay_id> (--text <payload> \| --arg <name=value>...)` | Calls one channel by the relay's API and prints its answer |
| `flz relay listen <relay_id>` | Prints what a relay emits, or with `--input` what it takes in |
| `flz relay scale <group> <partitions>` | Changes the number of members of a partition group |
| `flz relay rewind <relay_id> --to-iteration <n>` | Takes a workflow loop back to an earlier iteration |
| `flz relay replay <relay/tasklet/key> --from <seq|time>` | Runs a cell's kept inputs again from a point in the past |

## [`flz state`](relay_commands.md#flz-state)

| Command | What it does |
| --- | --- |
| `flz state get <relay/tasklet[/key]>` | Prints a cell's current state |
| `flz state list <relay>` | Lists a relay's cells |
| `flz state at <relay/tasklet/key> <seq|time>` | Prints a cell's state at a point in the past |
| `flz state rotate <relay/tasklet/key>` | Writes a snapshot of a cell's state now |
| `flz state retention <relay> <hours>` | Sets how long past states are kept |
| `flz state delete <relay/tasklet/key>` | Deletes a cell's stored state |
| `flz state outbox <relay/tasklet/key>` | Shows a cell's effectively-once outbox |
| `flz state journal <relay/instance>` | Shows the steps a workflow instance has committed |
| `flz state inbox <relay/instance>` | Shows a workflow instance's stored inputs |

## [`flz dlq`](relay_commands.md#flz-dlq)

| Command | What it does |
| --- | --- |
| `flz dlq list [relay]` | Lists the parked inputs |
| `flz dlq show <entry>` | Shows one parked input |
| `flz dlq replay <entry>` | Sends it through the relay again |
| `flz dlq drop <entry>` | Deletes it |

## [`flz schedule`](relay_commands.md#flz-schedule)

| Command | What it does |
| --- | --- |
| `flz schedule add <name> --cron <expr> --tz <zone> --factory <factory_id>` | Adds a schedule |
| `flz schedule list` | Lists the schedules with their next start |
| `flz schedule remove <name>` | Removes a schedule |
| `flz schedule run <name>` | Starts one instance now |

## [`flz plugin`](plugin_commands.md#flz-plugin)

| Command | What it does |
| --- | --- |
| `flz plugin install <path-or-https-url>` | Places a plugin package in the plugin folder |
| `flz plugin uninstall <plugin_id>` | Removes a plugin package |
| `flz plugin sync` | Makes the plugin folder hold exactly what `FLEZHA_PLUGINS` lists |
| `flz plugin enable <name> <provider_id>` | Enables a plugin under a name; `--relay_id` binds a relay too |
| `flz plugin bind <name> --relay_id <op>` | Connects a relay to an enabled plugin |
| `flz plugin unbind <name> --relay_id <op>` | Disconnects it; the plugin stays enabled |
| `flz plugin update <name>` | Changes a plugin's settings |
| `flz plugin disable <name>` | Turns a plugin off for every relay |
| `flz plugin list` | Lists the enabled plugins; `--installed` the packages, `--providers` the providers |
| `flz plugin help <provider_id>` | Lists a provider's settings and hooks |

## [`flz namespace`](access_commands.md#flz-namespace)

| Command | What it does |
| --- | --- |
| `flz namespace create <name>` | Creates a namespace |
| `flz namespace list` | Lists the namespaces |
| `flz namespace show <name>` | Shows a namespace's quotas |
| `flz namespace set-quota <name> --X<quota>=<value>` | Changes its quotas |
| `flz namespace delete <name>` | Deletes a namespace |

## [`flz auth`](access_commands.md#flz-auth)

| Command | What it does |
| --- | --- |
| `flz auth bootstrap <token>` | Claims the first admin with the controller's bootstrap token |
| `flz auth whoami` | Shows who the current token belongs to |
| `flz auth rebootstrap` | Recovers a lost admin |

## [`flz token`](access_commands.md#flz-token)

| Command | What it does |
| --- | --- |
| `flz token issue <principal> <namespace> <role>` | Issues a token, shown once |
| `flz token list` | Lists the tokens |
| `flz token revoke <token-id>` | Revokes a token |

## [`flz role`](access_commands.md#flz-role)

| Command | What it does |
| --- | --- |
| `flz role bind <principal> <role> <namespace>` | Gives a principal a role |
| `flz role unbind <principal> [<role>]` | Takes one role away, or all of them |
| `flz role list` | Lists who has which role |

## [`flz audit`](access_commands.md#flz-audit)

| Command | What it does |
| --- | --- |
| `flz audit list` | Lists the audit files |
| `flz audit read <file>` | Prints the records of one file |
| `flz audit tail` | Prints new records as they happen |
| `flz audit stats` | Shows the audit log's size and counts |
| `flz audit rotate` | Starts a new audit file now |
