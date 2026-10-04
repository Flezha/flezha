# Command reference

Every `flz` verb in one line, by group. A group's name leads to its help and
examples. `flz <group> help` and `flz <group> <verb> --help` print the same
on the command line.

## [`flz controller`](controller_commands.md#flz-controller)

| Command | What it does |
| --- | --- |
| `flz controller start [--mode <mode>]` | Starts the controller, in this terminal or as a daemon |
| `flz controller stop [--timeout-ms <n>] [--no-wait] [--force]` | Stops it once its operators have drained |
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
| `flz cluster status` | Lists the cluster's controllers, its leader and its operators' health |

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

## [`flz operator`](operator_commands.md#flz-operator)

| Command | What it does |
| --- | --- |
| `flz operator start <factory_id>` | Starts an operator from a factory a runner offers |
| `flz operator stop <operator_id>` | Stops an operator |
| `flz operator list` | Lists the operators in a namespace, with their health |
| `flz operator info` | Shows each operator's details and plugins |
| `flz operator state <operator_id>` | Prints an operator's state |
| `flz operator send <operator_id> --text <payload>` | Sends an input |
| `flz operator call <operator_id> --text <payload>` | Sends an input and prints the operator's answer |
| `flz operator listen <operator_id>` | Prints what an operator emits, or with `--input` what it takes in |
| `flz operator scale <group> <partitions>` | Changes the number of members of a partition group |
| `flz operator rewind <operator_id> --to-iteration <n>` | Takes a workflow loop back to an earlier iteration |
| `flz operator replay <operator/tasklet/key> --from <seq|time>` | Runs a cell's kept inputs again from a point in the past |

## [`flz state`](operator_commands.md#flz-state)

| Command | What it does |
| --- | --- |
| `flz state get <operator/tasklet[/key]>` | Prints a cell's current state |
| `flz state list <operator>` | Lists an operator's cells |
| `flz state at <operator/tasklet/key> <seq|time>` | Prints a cell's state at a point in the past |
| `flz state rotate <operator/tasklet/key>` | Writes a snapshot of a cell's state now |
| `flz state retention <operator> <hours>` | Sets how long past states are kept |
| `flz state delete <operator/tasklet/key>` | Deletes a cell's stored state |
| `flz state outbox <operator/tasklet/key>` | Shows a cell's effectively-once outbox |
| `flz state journal <operator/instance>` | Shows the steps a workflow instance has committed |
| `flz state inbox <operator/instance>` | Shows a workflow instance's stored inputs |

## [`flz dlq`](operator_commands.md#flz-dlq)

| Command | What it does |
| --- | --- |
| `flz dlq list [operator]` | Lists the parked inputs |
| `flz dlq show <entry>` | Shows one parked input |
| `flz dlq replay <entry>` | Sends it through the operator again |
| `flz dlq drop <entry>` | Deletes it |

## [`flz schedule`](operator_commands.md#flz-schedule)

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
| `flz plugin enable <name> <provider_id>` | Enables a plugin under a name; `--operator_id` binds an operator too |
| `flz plugin bind <name> --operator_id <op>` | Connects an operator to an enabled plugin |
| `flz plugin unbind <name> --operator_id <op>` | Disconnects it; the plugin stays enabled |
| `flz plugin update <name>` | Changes a plugin's settings |
| `flz plugin disable <name>` | Turns a plugin off for every operator |
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
