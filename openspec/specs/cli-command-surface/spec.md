# cli-command-surface Specification

## Purpose

The `flz` command line is a user-facing surface: how an option may be spelled, what a
subcommand list must name, and that every screen is pinned by a capture. These rules were
enforced by habit and by three goldens, and stated in no specification — which is why an
option spelling and a subcommand list could each contradict nothing.
## Requirements
### Requirement: An option's four spellings SHALL be one option

Every `flz` option SHALL be read in all four of `--long=value`, `--long value`,
`-s=value` and `-s value`, and a short alias SHALL resolve to its long key in each of
them. A reader that recognises an option in one spelling and not another SHALL NOT
exist: an option the user typed is either read or refused with the flag named,
never dropped in silence.

A bare word containing `=` and no leading `-` is a positional argument and SHALL NOT
be read as an option, except the plugin-setting form governed by the requirement
below.

#### Scenario: A short flag written with `=` reaches the request

- **WHEN** `flz plugin enable p prov -o=json` is run
- **THEN** the enable request SHALL carry the option `output` with the value `json`,
  identically to `--output=json`, `--output json` and `-o json`

#### Scenario: A positional `key=value` word is not an option

- **WHEN** an argument list contains a bare word such as `topic=t`
- **THEN** no option named `topic` SHALL be read from it

### Requirement: A subcommand list SHALL name every subcommand its command answers

A `flz` help screen carrying a `COMMANDS:` or `SUBCOMMANDS:` block SHALL list in that
block every subcommand the command answers, `help` included. A command that answers a
verb its own screen does not name leaves the user to discover it elsewhere, which
is what a help screen exists to prevent.

A screen that documents whole invocations, or options rather than subcommands, carries
no such block and is not governed by this requirement.

#### Scenario: Every command table's screen offers `help`

- **WHEN** the help screens are enumerated
- **THEN** every screen carrying a `COMMANDS:` or `SUBCOMMANDS:` block SHALL list
  `help` among its rows

### Requirement: Every help screen SHALL be pinned byte for byte by a capture

Each screen `flz` prints SHALL be captured from the binary and compared byte for byte
by the test suite, so that any change to user-visible text is deliberate and
visible in a diff. The screens are hand-aligned, so a renderer that computes a column
would silently change what a user sees; the capture is what makes the text
editable at all.

The set of captures and the set of screens SHALL be equal, so that a screen cannot
stop being checked without the suite saying so.

#### Scenario: An edited screen fails until its capture is retaken

- **WHEN** a help screen's text changes and its capture does not
- **THEN** the suite SHALL fail, naming the screen and the first line that differs

#### Scenario: A screen without a capture fails

- **WHEN** a screen has no capture file, or a capture file names no screen
- **THEN** the suite SHALL fail

### Requirement: A plugin setting SHALL be read with or without its leading dashes

A repeatable plugin setting SHALL be read from `X<key>=<value>` as well as from
`--X<key>=<value>` and `--X<key> <value>`, on every surface that takes settings —
`flz plugin enable`, `flz namespace create` and `flz namespace set-quota`. The bare
spelling is what the CLI's own help screens and usage lines teach, and a setting a
user typed that reaches no request is a plugin configured with nothing and told
nothing.

A word is a setting when, and only when, it is spelled `X<key>=<value>` with a
non-empty `<key>`. The `=` is what makes the bare spelling readable: with no leading
dashes there is nothing else to separate `Xhosts` from a positional word, so a bare
`X<key>` SHALL NOT consume the argument after it as its value, and a bare word whose
key does not begin with `X` SHALL remain a positional argument.

#### Scenario: The documented invocation carries its settings

- **WHEN** `flz plugin enable kafka_producer my_provider Xhosts=kafka:9092 Xtopic=my_topic` is run
- **THEN** the enable request SHALL carry the settings `hosts` and `topic` with the
  values the user typed, identically to the `--Xhosts=` / `--Xtopic=` spelling

#### Scenario: A bare setting does not eat the next word

- **WHEN** an argument list contains a bare `Xhosts` with no `=`
- **THEN** no setting SHALL be read from it, and the argument after it SHALL NOT be
  consumed as its value

### Requirement: Every option spelling the CLI prints SHALL be one it reads

A spelling the CLI prints SHALL be one the parser reads. Every option spelling that
appears in a `flz` help screen, or in a usage line the CLI prints, is read by the
parser that answers that command. The printed text is the interface's promise; a
spelling advertised and not read is an instruction that silently does nothing, which
is worse than one that is refused.

The plugin-setting family SHALL be checked mechanically: the test suite SHALL scan the
user-visible text for every settings-shaped token it prints and assert the parser
reads each one back with the key and value the text shows, so that an example added to
a screen is covered by the check without anyone remembering to extend it.

#### Scenario: An advertised setting the parser cannot read fails the suite

- **WHEN** a help screen or usage line prints a settings-shaped token the parser does
  not read
- **THEN** the suite SHALL fail, naming the text it came from and the token

### Requirement: A verb whose effect the user's words choose SHALL refuse a word it cannot act on

`flz boot await` and `flz reset` SHALL refuse an argument they do not recognise, naming the
argument on stderr, exiting non-zero and reaching no controller call. These two verbs are
the ones where guessing is worse than stopping: one blocks the user for as long as its
option says, the other deletes state. A typed word neither can act on means the run about to
happen is not the run that was asked for.

Refusing an argument the verb **does not recognise** is a guarantee about these two verbs
and not about the whole command line: every other `flz` verb passes an unrecognised word on
and answers with its defaults. Refusing an option written with **no value** is a separate
guarantee and is given by every verb, under the requirement above; these two verbs are not
excused from it and SHALL NOT re-state it in a walk of their own.

The refusal SHALL survive any widening of how these verbs read their options — an option
read in more spellings must not be an argument list checked in fewer, because the silence
that replaces a refusal is the failure this surface's requirements exist to prevent.

Which verbs refuse SHALL be a field of the verb's declaration rather than a walk each verb
writes for itself, so that the guarantee is read from one place and extending it to a third
verb is a decision recorded in that verb's row.

#### Scenario: An argument the verb cannot act on stops the run

- **WHEN** `flz boot await --nope` is run
- **THEN** the run SHALL exit non-zero, name `--nope` on stderr, and reach no
  `CliManager` call

#### Scenario: An option written with no value stops the run

- **WHEN** `flz boot await --timeout` is run with nothing after it
- **THEN** the run SHALL exit non-zero saying the option requires a value, and SHALL NOT
  fall back to the default wait

#### Scenario: Strictness is read from the declaration

- **WHEN** the verbs that refuse an unrecognised argument are enumerated
- **THEN** they SHALL be exactly the verbs whose rows declare it, and no verb SHALL carry
  an argument walk of its own to obtain it

### Requirement: An option written with no value SHALL be refused, never defaulted

Every `flz` option that takes a value SHALL be refused when the user writes it with
nothing after it, naming the flag on stderr, exiting non-zero and reaching no controller
call. An option that is absent and an option written with no value are different requests:
the first asks for the default, the second asks for a value the user did not supply.
A reader that cannot tell them apart answers the second with the first, in silence, and the
user is told nothing, which is the failure "either read or refused with the flag named"
already forbids for a spelling and must equally forbid for a value.

The distinction SHALL be carried by the option scanner rather than re-derived by each
reader. A scanner that answers only *value* or *nothing* forces every caller to guess which
of the two it was handed, and a refusal written at each call site is the same sentence
copied once per option.

A value the user wrote and left empty is a value that was supplied, not a value that is
missing: an option given an explicitly empty argument SHALL NOT be refused by this rule, and
the reader's own rule for an empty value applies.

#### Scenario: A trailing option is refused instead of taking the default

- **WHEN** `flz token issue p ns viewer --ttl` is run with nothing after `--ttl`
- **THEN** the run SHALL exit non-zero naming `--ttl` on stderr, and SHALL reach no
  `CliManager` call issuing a token with the default lifetime

#### Scenario: An option that is absent still takes its default

- **WHEN** `flz token issue p ns viewer` is run with no `--ttl` at all
- **THEN** the token SHALL be issued with the default lifetime, exactly as before

#### Scenario: A switch spelling may not short-circuit the refusal

- **WHEN** a command is given both the `--json` switch and a trailing `--output`
- **THEN** the run SHALL be refused naming `--output`, rather than answered with the
  format the switch asked for

#### Scenario: An explicitly empty value is not a missing value

- **WHEN** an option is written with an empty argument, such as `--init-values ""`
- **THEN** the run SHALL NOT be refused, and the reader's own rule for an empty value
  applies

### Requirement: A numeric option SHALL be refused when its value is not a number

Every `flz` option whose value is a number SHALL refuse a value it cannot read, naming the
option and the value on stderr, exiting non-zero and reaching no controller call. Dropping
the value instead leaves the reader answering with its default, which is the request the
user did not make: a `--limit` that will not parse becomes every record, a `--duration-ms`
that will not parse becomes the thirty-second default, and neither is distinguishable from an
option nobody typed.

Passing the unread value on to the controller is not an answer either. A value the command
line can see is wrong is refused where the option's name is still known, rather than sent on
as a string for something further away to reject in words that name neither the option nor
the user's spelling of it.

The parse SHALL be carried by the option scanner rather than re-derived by each reader, for
the reason the neighbouring requirement gives: the refusal names the option, and the option's
name is what the scanner has and the caller does not.

A range rule is the option's own and is not this rule. An option that additionally requires
its number to be positive, or to fall in a band, SHALL refuse out-of-range values in its own
words; only "this is not a number" is shared.

#### Scenario: A non-numeric value stops the run instead of taking the default

- **WHEN** `flz audit read f --limit abc` is run
- **THEN** the run SHALL exit non-zero naming `--limit` and the value `abc` on stderr, and
  SHALL reach no `CliManager` call reading the log

#### Scenario: A non-numeric value is not passed on to the controller

- **WHEN** `flz relay scale g 2 --timeout abc` is run
- **THEN** the run SHALL be refused, and no request carrying `abc` as a timeout SHALL be
  built

#### Scenario: A numeric value is read exactly as before

- **WHEN** `flz audit read f --limit 5` is run
- **THEN** the read request SHALL carry the limit `5`

#### Scenario: An absent numeric option still takes its default

- **WHEN** `flz audit read f` is run with no `--limit` at all
- **THEN** the read request SHALL carry no limit, exactly as before

### Requirement: One invocation SHALL build one request

The same `flz` invocation SHALL build the same controller request every time it is run. The
request SHALL be one door call naming the operation, its arguments as one JSON array whose
object keys are emitted in a fixed order, and the trailing options list.

An output that differs between two runs of one binary cannot be diffed against a run of
another, so it makes every comparison of two builds cost a proof that the difference was
not the change under test.

#### Scenario: Repeated identical invocations produce identical requests

- **WHEN** `flz relay start fac --id op --partitions 4` is run
  repeatedly against the same binary
- **THEN** every run SHALL produce the same door call, byte for byte

#### Scenario: A start without an id builds one request

- **WHEN** `flz relay start fac --partitions 4` is run repeatedly against the
  same binary
- **THEN** every run SHALL produce the same door call, byte for byte, because
  the id is generated by the controller and not by the CLI

### Requirement: A numeric positional argument SHALL be refused when its value is not a number

A positional argument the CLI reads as a number SHALL be refused by the same
reader every numeric option goes through, and SHALL NOT be forwarded to the
controller unread. A verb SHALL NOT answer the same typo two ways depending on
whether the user wrote the value as an option or as a positional.

#### Scenario: A word is written where a positional number is expected

- **WHEN** a user writes a non-numeric word in a positional slot the verb
  reads as a number
- **THEN** the CLI SHALL refuse it locally, naming the argument and the value, and
  SHALL NOT reach the controller

#### Scenario: A positional and an option are refused by one sentence

- **WHEN** a non-numeric value is written in a positional slot, and the same value
  is written behind a numeric option
- **THEN** both SHALL be refused by one sentence naming the argument and the
  value, differing only in what the argument is called — the flag for an option,
  and the name the verb's own usage line gives for a positional

#### Scenario: One verb refuses both spellings in one invocation

- **WHEN** a user writes an unreadable number as a positional and as an
  option in the same invocation
- **THEN** the verb SHALL refuse rather than refuse the option and forward the
  positional

### Requirement: Every verb SHALL carry its options and the caller's credential as one trailing list

Every request `flz` builds SHALL end with one options keyword list, whatever else the
verb carries, and the caller's credential SHALL ride that list and nothing else. The output
format the caller asked for SHALL ride the same list as `output`, set when `--json` or
`--output json` is written and absent otherwise, and no verb SHALL take the format as a
positional argument. A verb
whose optional flags were spelled as positional arguments with defaults SHALL carry
them in that list instead, so that the credential is always the last slot and a new
optional flag has somewhere to go. A verb whose only free argument is a JSON document
SHALL keep the document and gain the list beside it. This rule governs the request the
CLI builds, not the spelling of the user-facing flags. `flz relay start` SHALL
read a first message as `send` spells it, `--text`/`-t` with an optional `--channel`/`-c`,
for both relay kinds, and SHALL carry `text` and `channel` in the options list as text;
the controller SHALL deliver that message as the instance's first input on the mechanism a
tasklet's spawn uses, and an instance started without `--text` SHALL wait for `send`.
`--channel` without `--text` SHALL be refused by the verb. `--init-values` SHALL mean initial
state per tasklet for both kinds. `flz relay start` and `flz schedule add` SHALL declare
no `kind` option: the kind is the factory's, announced by its runner, and a written `--kind`
SHALL be refused as an unknown option by the same rule that refuses any option a verb does not
declare. The help screens of both verbs, pinned by their captures, SHALL NOT mention `--kind`.

#### Scenario: A verb with no options still ends with the list

- **WHEN** `flz boot status` or `flz plugin list` is run
- **THEN** the request SHALL end with an options list, empty when no token, request
  id or output format is given, and the controller SHALL apply the defaults it applies today

#### Scenario: The format rides the list

- **WHEN** `flz relay list --json` or `flz namespace delete ghost --json` is run
- **THEN** the request SHALL carry `output: "json"` in the options list and no positional
  format argument

#### Scenario: `relay start` carries its trailing flags in the list

- **WHEN** `flz relay start <factory> --id <op> --plugins '[…]'` is run
- **THEN** the request SHALL carry `id` and `plugins` in the options list, as text
  where the controller reads text, SHALL carry no `kind` entry, and SHALL carry no
  positional placeholder for an absent flag

#### Scenario: `relay start` carries a first message

- **WHEN** `flz relay start paper_order --text '{"symbol":"AAPL"}' --channel order` is run
  for a factory its runner announced as a steps relay
- **THEN** the options list SHALL carry `text` and `channel` as text, the instance SHALL
  receive that message as its first input on `order`, and a `once` steps relay SHALL run its
  pass over it

#### Scenario: `relay start` without a message starts an instance that waits

- **WHEN** `flz relay start paper_order` is run and
  `flz relay send <id> --text '{"symbol":"AAPL"}'` follows
- **THEN** the options list SHALL carry no `text` entry, the instance SHALL wait for its
  first message, and the send SHALL be that message

#### Scenario: `relay start` without an id carries none

- **WHEN** `flz relay start <factory>` is run with no `--id`
- **THEN** the options list SHALL carry no `id` entry, and the request SHALL carry no
  positional placeholder for it

#### Scenario: A written kind is an unknown option on both verbs

- **WHEN** `flz relay start echo --kind stream` or
  `flz schedule add n --cron "* * * * *" --tz UTC --factory f --kind steps` is run
- **THEN** the CLI SHALL refuse `--kind` as an option the verb does not declare, SHALL build
  no request, and `flz relay start --help` and `flz schedule --help` SHALL match their
  captures with no `--kind` line

#### Scenario: One rule, one test

- **WHEN** the CLI's request-shape test runs over every verb the wire contract exports
- **THEN** it SHALL assert the trailing list for every verb from one table, so that a
  verb added without the list fails the test rather than the user

### Requirement: Every verb SHALL declare its positional arguments and its options once

Every `flz` verb SHALL declare, in one row beside the verb's own name, the positional
arguments it takes and every option it reads — the long spelling, the short alias, the
kind of value and whether it is required — and every reader, refusal, arity check and
usage line for that verb SHALL be derived from that row rather than written beside it. A
verb that reads a word the row does not declare, or prints a word the row does not
declare, is a grammar the user cannot discover from the surface that teaches it.

The row is the only place a verb's grammar is written down. Help screens stay hand-aligned
text pinned by their captures, and SHALL be checked against the row rather than generated
from it: every option a screen or a usage line prints SHALL be declared by the row of the
verb whose screen it is, and every option the row declares SHALL appear in that screen.

#### Scenario: A verb with no declaration does not ship

- **WHEN** a command table gains a verb row that declares no positionals and no options
- **THEN** the build or the test suite SHALL fail naming the verb, rather than the verb
  reading its arguments by hand

#### Scenario: An option printed and not declared fails the suite

- **WHEN** a help screen or a usage line prints an option spelling that the printing
  verb's row does not declare
- **THEN** the suite SHALL fail naming the verb, the text it came from and the spelling

#### Scenario: A usage line is the row rendered

- **WHEN** a verb is invoked with too few positional arguments
- **THEN** the usage line it prints SHALL be rendered from its row, naming exactly the
  positionals and options the row declares, and SHALL be the same text the verb's help
  screen shows

### Requirement: An option SHALL be read wherever the user writes it

Every `flz` option SHALL be read wherever it appears in the argument list, and the
positional arguments SHALL be the words that remain once the declared options and their
values are taken out. A verb SHALL NOT decide which words are options by their position,
because a user who writes a flag before the subject of the command then has the flag
itself taken as the subject: the request that reaches the controller is one nobody asked
for, and the refusal that comes back names the wrong thing.

A word that matches no declared option is a positional argument, whatever it begins with,
except where the verb's row declares that it refuses unrecognised words.

#### Scenario: A credential written before the subject still authenticates

- **WHEN** `flz relay stop --token <token> op1` is run
- **THEN** the request SHALL name `op1` as the relay and carry `<token>` in its
  options list, identically to `flz relay stop op1 --token <token>`

#### Scenario: A namespace written before the subject reaches the request

- **WHEN** a verb that takes `--namespace` is invoked with the flag written before its
  positional arguments
- **THEN** the request SHALL carry the namespace, and the positional arguments SHALL be
  the words that remain

### Requirement: A refusal or a rendered error SHALL be written to stderr

Every refusal and every error a `flz` verb renders SHALL be written to the stderr of the process the user ran, whatever the verb's family and whatever the output format, and the stream SHALL NOT be a per-call argument. A user who pipes a verb's output into a file or another program is collecting the answer, not the failure; a failure written to stdout is a failure inside the data, and a failure written to the controller's own stderr is a failure the user never sees at all.

A verb body evaluated on the controller SHALL resolve the device it writes to from its caller, and SHALL NOT name a local stream. A named stream selects the device registered under that name on the node the body is running on, which for every controller-side verb is the controller's log rather than the user's terminal.

A failure SHALL be rendered by the error model's one renderer: in text, the line `error: <message> (<code>)` and, when the code has a hint, the line `hint: <hint>`; under `--json` or `--output json`, the one-line object `{"error": {"code", "class", "message", "details", "hint"}}`. A failure SHALL be rendered once: `flz` SHALL add no line of its own after a failure the controller rendered. `flz`'s own refusals, of the command line, of a controller it cannot reach and of an `flzvm` it cannot start, SHALL be rendered in the same two forms by the same catalogue.

A refused verb SHALL exit with the exit code of its error's class: 1 internal, 2 invalid, 3 unreachable, 4 denied, 5 not_found, 6 conflict, 7 unavailable. It SHALL halt the calling process without printing a language-level exit trace over the rendering. The verb's success output stays on stdout, and an acting verb's success output is text whatever the format.

#### Scenario: A relay failure is not in the data

- **WHEN** a `flz relay` verb fails and its stdout is captured separately from its
  stderr, with and without `--json`
- **THEN** the error SHALL appear on stderr and stdout SHALL carry none of it

#### Scenario: One device, declared nowhere per verb

- **WHEN** the controller-side verb bodies are enumerated
- **THEN** none SHALL name the stream it prints its failure to, and none SHALL render a
  reason as text itself

#### Scenario: A verb evaluated on the controller renders on the caller's stderr

- **WHEN** a `flz` verb is evaluated on the controller node through `flzvm rpc` and
  refuses
- **THEN** the refusal SHALL appear on the stderr of the process that ran `flz`,
  and SHALL NOT appear only in the controller's own log

#### Scenario: The rendering is not buried under an exit trace

- **WHEN** a `flz` verb refuses
- **THEN** the caller's stderr SHALL carry the rendering and no language-level exit
  banner, and no second line from `flz`

#### Scenario: The text form names the code and the next command

- **WHEN** `flz namespace delete ghost` is run and `ghost` does not exist
- **THEN** stderr SHALL carry `error: ` followed by a sentence naming `ghost` and ending
  `(namespace.not_found)`, then `hint: flz namespace list`, and the exit code SHALL be 5

#### Scenario: The JSON form is one parseable object

- **WHEN** `flz namespace delete ghost --json` is run and `ghost` does not exist
- **THEN** stderr SHALL carry exactly one line that parses as JSON, whose `error.code` is
  `namespace.not_found`, `error.class` is `not_found`, `error.details.namespace` is `ghost`
  and `error.hint` is `flz namespace list`, and stdout SHALL be empty

#### Scenario: A refusal per class exits with its code

- **WHEN** a verb is refused once for each class: a malformed command line, a token-less
  gated verb under enforcement, an unknown namespace, a duplicate schedule name, a quota
  reached, a controller that is not running, a verb whose controller answers not ready, and
  an uncatalogued reason injected in a test
- **THEN** they SHALL exit 2, 4, 5, 6, 6, 3, 7 and 1 in that order

### Requirement: `flz namespace list` SHALL answer from the namespace catalog

`flz namespace list` SHALL answer from the durable namespace catalog, the same store `flz namespace create`, `delete`, `show` and `set-quota` write, and SHALL report for each namespace whether a hub for it is running on this controller. A namespace created a moment earlier SHALL therefore appear, whether or not anything is running in it.

The verb's declared row SHALL be unchanged: the same verb name, the same action and the same scope it binds today. Only the data the body reads changes, so no user gains or loses the ability to run it.

A caller that is not an admin in the system namespace SHALL see only the namespace it is bound to, through the same filter the other catalog-reading verbs use.

The command's help text SHALL describe what it now answers, rather than naming running namespaces only.

#### Scenario: A created namespace is listed

- **WHEN** a user runs `flz namespace create billing` and then `flz namespace list`
- **THEN** `billing` SHALL appear in the output

#### Scenario: The output says what is running

- **GIVEN** a catalog holding two namespaces, one of which has a live hub on this controller
- **WHEN** a user runs `flz namespace list`
- **THEN** both SHALL appear, and the output SHALL distinguish the one with a live hub from the one without

#### Scenario: A namespace-scoped caller sees its own namespace only

- **GIVEN** a caller bound to `team-orders` and a catalog holding several namespaces
- **WHEN** it runs `flz namespace list`
- **THEN** the output SHALL name `team-orders` and no other namespace

#### Scenario: The verb's authorization is unchanged

- **WHEN** the declared verb row for `flz namespace list` is read after this change
- **THEN** its verb name, action and scope SHALL be the ones it bound before, so a caller authorized for it before is authorized for it after

### Requirement: Every option SHALL be spelled in kebab-case with at most a one-letter short spelling

Every option a `flz` verb row declares SHALL have a long spelling of two dashes followed by
lowercase words joined by single dashes, and SHALL have either no short spelling (the row
repeats the long spelling) or a short spelling of one dash and one letter. An option spelled
with an underscore, and a short spelling of two or more letters, SHALL NOT be declared. A
two-letter short spelling reads as two one-letter flags run together, and an underscore in one
long spelling among dashed ones is a spelling the user has to remember as the exception.

The rule SHALL be enforced by a CLI test that walks every option every verb row declares,
so that an option added with a non-conforming spelling fails the test rather than reaching a
user.

#### Scenario: The initial values option is kebab-case

- **WHEN** `flz relay start echo --init-values '{"count": 1}'` or
  `flz schedule add n --cron "* * * * *" --tz UTC --factory f --init-values '{}'` is run
- **THEN** the request SHALL carry the initial values, and `--init_values` and `-iv` SHALL
  NOT be read as that option

#### Scenario: A two-letter short spelling fails the test

- **WHEN** a verb row declares an option whose short spelling is `-si`
- **THEN** the CLI test SHALL fail naming the verb and the spelling

#### Scenario: An underscore in a long spelling fails the test

- **WHEN** a verb row declares an option whose long spelling contains `_`
- **THEN** the CLI test SHALL fail naming the verb and the spelling

#### Scenario: An option may have no short spelling

- **WHEN** `flz relay rewind my_loop --to-iteration 3` or
  `flz relay start echo --partitions 4` is run
- **THEN** the option SHALL be read, and neither option SHALL have a short spelling

#### Scenario: `-i` names the id on start

- **WHEN** `flz relay start echo -i my_echo` is run
- **THEN** the request SHALL carry the id `my_echo`, identically to `--id my_echo`

### Requirement: Every verb SHALL accept the output format

`--json` and `--output <text|json>` SHALL be options every `flz` verb carries, declared once
beside `--token` and `--help`, so that no verb refuses them as unknown. On every verb that
reaches a controller, they SHALL select the form of the answer, whether the verb reads or
acts: under `--json` a success SHALL be printed on stdout as the JSON the controller's door
answers for the same operation, the same JSON an SDK or HTTP caller receives. On every verb,
they SHALL select the form of a failure. When `flz` refuses the command line itself, it SHALL
honour a `--json` or `--output json` written anywhere in it.

#### Scenario: An acting verb takes --json

- **WHEN** `flz relay start echo --json` is run against a controller whose namespace
  quota is reached
- **THEN** the verb SHALL NOT refuse `--json` as an unknown option, and stderr SHALL carry the
  JSON error with code `quota.exceeded`

#### Scenario: An acting verb's success is JSON

- **WHEN** `flz relay start echo --json` succeeds
- **THEN** stdout SHALL carry one JSON value naming the started relay's id, and the exit
  code SHALL be 0

#### Scenario: A listing that had no JSON form has one

- **WHEN** `flz dlq list --json` is run
- **THEN** stdout SHALL carry the parked entries as one JSON array, and an empty queue SHALL
  print `[]`

#### Scenario: A command-line refusal honours --json

- **WHEN** `flz relay start echo --nope --json` is run
- **THEN** stderr SHALL carry the JSON error with code `usage.unknown_option`, details naming
  `--nope`, and the exit code SHALL be 2

### Requirement: `flz relay call` SHALL print the answer and exit by the failure's class

`flz relay call <relay_id>` SHALL read `--text` / `-t` (required, the input's payload),
`--channel` / `-c` (the entry channel, optional), `--answer-channel` (the channel whose output
answers, optional, no short spelling), `--key` (a partition group's key, optional), `--timeout`
(the deadline in milliseconds, no short spelling, default 30 000) and `--namespace`, declared in
its verb row like every verb's. It SHALL call the relay through the published
`Flezha.Service.Relay.call/6`, print the answer's payload on stdout followed by one newline,
and exit 0. A failure SHALL be rendered on stderr in the error model of `give-every-error-a-code`
and SHALL exit with the code of its class: `call.timeout` 7, a denied act 4,
`relay.not_found` 5, `call.target_unkeyed` 2. Its help screen SHALL be pinned by a capture.

#### Scenario: An answer is printed

- **WHEN** `flz relay call echo --text hello` is run against a relay that emits its input on an output
- **THEN** stdout SHALL be `hello` and one newline, stderr SHALL be empty, and the exit code SHALL be 0

#### Scenario: The answer channel picks the answer

- **WHEN** `flz relay call quoter --text EUR/USD --answer-channel quotes` is run against a relay that emits on `audit` and then on `quotes`
- **THEN** stdout SHALL be the `quotes` output and one newline

#### Scenario: A timeout exits 7

- **WHEN** `flz relay call slow --text x --timeout 100` is run against a relay whose turn takes longer
- **THEN** stderr SHALL carry `call.timeout`, stdout SHALL be empty, and the exit code SHALL be 7

#### Scenario: The timeout has no short spelling

- **WHEN** `flz relay call echo -t hello` is run
- **THEN** `-t` SHALL be read as `--text`, and the deadline SHALL be the default

### Requirement: `flz relay send` and `flz relay call` SHALL reach a partition group's member by key

`flz relay send` SHALL read `--key`, and both verbs SHALL resolve a partition group id and a key
to the member whose key-group range owns the key, by the one resolution `flz state get` uses. A
group named with no key SHALL fail naming the group's members, and SHALL NOT answer that the
relay was not found.

#### Scenario: `send` to a group reaches the owner

- **WHEN** `flz relay send prices --key EURUSD --channel trades --text '{"pair":"EURUSD"}'` is run against a group of two members
- **THEN** the member owning `EURUSD` SHALL receive the input, and the verb SHALL report the delivery

#### Scenario: `send` to a group without a key names the members

- **WHEN** `flz relay send prices --text x` is run against a partition group
- **THEN** it SHALL fail naming `prices#p0` and `prices#p1`, and SHALL NOT print `relay 'prices' not found`

### Requirement: `flz relay start` SHALL take no option that changes the class's behaviour

`flz relay start` SHALL take `--id`, `--partitions`, `--init-values`, the first
message (`--text` with `--channel`), the plugins attached at start (`--plugins`), the
namespace and the caller's credential, and the output format, and no other option: each
names what makes an instance this instance, never how its class behaves.
`--snapshot-interval` SHALL not exist. The help capture of the verb SHALL be re-pinned.

#### Scenario: The retired option is unknown

- **WHEN** `flz relay start echo --snapshot-interval 5000` runs
- **THEN** the CLI SHALL refuse the option as unknown on stderr and exit non-zero, and the pinned help capture SHALL list no such option

#### Scenario: A start names how it runs nowhere

- **WHEN** `flz relay start --help` is read
- **THEN** its help SHALL say that how the relay runs is the factory's, announced by its runner, and SHALL list no option that sets it

### Requirement: `flz relay info` SHALL show the relay's API

`flz relay info <id>` SHALL print an `API` section listing each entry channel of the
relay's factory with its format, whether it answers, its description, its input schema on one
line, and one example `flz relay call` command, and SHALL say when connected runners announce
different APIs or a channel's tool name is taken. With `--json` the section SHALL be the entries
as data.

#### Scenario: Info shows how to call

- **WHEN** `flz relay info orders-eu` is run for an `orders` relay whose `place` channel is
  described
- **THEN** the output holds an `API` section naming `place`, `json → answer`, its description, its
  schema and a line `flz relay call orders-eu --channel place --arg pair=... --arg amount=...`

### Requirement: `flz relay call` SHALL take typed arguments

`flz relay call <id> --channel <ch>` SHALL accept repeated `--arg name=value`, read the
channel's schema once through the door, coerce each value by its property type (`string`,
`number`, `integer`, `boolean`) and call `Relay.call_channel`. It SHALL refuse with
`api.input_invalid`, naming `--text` in its hint, `--arg` on a channel that is not `json`, has no
schema, or names a property typed `object` or `array`, or a value that does not coerce. A name
the schema does not declare SHALL be kept as one field of that name, never nested, and is
refused when the schema forbids additional properties. `--key` SHALL reach the partition member
it owns, and a partition group's API SHALL be its members' factory's. `--text` SHALL send its
payload as the value unchanged, also to a relay whose runners announced no API, and a channel
without an answer SHALL print `accepted`.

#### Scenario: Numbers are numbers

- **WHEN** `flz relay call orders-eu --channel place --arg pair=EUR/USD --arg amount=1000` is run
- **THEN** the relay receives `{"pair":"EUR/USD","amount":1000}` with `amount` a number

#### Scenario: A value that does not coerce

- **WHEN** `--arg amount=lots` is given for a number property
- **THEN** the command exits with the class's code and prints `api.input_invalid` naming `amount`

