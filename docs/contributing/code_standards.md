# Code standards

The rules a change to this repository is held to, in every language it carries:
Elixir, Rust, Java, and the Node, Python, Ruby and PHP SDKs.

Most of them are written here to be read and applied, not measured by a tool.
Some have gates, and where one exists it is named below. A gate is the
enforcement; this page is the rule. [The formatters](#the-formatters) and
[the gates](#the-gates) at the end say what CI checks and how to run the same
checks yourself.

## Ceilings

- **No source file over 800 lines.** Counted as written, including comments and
  the module docstring. A file at the ceiling is a file that has taken on a
  second job, and splitting it on the state it mutates is almost always the cut.
- **No function over 50 lines.** A macro body counts as a function; so does a
  builder method and a `match` arm block long enough to be one.
- **No Rust function over cognitive complexity 25.** The gate row
  `rust-clippy` (`cargo clippy --workspace --all-targets`) is the reading; the
  threshold is in `clippy.toml`.
- **No new dependency cycle.** `mix xref graph --format cycles` is the reading
  for Elixir. A change may not add a module to an existing cycle either: a cycle
  that grows is a cycle that is getting harder to cut, and the count alone will
  not say so.

The gate row `ceilings` (`scripts/check_ceilings.py`) is the reading for the
other three: every production file and function, and the number of cycles and
the files in the largest. What the tree held when the row landed is recorded in
`scripts/ceilings_baseline.txt`, which may only shrink: a breaker outside it or
larger than its entry fails the row, and so does an entry the tree no longer
holds at its recorded size, so the commit that shrinks a breaker lowers its line.

A change that would cross a ceiling either does not land in that shape, or it
lands with the cut that keeps it under. There is no allowance for going over by
four lines because the alternative is inconvenient.

## One job lives in one place

A cross-cutting job is decided once, in one seam, and every caller submits to
it. Judging whether a topology is one the runtime can honour, authorizing an
administration verb, choosing a remedy for an unhealthy relay: each of these is
one job. When the same job is decided in six builders, adding one rule means
editing six files, and the sixth is the one that gets missed and ships a class
that registers on one SDK and dies at boot on another.

The practical test is not "is this code duplicated" but "if the rule changed,
how many places would have to agree". If the answer is more than one, the job
has scattered and the seam is what the change should restore.

## Writing a check

Four rules about the checks themselves. Each was learned by shipping the
opposite.

**A reason describes a property of the thing, never a change that owes it a
fix.** An exemption justified by "the change that will give this an output has not
landed yet" is stale the moment that change lands, and nothing notices: the list
still parses, the gate still passes, and the reason is now false. Write what is
true about the entry instead, so that the day it stops being true is the day
somebody has to edit it.

**A replacement check may not be weaker than the one it retires, in any shape.**
Not weaker in what it measures, not weaker in where it looks, not weaker by
moving from a failure to a warning. Retiring a check and putting a smaller one
in its place is how coverage is lost while every gate stays green. If the
replacement genuinely cannot cover something the old one did, say so where the
old check was, and say what is now unguarded.

**An exceptions list is for debt that predates the check, never a place to park
code written the same day.** The list is a record of what was already there when
the rule arrived, and it only ever shrinks. A rule that grew an exception for
every change that found it inconvenient is not a rule, and a list of 110
exceptions is the proof.

**A check that measures zero sites cannot fail, and must say so rather than
pass.** A scan whose pattern matches nothing, a walk whose root does not exist,
a gate whose file list is empty: each reports success, and each is measuring
nothing at all. Zero sites is a result to report, not a green run.

## How a test waits

A test waits on a condition, never on the clock. `Process.sleep` in front of an
assertion is not a settle, it is a guess about how fast the machine is, and it
stops being true the day the machine is busy.

**Poll for the state the assertion needs, with a deadline and a message naming
what never happened.** There is one polling loop in the suite for this,
`FlezhaTest.Helper.NodeTestHelper.wait_until/3`. Reuse it rather than writing a
second one. A sleep inside that loop is the poll interval and is correct; a
sleep in a test body is the smell.

**A fixed delay in front of a negative assertion can pass without testing
anything.** "The watermark did not advance in 200 ms" is also satisfied by a
system that had not started yet, so the test cannot tell the guarantee it claims
to prove from a slow boot. Either establish the precondition with a positive
signal first, or sample the claim across the whole window and stop on the event
that ends it.

**When a claim rests on two reads being ordered, read the value that can change
first.** Reading the watermark before the store, then treating a still-empty
store as proof that the sample predates the park, is race-free in the direction
that matters. The reverse order leaves a window, however small.

**`Process.sleep(:infinity)` in a fixture is not a wait.** A process that must
never answer is how the wedge and dead-handle paths are tested, and it is the
subject rather than a delay.

Benchmarks are the one exception. A fixed settle window before a measurement is
correct in `lib/mix/tasks/bench/`, because a condition-bound wait would measure
a slightly different system on every run and the numbers would stop being
comparable.

No gate counts sleeps, and a count would not help: every sleep left in the suite
is a poll interval or a fixture, and a counter cannot tell either of those from
a clock-bound wait. This rule is held in review.

## Less code, not more

The goal is a smaller system. Prefer, in this order: use what exists, extend or
parameterise it, refactor two things into one shared core, and only then write
something new. A new module beside a near-identical old one is a defect, not a
delivery.

Production lines added and removed are read from `git diff --numstat`, split by
hand into production and test paths. What counts as production is defined in
`scripts/production_code.py`, which `python3 scripts/production_code.py` prints
per repository.

## The formatters

Each language has one formatter, and CI fails a pull request whose code it would
change:

```sh
$ scripts/format-apply.sh --check    # report what is unformatted, change nothing
$ scripts/format-apply.sh            # format it (also `make fmt`)
```

Rust is formatted by a pinned nightly `rustfmt`, configured in `rustfmt.toml`,
and Elixir by `mix format`, configured in `.formatter.exs`. The check covers the
whole Rust workspace; formatting touches only the Rust files your tree changed
against `HEAD` or added, so it never rewrites a file someone else is editing. The script names the
nightly it uses. Each SDK other than Rust and Elixir is formatted in its own
repository. A formatter that is not installed drops its language from the
report and nothing else. The commits that only reformatted code are listed in
`.git-blame-ignore-revs`, so `git blame` skips them.

## The gates

Every check CI runs is a gate, the test suites included, and every gate is a
row in `scripts/gate_rows.py`. That list is the one place a gate is added: each
workflow's check steps select rows by group, so a new row reaches CI with no
other edit, and a workflow cannot spell a check of its own.
`scripts/naming_gate.py` runs the rows, so what you run is what CI runs:

```sh
$ python3 scripts/naming_gate.py --list            # every gate and its command
$ python3 scripts/naming_gate.py --list @core      # the rows one group selects
$ python3 scripts/naming_gate.py --run @naming     # the naming and structure gates
$ python3 scripts/naming_gate.py --run @format     # formatters, license headers, notices
$ python3 scripts/naming_gate.py --run <id>        # one gate
```

A row may run in a directory of its own, under extra environment and a time
limit, and may be declared quiet: a quiet row that passes but prints a warning,
an uncaptured `[error]` log line or a stray stderr line is a finding, reported
as NOISY. `--logs DIR` writes each row's output to `DIR/<id>.log` and its
verdict and duration to `DIR/summary.txt`.

The runner reports every gate and ends on one of three non-zero statuses when
something is wrong:

| Status | Means |
|---|---|
| 1 | A gate found something in the source |
| 2 | A tool a gate needs is not installed |
| 3 | A tool ran and did not finish, or a gate reached no verdict |

Only 1 says anything about your code. On 2 and 3 the gate has checked nothing,
so neither is a pass. CI fails on all three.

One gate needs a word of its own. `check-layer-direction.sh` holds the
controller to its dependency order: a file under `lib/flezha/` may not name a
module whose file lives in a higher layer. A module's layer is the directory its
file is in, not its name, so moving a file into the directory that owns its
feature is how an entry is fixed. `scripts/layer-direction-baseline.txt` lists
what the tree held when the gate was added, and the list may only shrink. A new
directory under `lib/flezha/` has to be ranked in the script before code goes
into it, or the gate exits 3.
