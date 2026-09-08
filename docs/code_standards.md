# Code standards

The rules a change to this repository is held to, in every language it carries:
Elixir, Rust, Java, and the Node, Python, Ruby and PHP SDKs.

They are written here rather than measured by a tool. There used to be a script
that ratcheted them against a recorded snapshot, but the snapshot lived in an
untracked directory on one laptop, so on any other checkout the instrument could
only ever be green by being ignored, and for weeks it was. A rule a person or a
model can read and apply is worth more than a check nobody runs. What the tool
also gave up with it is drift: a sentence can say "no file over 800 lines", and
only a recorded snapshot could have said "this area had 7 near-duplicate blocks
and now has 13". That trade was made deliberately.

Some of these rules do have gates, and where one exists it is named below. A
gate is the enforcement; this page is the rule.

## Ceilings

- **No source file over 800 lines.** Counted as written, including comments and
  the module docstring. A file at the ceiling is a file that has taken on a
  second job, and splitting it on the state it mutates is almost always the cut.
- **No function over 50 lines.** A macro body counts as a function; so does a
  builder method and a `match` arm block long enough to be one.
- **No new dependency cycle.** `mix xref graph --format cycles` is the reading
  for Elixir. A change may not add a module to an existing cycle either: a cycle
  that grows is a cycle that is getting harder to cut, and the count alone will
  not say so.

A change that would cross a ceiling either does not land in that shape, or it
lands with the cut that keeps it under. There is no allowance for going over by
four lines because the alternative is inconvenient.

## One job lives in one place

A cross-cutting job is decided once, in one seam, and every caller submits to
it. Judging whether a topology is one the runtime can honour, authorizing an
operator verb, choosing a remedy for an unhealthy operative: each of these is
one job. When the same job is decided in six builders, adding one rule means
editing six files, and the sixth is the one that gets missed and ships a class
that registers on one SDK and dies at boot on another.

The practical test is not "is this code duplicated" but "if the rule changed,
how many places would have to agree". If the answer is more than one, the job
has scattered and the seam is what the change should restore.

The one standing exception is the Elixir SDK, which is its own core and inherits
nothing from `flz_core`. It re-expresses a shared check against the same
fixture rather than calling it, which is a second seam and not a bypass of the
first. See `docs/architecture/runner.md`, "One change, every SDK".

## Writing a check

Four rules about the checks themselves. Each was learned by shipping the
opposite.

**A reason describes a property of the thing, never a change that owes it a
fix.** An exemption justified by "the change that will give this a sink has not
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

## Less code, not more

The goal is a smaller system. Prefer, in this order: use what exists, extend or
parameterise it, refactor two things into one shared core, and only then write
something new. A new module beside a near-identical old one is a defect, not a
delivery.

Production lines added and removed are read from `git diff --numstat`, split by
hand into production and test paths. There is no tool that does the split.
