# The Ruby suite's test toolchain is declared, and the pin is the half that matters

**Provenance, and a gap worth knowing.** This change has **no archived OpenSpec
change directory** — `openspec/changes/archive/` contains no
`pin-ruby-test-dependency`, no unarchived directory exists, and the id appears
inside no archived file. The board records it DONE without the ", archived" its
neighbours carry, so it was executed as a direct fix and never materialised as an
OpenSpec change. **The commit `ebec40aa` and its diff are the entire surviving
design record**, and this note is reconstructed from them. Nothing here is a
measurement this note took.

## The defect

`runner_test.rb` calls `Process.stub`, which is `Object#stub` from
`minitest/mock`. **Nothing required `minitest/mock`, `minitest` was declared
nowhere, and there is no lockfile.** The suite was green only because this box's
minitest 5.25.4 has `autorun` load `mock`. CI reported the other resolution on
`main` as `undefined method 'stub' for module Process`. Nothing was version-gated
in our favour, so the same resolution reaches `dev` by the same route.

## The recorded diagnosis had it backwards, and the correction is the point

**minitest 6.0 ships no `mock.rb` at all** — mock is extracted to its own gem —
so the `require` alone cannot fix it: under 6.0.6 the suite stops at
`cannot load such file -- minitest/mock`. **The pin is what makes the suite run**;
the `require` is what makes a failure name a library instead of a missing method
on a module, and what keeps it correct on any 5.x whose `autorun` drops the line.

## Proved on the arm where the ambient convenience is ABSENT

A green run proves nothing here — **it was already green**. With `autorun`
replaced by minitest 6's shape, the suite passes with the `require` and gives
three `NoMethodError: undefined method 'stub' for module Process` without it.
With this box's real `autorun` it passes either way, **which is exactly why the
recorded green run was not evidence**.

## `rake` and `async` were undeclared too, and `async` stays a DEVELOPMENT dependency

The fiber executor requires `async` lazily behind a guard that raises a message
naming the gem, so no consumer is broken — but the suite drives the fiber arm and
is red without it. **Promoting it to a runtime dependency would force it on every
threads consumer and contradict the deliberate lazy require.**

CI installed all four unpinned, and a pin nothing reads stops nothing. The
workflow now resolves each version by grepping the gemspec — the shape
`format.yml` already uses for rubocop, so there is one convention rather than two.

## The instrument taxed the hygiene the project mandates

`refactor_metrics.py` counted `flezha.gemspec` as **Ruby production code**, while
`language()` returns `None` for `package.json`, `pyproject.toml`,
`composer.json`, `pom.xml` and `Cargo.toml`. **Declaring a dependency therefore
cost a blocking production line in Ruby and nothing in the other five SDKs.**
`gemspec` is dropped from the extension table; `Rakefile` still maps to Ruby by
basename. `sdk/ruby prod_loc` reads 1 921 against a baseline of 1 955, to be
re-recorded.

## Verification

Six naming checks, the gate self-test, metrics and the full formatter run all
exit 0. Ruby suite 82 runs, 277 assertions, 0 failures **on both arms**.
