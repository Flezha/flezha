# Brief: reproduce a row

You are reproducing one row before anyone writes code for it. **You change no
code.** Read only, plus whatever you must run to see the defect.

Read `wip/INVARIANTS.md`, `wip/RULES.md`, `CLAUDE.md`, and the row itself
(`wip/WORK.md`, and its entry in `docs/known-bugs.md` if it has one).

The row was written against an older tree. Rows go stale: on 2026-09-06, seven
of twelve candidates had already been fixed by later work. So the first question
is not "how do I fix this", it is **"is this still true today"**.

Do this:

1. Read the code the row names, at HEAD. Is the mechanism it describes still
   there, unchanged?
2. Run something. A command with real output, not an argument from reading. If
   the row already carries a command, run that one first.
3. Time box: <30 minutes>. If it will not reproduce in that time, that is a
   result, not a failure.

Report, in this shape and nothing longer:

- **Verdict**: reproduces / does not reproduce / cannot tell, and why.
- **Evidence**: the exact commands and the output they really gave.
- **Mechanism**: the function and the line where the defect actually lives, if
  it reproduces. Not a fix, not a plan.
- **Blast radius**: every other site with the same shape, found by grep, so the
  sweep is known before the fix starts.
- **What a regression test would have to do** to be red today. One or two lines.

Do not fix it. Do not write a proposal. Do not touch the boards.

## Running one row ahead

Usually you reproduce the NEXT row while another row is being coded in the same
tree, so your reading is off the critical path. Then the tree is someone else's:

- Create, change and delete nothing in the tree. Read, and run only commands
  that leave no file behind in it (existing tests, the CLI, grep, git log).
- Write any new regression test as a draft under `wip/repro/<row-id>/`, which
  git ignores and which outlives the session, and give the exact path it belongs
  at in the tree. The supervisor places it and runs it red when the row becomes
  current.
- Run nothing that loads the machine (a release build, a full suite, a load
  test) while the row in flight is measuring performance. The supervisor says so
  in your brief when it is.
