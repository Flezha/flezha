# Brief: reproduce a row

You are reproducing one row before anyone writes code for it. **You change no
code.** Read only, plus whatever you must run to see the defect.

Read the row itself (`wip/WORK.md`, and its entry in `docs/known-bugs.md` if it
has one) and the code it names. Do not read the rulebooks end to end.

The row was written against an older tree. Rows go stale: on 2026-09-06, seven
of twelve candidates had already been fixed by later work. So the first question
is not "how do I fix this", it is **"is this still true today"**.

Do this:

1. Read the code the row names, at HEAD. Is the mechanism it describes still
   there, unchanged?
2. Show it. Point at the file and line where the mechanism lives at HEAD, and
   where it helps, drive the product by hand (the CLI, a private controller from
   `wip/tools/env.sh`, an IEx expression) and quote the real output. Never run a
   test, a test file, a gate row or a suite: tests run only at the section
   checkpoint (`wip/INVARIANTS.md`, "Tests run at three checkpoints").
3. Time box: <30 minutes>. If it will not reproduce in that time, that is a
   result, not a failure.
4. Say whether the fix is local or needs a decision: a behaviour or public
   surface change, a record that is wrong in some window, a rule every SDK must
   share. A decision is found in the first ten minutes, not after the code.

Report, in this shape and nothing longer:

- **Verdict**: reproduces / does not reproduce / cannot tell, and why.
- **Evidence**: the exact commands and the output they really gave.
- **Mechanism**: the function and the line where the defect actually lives, if
  it reproduces. Not a fix, not a plan.
- **Local or decision**: which, and why, in one line.
- **Blast radius**: every other site with the same shape, found by grep, so the
  sweep is known before the fix starts.
- **What a regression test would have to do** to be red today. One or two lines.

Do not fix it. Do not write a proposal. Do not touch the boards.

## Running one row ahead

Usually you reproduce the NEXT row while another row is being coded in the same
tree, so your reading is off the critical path. Then the tree is someone else's:

- Create, change and delete nothing in the tree. Read, and run only commands
  that leave no file behind in it (the CLI, grep, git log); never a test.
- Write any new regression test as a draft under `wip/repro/<row-id>/`, which
  git ignores and which outlives the session, and give the exact path it belongs
  at in the tree. The supervisor places it when the row becomes current; it
  runs red at the section checkpoint.
- Run nothing that loads the machine (a release build, a full suite, a load
  test) while the row in flight is measuring performance. The supervisor says so
  in your brief when it is.
