# A Python runner exits 0 after SIGTERM, and the watcher holds no lock

`exit-cleanly-after-sigterm-in-python` (KI-125). One call in
`sdk/python/src/flezha/runner.py` `_watch_stdin_eof`, one regression test in
`sdk/python/tests/test_runner.py`, one ADDED requirement in `python-sdk`.

## The defect was a lock, not a thread

A daemon thread parked in `sys.stdin.buffer.read(1)` owns the
`_io.BufferedReader` lock for the whole blocking read. At finalisation the
interpreter deallocates `sys.stdin`; its `close` enters `_enter_buffered_busy`,
waits **one second** for that lock, and `Py_FatalError`s — SIGABRT, exit 134,
after every graceful stop of a runner whose stdin is a pipe (every runner an
orchestrator or the harness starts). The state ship was never late: the
snapshot landed 53–55 ms after the SIGTERM before and after the fix; the exit
was the defect, and the harness could not see it because it closes the port
rather than reading its exit status.

`os.read(sys.stdin.fileno(), 1)` takes no interpreter-level lock and releases
the GIL; at exit the thread is inside `read(2)` holding nothing finalisation
needs, and `exit_group` ends it. Same loop, same three outcomes.

## Why not the other two shapes the entry named

- **End the watcher before exit** — CPython cannot interrupt a thread inside
  `read(2)`: closing or `dup2`-ing over fd 0 from another thread does not wake a
  read already in the kernel. It would be a self-pipe plus a `select` loop with
  a poll interval, a second mechanism beside the read. Ruby kills its watcher in
  teardown because the Ruby VM *can* interrupt a blocking region.
- **`os._exit(0)` after the drain** — skips every `atexit` handler and the
  stdio flush on every stop path, for a hazard only signal-with-open-stdin has;
  and `run()` is a library call that returns to its caller.

## The regression test must not close stdin

`Popen.communicate` closes the child's stdin first, which turns a signalled
stop into an EOF-driven one — the watcher returns, the lock is released, and
the test is green on the defective code. The test `wait`s with stdin open and
reads stderr only after the exit. The settle is the child's `up` line under
`select` with a bound; no sleep. Its red at HEAD is `assert -6 == 0` with the
`_enter_buffered_busy` line in stderr.

## The e2e evidence, and how to see an exit status

`NodeTestHelper.start_runner_port(ns, port_opts: [:exit_status])` is the
harness's own seam; `{port, {:exit_status, n}}` then arrives when the process
ends. A scratch copy of `state_freshness_e2e_test.exs` with that option read
`134 after 2195 ms` at HEAD and `0 after 53 ms` after; node `0 after 53 ms`
and ruby `0 after 54 ms`, unchanged. PHP has no watcher thread (SIGTERM and
EOF are polled between polls). Nothing is owed to another SDK.

## What ruff and pyright say that is not this change

`ruff check` (not the gate; the gate is `ruff format`) reports `I001`,
`RUF022`, `PYI034` on `runner.py` and `I001`, `S110`, `BLE001` on
`test_runner.py` — the same six at HEAD. `pyright` as CI runs it (`src/flezha`)
is clean; on `tests/test_runner.py` it reports two pre-existing
`reportOptionalMemberAccess` errors in `test_start_declares_then_connects`,
also at HEAD.
