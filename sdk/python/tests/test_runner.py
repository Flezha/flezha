"""The runner's lifecycle: what it claims, when, and what it refuses."""

from __future__ import annotations

import json
import logging
import select
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

import flezha
from flezha import logger
from flezha.errors import FlezhaError
from flezha.runner import Runner

from conftest import FakeCore, demo, work_activation


@flezha.procedure
def other(value: str) -> str:
    return value


@flezha.stream_operative("other")
def other_op(c: flezha.Channels) -> None:
    c.input("in").into(other).sink("out")


@pytest.fixture
def runner() -> Any:
    made: list[Runner] = []

    def build(namespace: str = "test") -> Runner:
        r = flezha.create_runner(namespace)
        made.append(r)
        return r

    yield build
    for r in made:
        try:
            r.stop()
            r.drain()
        except Exception:
            pass


def test_creating_a_runner_connects_nothing(runner: Any) -> None:
    r = runner("my_app")
    assert r.namespace == "my_app"
    assert r.operatives() == []


def test_start_declares_then_connects(runner: Any) -> None:
    core = FakeCore()
    r = runner("my_app").define_stream(demo).start(core=core)
    try:
        assert r is not None
        assert core.started == "my_app"
        assert len(core.declared) == 1
        assert json.loads(core.declared[0])["factory_id"] == "demo"
        assert r.operatives() == ["demo"]
    finally:
        r.stop()
        r.drain()


def test_run_is_start_plus_await_termination(runner: Any) -> None:
    core = FakeCore()
    r = runner()
    r.define_stream(demo)

    def stop_soon() -> None:
        while core.started is None:
            threading.Event().wait(0.01)
        r.stop()

    threading.Thread(target=stop_soon, daemon=True).start()
    r.run(core=core)
    assert core.shut_down


def test_zero_declared_types_is_legal(runner: Any) -> None:
    core = FakeCore()
    r = runner().start(core=core)
    try:
        assert core.declared == []
        assert core.started == "test"
    finally:
        r.stop()
        r.drain()


def test_a_second_start_on_the_same_runner_is_refused(runner: Any) -> None:
    core = FakeCore()
    r = runner().define_stream(demo).start(core=core)
    try:
        with pytest.raises(FlezhaError, match="already started"):
            r.start(core=FakeCore())
    finally:
        r.stop()
        r.drain()


def test_only_one_runner_may_be_started_per_process(runner: Any) -> None:
    first = runner().define_stream(demo).start(core=FakeCore())
    try:
        second = runner()
        with pytest.raises(FlezhaError, match="already started in this process"):
            second.start(core=FakeCore())
    finally:
        first.stop()
        first.drain()
    # The slot is released by the drain, so the next runner may take it.
    third = runner().start(core=FakeCore())
    third.stop()
    third.drain()


def test_a_declaration_that_fails_releases_the_slot(runner: Any) -> None:
    @flezha.stream_operative("broken")
    def broken(c: flezha.Channels) -> None:
        c.input("dangling")

    with pytest.raises(FlezhaError):
        runner().define_stream(broken).start(core=FakeCore())
    later = runner().start(core=FakeCore())
    later.stop()
    later.drain()


def test_a_post_start_define_reaches_the_core(runner: Any) -> None:
    core = FakeCore()
    r = runner().define_stream(demo).start(core=core)
    try:
        r.define_stream(other_op)
        assert [json.loads(body)["factory_id"] for body in core.declared] == ["demo", "other"]
        assert r.operatives() == ["demo", "other"]
    finally:
        r.stop()
        r.drain()


def test_removing_an_undeclared_type_is_refused_naming_it(runner: Any) -> None:
    r = runner().define_stream(demo)
    with pytest.raises(FlezhaError, match="absent"):
        r.remove_operative("absent")


def test_removing_a_type_with_live_instances_is_refused(runner: Any) -> None:
    from flezha.dispatch import dispatch_now

    core = FakeCore()
    r = runner().define_stream(demo).start(core=core)
    try:
        deps = r._executor._deps  # type: ignore[attr-defined]
        dispatch_now(work_activation(1, "abc"), core, deps)
        with pytest.raises(FlezhaError, match="live instances"):
            r.remove_operative("demo")
        assert r.operatives() == ["demo"]
    finally:
        r.stop()
        r.drain()


def test_removing_a_type_with_no_instances_succeeds(runner: Any) -> None:
    core = FakeCore()
    r = runner().define_stream(demo).define_stream(other_op).start(core=core)
    try:
        r.remove_operative("other")
        assert r.operatives() == ["demo"]
    finally:
        r.stop()
        r.drain()


def test_a_duplicate_factory_id_is_refused(runner: Any) -> None:
    r = runner().define_stream(demo)
    with pytest.raises(FlezhaError, match="duplicate factory id"):
        r.define_stream(demo)


def test_define_modules_imports_and_registers(runner: Any) -> None:
    core = FakeCore()
    r = runner().define_modules("declaring_module").start(core=core)
    try:
        assert r.operatives() == ["from_module"]
    finally:
        r.stop()
        r.drain()


def test_an_unimportable_module_names_it_and_chains_the_original(runner: Any) -> None:
    with pytest.raises(FlezhaError) as caught:
        runner().define_modules("no_such_module_anywhere")
    assert "no_such_module_anywhere" in str(caught.value)
    assert isinstance(caught.value.__cause__, ImportError)


def test_the_context_manager_drains_on_exit(runner: Any) -> None:
    core = FakeCore()
    with runner().define_stream(demo).start(core=core) as r:
        assert r.operatives() == ["demo"]
    assert core.shut_down
    assert core.freed


def test_the_startup_line_names_the_tier_the_executor_and_the_sizing(
    runner: Any, caplog: pytest.LogCaptureFixture
) -> None:
    core = FakeCore()
    with caplog.at_level(logging.INFO, logger="flezha.runner"):
        r = runner().define_stream(demo).start(core=core)
    try:
        line = caplog.text
        assert "tier" in line
        assert "executor" in line
        assert "base" in line and "ceiling" in line
    finally:
        r.stop()
        r.drain()


def test_the_executor_is_an_axis_of_the_run(monkeypatch: pytest.MonkeyPatch, runner: Any) -> None:
    monkeypatch.setenv("FLEZHA_PYTHON_EXECUTOR", "asyncio")
    core = FakeCore()
    r = runner().define_stream(demo).start(core=core)
    try:
        assert r._executor.name == "asyncio"  # type: ignore[attr-defined]
    finally:
        r.stop()
        r.drain()


def test_an_unknown_executor_is_refused(monkeypatch: pytest.MonkeyPatch, runner: Any) -> None:
    monkeypatch.setenv("FLEZHA_PYTHON_EXECUTOR", "goroutines")
    with pytest.raises(FlezhaError, match="unknown executor"):
        runner().define_stream(demo).start(core=FakeCore())


def test_a_controller_initiated_shutdown_ends_the_runners_own_wait(runner: Any) -> None:
    """Shutdown is observed three ways; this is the third.

    A worker that sees `shutting_down` from its next poll ends — but a runner
    that only watched its own stop would keep waiting for a termination nothing
    was going to signal.
    """
    core = FakeCore()
    r = runner().define_stream(demo)

    def close_from_the_controller() -> None:
        while core.started is None:
            threading.Event().wait(0.01)
        core.shut_down = True

    threading.Thread(target=close_from_the_controller, daemon=True).start()
    r.run(core=core)
    assert core.freed


def test_the_drain_frees_the_context(runner: Any) -> None:
    core = FakeCore()
    r = runner().define_stream(demo).start(core=core)
    r.stop()
    r.drain()
    assert core.shut_down
    assert core.freed


#: A runner as an orchestrator or the e2e harness runs one: started, told it is
#: up, then waiting to be stopped — with standard input a pipe, never a tty.
_SIGNALLED_RUNNER = """
import flezha
from conftest import FakeCore

runner = flezha.create_runner("signalled").start(core=FakeCore())
print("up", flush=True)
runner.await_termination()
"""


def test_a_signalled_runner_with_a_piped_stdin_exits_cleanly() -> None:
    """A stop by signal, with standard input a pipe that stays open.

    The watcher on standard input must hold nothing the interpreter's
    finalisation waits on: a daemon thread parked inside a buffered read owns
    the reader's lock, and finalisation aborts the process rather than take it.
    """
    child = subprocess.Popen(
        [sys.executable, "-c", _SIGNALLED_RUNNER],
        cwd=Path(__file__).parent,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert child.stdout is not None and child.stderr is not None
    try:
        ready, _, _ = select.select([child.stdout], [], [], 20)
        assert ready and child.stdout.readline() == "up\n", child.stderr.read()
        child.send_signal(signal.SIGTERM)
        # Standard input stays open until the exit: closing it would end the
        # watcher by end of file, which is not the path under test.
        child.wait(timeout=20)
        stderr = child.stderr.read()
    finally:
        if child.poll() is None:
            child.kill()
        for stream in (child.stdin, child.stdout, child.stderr):
            if stream is not None:
                stream.close()
        child.wait()
    assert child.returncode == 0, stderr


def test_a_quiet_library_installs_no_handler() -> None:
    root = logging.getLogger(logger.ROOT_LOGGER_NAME)
    assert all(isinstance(handler, logging.NullHandler) for handler in root.handlers)


def test_one_call_turns_diagnostics_on() -> None:
    root = logger.configure("info")
    try:
        assert root.level == logging.INFO
        assert any(not isinstance(h, logging.NullHandler) for h in root.handlers)
    finally:
        for handler in list(root.handlers):
            if not isinstance(handler, logging.NullHandler):
                root.removeHandler(handler)
        root.setLevel(logging.NOTSET)
