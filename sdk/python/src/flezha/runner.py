"""The runner: what an author starts, and everything it owns while running.

Classes are registered explicitly, by naming them or the modules that declare
them. There is no autoload, no path walk and no package scan: what runs is what
the author said runs, and a class that is defined but not registered never
reaches the core.

Startup order::

    declare classes -> (fork, if the operator forks at all) -> start

The core must NOT be started before a fork: it owns native threads that
``fork(2)`` does not carry, and :meth:`Core.guard <flezha.native.Core.guard>`
raises naming that rule rather than letting a child serve nothing.
"""

from __future__ import annotations

import importlib
import json
import os
import signal
import sys
import threading
from types import TracebackType
from typing import Any

from . import determinism, graph
from .ctx import WorkflowCtx
from .diagnostics import logger
from .dispatch import DispatchDeps, Registry
from .errors import FlezhaError
from .executor import Executor, build as build_executor
from .native import ACTIVATION_SCHEMA_VERSION, Core, build_tier
from .operative import Operative

__all__ = ["Runner", "create_runner", "ACTIVATION_SCHEMA_VERSION"]

_log = logger("runner")

#: Only one runner may be *started* per process: a runner is a transport
#: connection and a connection lives in one process. Any number of unstarted
#: runners may exist.
_started_slot = threading.Lock()


class Runner:
    """One runner: its namespace, its declared types and its executor."""

    def __init__(self, namespace: str) -> None:
        self._namespace = namespace
        self._operatives: list[Operative] = []
        self._registry = Registry()
        self._core: Core | None = None
        self._executor: Executor | None = None
        self._executor_kind = os.environ.get("FLEZHA_PYTHON_EXECUTOR", "thread")
        self._executor_options: dict[str, Any] = {}
        self._started = False
        self._stopped = threading.Event()
        self._teardown: list[Any] = []
        self._holds_slot = False

    # -- declaration -------------------------------------------------------

    @property
    def namespace(self) -> str:
        return self._namespace

    def operatives(self) -> list[str]:
        """The factory ids this runner currently declares."""
        return self._registry.ids() if self._started else [o.factory_id for o in self._operatives]

    def define_stream(self, operative: Operative) -> Runner:
        """Register a stream operative type.

        Legal both before and after :meth:`start`; a post-start declaration
        reaches the core immediately and the controller at the next bind.
        """
        return self._define(operative)

    def define_workflow(self, operative: Operative) -> Runner:
        """Register a workflow operative type.

        The same registration as a stream, and deliberately the same code: what
        a class declares is read from the class, so the two names are for the
        reader rather than for the runner.
        """
        return self._define(operative)

    def define_modules(self, *modules: str) -> Runner:
        """Import the named modules and register everything they declare.

        No implicit package scanning and no filesystem discovery: the modules
        are named, and one that cannot be imported says so with the original
        error chained.
        """
        for name in modules:
            try:
                module = importlib.import_module(name)
            except ImportError as error:
                raise FlezhaError(
                    f"the module `{name}` names operative declarations but could not be "
                    f"imported: {error}"
                ) from error
            for value in vars(module).values():
                if isinstance(value, Operative):
                    self._define(value)
        return self

    def remove_operative(self, factory_id: str) -> Runner:
        """Undeclare a type. Refuses one with live instances, and one never declared."""
        if not any(o.factory_id == factory_id for o in self._operatives):
            raise FlezhaError(f"no operative type `{factory_id}` is declared on this runner")
        if self._started:
            registered = self._registry.get(factory_id)
            if registered is not None and registered.has_entities():
                raise FlezhaError(
                    f"`{factory_id}` has live instances on this runner; stop them before "
                    f"removing the type"
                )
            if registered is not None:
                self._registry.remove(factory_id)
            # The core keeps its own declaration until the context ends: the
            # ABI has no entry point that retracts one, so this runner stops
            # serving the type and the controller learns of the removal at the
            # next bind, whose init frame is serialized from the core's set.
            _log.debug(
                "the removal of `%s` has no core surface; this runner stops serving it and "
                "the declaration lapses when the context ends",
                factory_id,
            )
        self._operatives = [o for o in self._operatives if o.factory_id != factory_id]
        return self

    def executor(self, kind: str, **options: Any) -> Runner:
        """Which executor dispatches: ``thread`` (the default) or ``asyncio``."""
        self._executor_kind = kind
        self._executor_options = options
        return self

    # -- lifecycle ---------------------------------------------------------

    def start(self, *, core: Core | None = None) -> Runner:
        """Declare, connect and start serving.

        Everything up to and including the first poll, and nothing that blocks.
        Separate from :meth:`run` so a host with its own supervision tree can
        own the waiting, and so the startup order is something a test can
        observe rather than a comment.
        """
        if self._started:
            raise FlezhaError("this runner is already started")
        if not _started_slot.acquire(blocking=False):
            raise FlezhaError(
                "a runner is already started in this process; a runner is one transport "
                "connection, so N processes are N runners"
            )
        self._holds_slot = True
        try:
            # Before anything else: a native library built against a different
            # activation schema is refused here, with no controller connection
            # attempted.
            self._core = core or Core.create()
            determinism.install()
            for operative in self._operatives:
                self._declare(operative)
            self._core.start(self._namespace)
            self._observe_shutdown()

            deps = DispatchDeps(
                registry=self._registry,
                bridge=self._core,
                workflow_ctx=lambda instance_id: WorkflowCtx(instance_id, self._core),
            )
            self._executor = build_executor(
                self._executor_kind,
                self._core,
                deps,
                on_core_shutdown=self.stop,
                **self._executor_options,
            )
            self._executor.start()
            self._started = True
        except BaseException:
            self._release_slot()
            raise

        _log.info(
            "runner up on namespace %s: %s tier, %s",
            self._namespace,
            build_tier(),
            self._executor.describe(),
        )
        return self

    def await_termination(self) -> Runner:
        """Block until the runner is stopped, then drain."""
        self._stopped.wait()
        self.drain()
        return self

    def run(self, *, core: Core | None = None) -> Runner:
        """Declare, connect and serve until shutdown.

        Sugar for :meth:`start` followed by :meth:`await_termination`, never an
        independent implementation.
        """
        return self.start(core=core).await_termination()

    def stop(self) -> None:
        """Stop fetching and drain what is in flight.

        Reached from a signal handler, the standard-input watcher and the
        executor's report of a controller-initiated close, so it is idempotent
        — which `Event.set` already is.
        """
        self._stopped.set()

    def drain(self) -> None:
        """Stop fetching, drain what is in flight, and release the core."""
        if self._core is None:
            return
        _log.info("draining")
        self._core.shutdown()
        undrained = self._executor.stop() if self._executor else 0
        for undo in self._teardown:
            undo()
        self._teardown.clear()
        # The context is released only when nothing is still running against
        # it. A worker the bounded drain gave up on is inside user code and may
        # yet call the core; freeing the context under it would be a
        # use-after-free in native code. The process is on its way out either
        # way, so the kernel reclaims what this skips.
        if undrained == 0:
            self._core.free()
        self._core = None
        self._executor = None
        self._started = False
        self._release_slot()

    def __enter__(self) -> Runner:
        """Start, unless the caller already did.

        A runner started explicitly — which is how a host injects a core, or
        sequences its own boot — still drains on the way out of a ``with``.
        """
        return self if self._started else self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()
        self.drain()

    # -- internals ---------------------------------------------------------

    def _release_slot(self) -> None:
        if self._holds_slot:
            self._holds_slot = False
            _started_slot.release()

    def _define(self, operative: Operative) -> Runner:
        if any(o.factory_id == operative.factory_id for o in self._operatives):
            raise FlezhaError(
                f"duplicate factory id `{operative.factory_id}`: a runner declares each once"
            )
        self._operatives.append(operative)
        if self._started:
            # Declaring on a live core appends to the set its connect frame is
            # serialized from, which is the resync of record. An FFI host ships
            # no `UpdateFactories` frame of its own: the ABI has no entry point
            # for one, so the controller learns the new type at the next bind.
            # The in-process Rust host, which owns the runner channel directly,
            # is the only one that can notify a live controller — OWED to
            # `sdk-abi-factory-resync`, and Java carries the same gap.
            self._declare(operative)
        return self

    def _declare(self, operative: Operative) -> None:
        spec = operative.spec
        graph.validate(spec)
        self._registry.add(spec)
        assert self._core is not None
        self._core.declare_class(json.dumps(graph.to_host_class_decl(spec)).encode())

    def _observe_shutdown(self) -> None:
        """Shutdown is observed three ways, because the harness, an operator and
        the controller each have their own: end of file on standard input, a
        signal, and the core's own stop — which every worker sees as
        ``shutting_down`` from its next poll."""
        if threading.current_thread() is threading.main_thread():
            for name in ("SIGINT", "SIGTERM"):
                sig = getattr(signal, name, None)
                if sig is None:
                    continue
                previous = signal.signal(sig, lambda *_: self.stop())
                self._teardown.append(lambda s=sig, p=previous: signal.signal(s, p))
        if not sys.stdin.isatty():
            self._watch_stdin_eof()

    def _watch_stdin_eof(self) -> None:
        """Exit on end of file on standard input.

        The e2e harness manages a runner as an Erlang Port and stops it by
        closing the port — which is this process's standard input. A read
        *error* is not end of file, and conflating the two is how a runner shuts
        itself down moments after connecting; only a genuine EOF, or a
        descriptor that has gone away, ends the watch.
        """

        def watch() -> None:
            stream = getattr(sys.stdin, "buffer", sys.stdin)
            while True:
                try:
                    if stream.read(1) == b"":
                        break
                except (BlockingIOError, InterruptedError):
                    continue
                except (ValueError, OSError):
                    break
            _log.info("standard input reached end of file; draining")
            self.stop()

        thread = threading.Thread(target=watch, name="flezha-stdin", daemon=True)
        thread.start()


def create_runner(namespace: str) -> Runner:
    """Start here: ``flezha.create_runner("default").define_stream(op).run()``.

    Creating a runner connects nothing, claims no slot and spawns no thread.
    """
    return Runner(namespace)
