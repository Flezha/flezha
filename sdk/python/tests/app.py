"""Where this package's conformance test app is.

The app declares the fixture classes ``conformance.json`` names for each
capability, and the suites here run those same ones rather than a second set
declared beside them: a class that drifts from the one end-to-end drives would
prove nothing about the SDK under test.

Two layouts, found rather than counted: ``test_app/`` beside the package once
this package has a repository of its own, and ``test/apps/python_runner`` in
the enclosing tree while it sits inside the core's.
"""

from __future__ import annotations

import functools
import importlib
import json
import pathlib
import sys
import types
from typing import Any

from contract import PACKAGE_ROOT, ascend_to

#: The member every candidate directory is recognised by.
MANIFEST = "conformance.json"

#: Where the app sits in the core's tree, relative to any ancestor.
IN_TREE = "test/apps/python_runner"


@functools.cache
def root() -> pathlib.Path:
    """The test app's directory."""
    beside = PACKAGE_ROOT / "test_app"
    if (beside / MANIFEST).is_file():
        return beside
    tree = ascend_to(PACKAGE_ROOT, f"{IN_TREE}/{MANIFEST}")
    if tree is not None:
        return tree / IN_TREE
    raise FileNotFoundError(
        f"no conformance test app beside {PACKAGE_ROOT} (test_app/) or in any tree "
        f"above it ({IN_TREE}); these suites run the classes it declares"
    )


def module(name: str) -> types.ModuleType:
    """One module of the test app, imported.

    The app is not a package and is not installed, so it is importable only
    after its directory is on the path.
    """
    located = str(root())
    if located not in sys.path:
        sys.path.insert(0, located)
    return importlib.import_module(name)


@functools.cache
def declared_fixtures() -> dict[str, list[Any]]:
    """The capability-to-fixture-class table ``conformance.json`` declares."""
    return json.loads((root() / MANIFEST).read_text())["fixtures"]
