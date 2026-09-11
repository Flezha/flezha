"""Administering the namespace this runner's connection proved.

No procedure has to be running and the application holds no credential of its
own: the controller judges every call as the identity the connection already
established, so nothing an unattended process holds expires. An operator grants
a namespace's applications the authority once and revokes it the same way, and
a revocation takes effect on the next call::

    flz role bind runner@team-orders app team-orders

Every method here is one line over a single native entry point, because the
verb travels as data. Adding a verb costs a method and no native work.
"""

from __future__ import annotations

import json
from typing import Any

from .native import Core

__all__ = ["Admin"]


class Admin:
    """The administration surface for the namespace a runner proved."""

    __slots__ = ("_core",)

    def __init__(self, core: Core) -> None:
        self._core = core

    def start_operative(
        self,
        operative_id: str,
        factory_id: str,
        *,
        kind: str | None = None,
        init_values: Any = None,
        opts: Any = None,
    ) -> Any:
        """Start an operative, on the specification ``flz operative start`` takes."""
        fields: dict[str, Any] = {"id": operative_id, "factory_id": factory_id}
        if kind is not None:
            fields["kind"] = kind
        if init_values is not None:
            fields["init_values"] = init_values
        if opts is not None:
            fields["opts"] = opts
        return self.call("Operative.start", fields)

    def stop_operative(self, operative_id: str) -> Any:
        """Stop an operative, or the whole partition group it names."""
        return self.call("Operative.stop", operative_id)

    def send(self, operative_id: str, message: str, channel: str = "") -> Any:
        """Inject a message into an operative's input stream, blocking for the verdict."""
        return self.call("Operative.send", operative_id, message, channel)

    def enable_plugin(self, plugin_id: str, fields: dict[str, Any]) -> Any:
        """Enable a plugin, built from the fields ``flz plugin enable`` takes."""
        return self.call("PluginManager.enable", plugin_id, fields)

    def disable_plugin(self, plugin_id: str) -> Any:
        """Disable a plugin, or every partitioned instance of it."""
        return self.call("PluginManager.disable", plugin_id)

    def list_operatives(self) -> Any:
        """The operative ids running in this namespace."""
        return self.call("Operative.list_operatives")

    def list_plugins(self) -> Any:
        """The plugins enabled on the controller node answering this call."""
        return self.call("PluginManager.list")

    def entity_state(self, operative_id: str, entity_key: str) -> Any:
        """One entity's live state, and the change sequence it was read at."""
        return self.call("Operative.entity_state", operative_id, entity_key)

    def call(self, verb: str, *args: Any) -> Any:
        """Any published function, by name.

        What succeeds follows from the role the namespace is bound to, never
        from a list kept here. ``args`` are the call's arguments other than the
        namespace, which the controller fills from the connection.
        """
        return json.loads(self._core.admin_call(verb, json.dumps(list(args))))
