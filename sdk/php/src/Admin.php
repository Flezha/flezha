<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Ffi\Core;

/**
 * Administering the namespace this runner's connection proved, from ordinary
 * application code.
 *
 * No procedure has to be running and the application holds no credential of
 * its own: the controller judges every call as the identity the connection
 * already established, so nothing an unattended process holds expires. An
 * operator grants a namespace's applications the authority once and revokes it
 * the same way, and a revocation takes effect on the next call:
 *
 *     flz role bind runner@team-orders app team-orders
 *
 * Every method here is one line over a single native entry point, because the
 * verb travels as data. Adding a verb costs a method and no native work.
 */
final class Admin
{
    public function __construct(private readonly Core $core)
    {
    }

    /** Start an operative, on the specification `flz operative start` takes. */
    public function startOperative(
        string $operativeId,
        string $factoryId,
        ?string $kind = null,
        mixed $initValues = null,
        mixed $opts = null,
    ): mixed {
        $fields = ['id' => $operativeId, 'factory_id' => $factoryId];
        if ($kind !== null) {
            $fields['kind'] = $kind;
        }
        if ($initValues !== null) {
            $fields['init_values'] = $initValues;
        }
        if ($opts !== null) {
            $fields['opts'] = $opts;
        }

        return $this->call('Operative.start', $fields);
    }

    /** Stop an operative, or the whole partition group it names. */
    public function stopOperative(string $operativeId): mixed
    {
        return $this->call('Operative.stop', $operativeId);
    }

    /** Inject a message into an operative's input stream, blocking for the verdict. */
    public function send(string $operativeId, string $message, string $channel = ''): mixed
    {
        return $this->call('Operative.send', $operativeId, $message, $channel);
    }

    /** Enable a plugin, built from the fields `flz plugin enable` takes. */
    public function enablePlugin(string $pluginId, array $fields): mixed
    {
        return $this->call('PluginManager.enable', $pluginId, $fields);
    }

    /** Disable a plugin, or every partitioned instance of it. */
    public function disablePlugin(string $pluginId): mixed
    {
        return $this->call('PluginManager.disable', $pluginId);
    }

    /** The operative ids running in this namespace. */
    public function listOperatives(): mixed
    {
        return $this->call('Operative.list_operatives');
    }

    /** The plugins enabled on the controller node answering this call. */
    public function listPlugins(): mixed
    {
        return $this->call('PluginManager.list');
    }

    /** One entity's live state, and the change sequence it was read at. */
    public function entityState(string $operativeId, string $entityKey): mixed
    {
        return $this->call('Operative.entity_state', $operativeId, $entityKey);
    }

    /**
     * Any published function, by name. What succeeds follows from the role the
     * namespace is bound to, never from a list kept here. The arguments are
     * the call's own other than the namespace, which the controller fills from
     * the connection.
     */
    public function call(string $verb, mixed ...$args): mixed
    {
        $answer = $this->core->adminCall($verb, json_encode(array_values($args), JSON_THROW_ON_ERROR));

        return json_decode($answer, true, 512, JSON_THROW_ON_ERROR);
    }
}
