package com.flezha.sdk.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flezha.sdk.internal.RunnerHandle;
import javax.annotation.Nonnull;

/**
 * Administering the namespace this runner's connection proved, from ordinary
 * application code.
 *
 * <p>No procedure has to be running and the application holds no credential of
 * its own: the controller judges every call as the identity the connection
 * already established, so nothing an unattended process holds expires. An
 * operator grants a namespace's applications the authority once and revokes it
 * the same way, and a revocation takes effect on the next call:
 *
 * <pre>{@code flz role bind runner@team-orders team-orders app}</pre>
 *
 * <p>Every method here is one line over a single native entry point, because
 * the verb travels as data. Adding a verb costs a method and no JNI.
 */
public final class Admin {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final RunnerHandle handle;

    public Admin(RunnerHandle handle) {
        this.handle = handle;
    }

    /** Start an operative. Answers the controller's own result. */
    @Nonnull
    public JsonNode startOperative(OperativeSpec spec) {
        return call("Operative.start", spec.fields());
    }

    /** Stop an operative, or the whole partition group it names. */
    @Nonnull
    public JsonNode stopOperative(String operativeId) {
        return call("Operative.stop", operativeId);
    }

    /**
     * Inject a message into an operative's input stream and block for the
     * delivery verdict. {@code channel} may be empty for none.
     */
    @Nonnull
    public JsonNode send(String operativeId, String message, String channel) {
        return call("Operative.send", operativeId, message, channel);
    }

    /** Enable a plugin, built from the fields {@code flz plugin enable} takes. */
    @Nonnull
    public JsonNode enablePlugin(String pluginId, JsonNode fields) {
        return call("PluginManager.enable", pluginId, fields);
    }

    /** Disable a plugin, or every partitioned instance of it. */
    @Nonnull
    public JsonNode disablePlugin(String pluginId) {
        return call("PluginManager.disable", pluginId);
    }

    /** The operative ids running in this namespace. */
    @Nonnull
    public JsonNode listOperatives() {
        return call("Operative.list_operatives");
    }

    /** The plugins enabled on the controller node answering this call. */
    @Nonnull
    public JsonNode listPlugins() {
        return call("PluginManager.list");
    }

    /** One entity's live state, and the change sequence it was read at. */
    @Nonnull
    public JsonNode entityState(String operativeId, String entityKey) {
        return call("Operative.entity_state", operativeId, entityKey);
    }

    /**
     * Any published function, by name, for a verb this surface has not yet
     * grown a method for. What succeeds follows from the role the namespace is
     * bound to, never from a list kept here.
     *
     * @param args the call's arguments other than the namespace, which the
     *     controller fills from the connection. A {@link JsonNode} is sent as
     *     it is; anything else is sent as its text.
     */
    @Nonnull
    public JsonNode call(String verb, Object... args) {
        ArrayNode array = JsonNodeFactory.instance.arrayNode();
        for (Object arg : args) {
            array.add(arg instanceof JsonNode node ? node : JsonNodeFactory.instance.textNode(String.valueOf(arg)));
        }
        return read(handle.adminCall(verb, array.toString()));
    }

    private JsonNode read(String answer) {
        try {
            return JSON.readTree(answer);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("the controller answered unreadable JSON: " + e.getMessage(), e);
        }
    }
}
