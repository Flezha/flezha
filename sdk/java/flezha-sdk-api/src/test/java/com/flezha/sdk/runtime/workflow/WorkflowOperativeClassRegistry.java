package com.flezha.sdk.runtime.workflow;

import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;

/**
 * Test double for the runtime's workflow class registry, standing at the name
 * {@code FlezhaRunner} resolves it by. The statics are the stream double's,
 * inherited, because {@code RuntimeBridge} finds a public static method on a
 * superclass by name as readily as on the class itself.
 */
public final class WorkflowOperativeClassRegistry extends StreamOperativeClassRegistry {

    private WorkflowOperativeClassRegistry() {}
}
