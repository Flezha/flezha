/**
 * Annotations supplying declarations the type signature cannot express.
 *
 * <p>The runtime recovers most of what it needs from a procedure's or operative's {@code implements}
 * clause, reading the type arguments as {@code Class} handles. These annotations cover the
 * remainder: facts that are not expressible as a type argument, or that Java's erasure removes
 * before the runtime can observe them.
 *
 * <p>{@link com.flezha.sdk.authoring.EntityKey} marks the member that identifies an entity.
 * {@link com.flezha.sdk.authoring.EntitiesFor} declares which entities an input is routed to where
 * that is not derivable from the wiring. {@link com.flezha.sdk.authoring.InitState} marks the
 * method that constructs the initial per-entity state, and
 * {@link com.flezha.sdk.authoring.Journal} marks the type recorded in the change log.
 * {@link com.flezha.sdk.authoring.Procedure}, {@link com.flezha.sdk.authoring.State} and
 * {@link com.flezha.sdk.authoring.StreamOperative} identify the authoring roles a class plays, and
 * share their names with the interfaces of the same names in other packages.
 *
 * <p>Reach for one of these only when the compiler cannot infer the fact on its own. An annotation
 * that duplicates something already visible in the type signature adds a second place for the same
 * declaration to be wrong.
 */
@javax.annotation.ParametersAreNonnullByDefault
package com.flezha.sdk.authoring;
