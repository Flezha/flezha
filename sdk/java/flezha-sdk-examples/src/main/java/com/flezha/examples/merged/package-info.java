/**
 * One procedure consuming two entry channels through a merged subscription.
 *
 * <p>A temperature detector reads sensor values on one channel and its alert threshold on another,
 * so the threshold can be changed while the operative is running rather than fixed at deployment.
 *
 * <p>A procedure declares a single input type, so two channels must be reconciled into one before
 * they can reach it. Java has no sum types, so
 * {@link com.flezha.examples.merged.DetectorInput} fills that role: one record with a slot per
 * source, exactly one populated on any given message, and a static factory per variant. Those
 * factories are the mappings supplied to {@code merge}, referenced as method handles such as
 * {@code DetectorInput::ofReading}, and they are the whole of the technique.
 *
 * <p>{@link com.flezha.examples.merged.ThermostatOperative} declares both sources against one
 * consumer. Either source could equally be another procedure's output stream rather than an entry
 * channel; {@code from} accepts both.
 *
 * <p>The configuration message is an ordinary input and is recorded in the same durable state as
 * the readings, so a restart resumes with the threshold that was in force rather than the deployed
 * default. {@link com.flezha.examples.merged.DetectorProcedure} initialises that threshold to
 * positive infinity, so an operative that starts before receiving its configuration alerts on
 * nothing rather than on everything.
 */
package com.flezha.examples.merged;
