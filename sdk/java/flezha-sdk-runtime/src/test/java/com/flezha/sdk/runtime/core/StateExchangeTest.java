package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * State across the host boundary: the restore fold and its migration decision,
 * the snapshot and live-state reads, ApplyState / MigrationSerialize, and the
 * Stateless marker.
 */
class StateExchangeTest extends DispatchTestBase {

    @Test
    @DisplayName("a RestoreFold body tagged with an older version goes to the migration, not the codec")
    void restoreFoldMigratesTaggedBodies() {
        StreamOperativeClassRegistry.registerClass(MigratingCounterOperative.class);
        ActivationDispatcher dispatcher = new ActivationDispatcher(id -> session);

        ActivationOutcome restored = dispatcher
                .dispatch(command(
                        "migrating_counter_op",
                        0,
                        Optional.empty(),
                        new ActivationKind.RestoreFold(
                                Optional.of(new ActivationKind.RestoreBody(
                                        "5".getBytes(StandardCharsets.UTF_8), OptionalInt.of(1))),
                                List.of(new ActivationKind.RestoreBody(
                                        "3".getBytes(StandardCharsets.UTF_8), OptionalInt.of(1))))))
                .outcome();
        assertThat(restored).isInstanceOf(ActivationOutcome.Restored.class);

        ActivationOutcome json = dispatcher
                .dispatch(command("migrating_counter_op", 0, Optional.empty(), new ActivationKind.LiveStateRead()))
                .outcome();
        assertThat(new String(((ActivationOutcome.StateJson) json).json(), StandardCharsets.UTF_8))
                .as("snapshot migrated to 5, then the migrated change folded +3")
                .isEqualTo("{\"n\":8}");
    }

    @Test
    @DisplayName("a Procedure declaring no migration refuses an older body rather than decoding it")
    void restoreFoldWithoutAMigrationRefuses() {
        StreamOperativeClassRegistry.registerClass(CounterOperative.class);
        ActivationDispatcher dispatcher = new ActivationDispatcher(id -> session);

        ActivationOutcome outcome = dispatcher
                .dispatch(command(
                        "counter_op",
                        0,
                        Optional.empty(),
                        new ActivationKind.RestoreFold(
                                Optional.of(new ActivationKind.RestoreBody(
                                        "5".getBytes(StandardCharsets.UTF_8), OptionalInt.of(1))),
                                List.of())))
                .outcome();

        assertThat(outcome).isInstanceOf(ActivationOutcome.Failed.class);
        assertThat(((ActivationOutcome.Failed) outcome).error().message())
                .as("the failure names the procedure and the version it was asked for")
                .contains("count_proc")
                .contains("no state migration")
                .contains("1");
    }

    @Test
    @DisplayName("RestoreFold rebuilds snapshot + changes; SnapshotRequest and LiveStateRead read the result")
    void restoreSnapshotAndLiveRead() {
        StreamOperativeClassRegistry.registerClass(CounterOperative.class);
        ItemEffect first = soleEffect(work("counter_op", 0, Optional.empty(), 0, input(0, "a", 1)));
        soleEffect(work("counter_op", 0, Optional.empty(), 1, input(0, "b", 2)));

        ActivationOutcome json =
                outcome(command("counter_op", 0, Optional.empty(), new ActivationKind.LiveStateRead()));
        assertThat(new String(((ActivationOutcome.StateJson) json).json(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":2}");

        ActivationOutcome snap =
                outcome(command("counter_op", 0, Optional.empty(), new ActivationKind.SnapshotRequest()));
        byte[] snapshot = ((ActivationOutcome.Snapshot) snap).state();

        // A fresh dispatcher (a rebind) folds snapshot(n=2) + one change → 3.
        ActivationDispatcher rebound = new ActivationDispatcher(id -> session);
        ActivationOutcome restored = rebound.dispatch(command(
                        "counter_op",
                        0,
                        Optional.empty(),
                        new ActivationKind.RestoreFold(
                                Optional.of(new ActivationKind.RestoreBody(snapshot, OptionalInt.empty())),
                                first.changes().stream()
                                        .map(c -> new ActivationKind.RestoreBody(c, OptionalInt.empty()))
                                        .toList())))
                .outcome();
        assertThat(restored).isInstanceOf(ActivationOutcome.Restored.class);
        ActivationOutcome reboundJson = rebound.dispatch(
                        command("counter_op", 0, Optional.empty(), new ActivationKind.LiveStateRead()))
                .outcome();
        assertThat(new String(((ActivationOutcome.StateJson) reboundJson).json(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":3}");
    }

    @Test
    @DisplayName("ApplyState replaces the state from its JSON form; MigrationSerialize reads it back")
    void applyStateRoundTrips() {
        StreamOperativeClassRegistry.registerClass(CounterOperative.class);
        ActivationOutcome applied = outcome(command(
                "counter_op",
                0,
                Optional.empty(),
                new ActivationKind.ApplyState("{\"n\":42}".getBytes(StandardCharsets.UTF_8))));
        assertThat(applied).isInstanceOf(ActivationOutcome.StateApplied.class);

        ActivationOutcome migrated =
                outcome(command("counter_op", 0, Optional.empty(), new ActivationKind.MigrationSerialize()));
        assertThat(new String(((ActivationOutcome.MigrationState) migrated).json(), StandardCharsets.UTF_8))
                .isEqualTo("{\"n\":42}");
    }

    // A stateless procedure has nothing to ship, and Jackson refuses an empty
    // bean: the marker is what the migration serialize and the graceful
    // shutdown's state ship carry for a stateless Java operative.
    @Test
    @DisplayName("a Stateless state serializes as an empty object and reads back")
    void statelessStateShipsAsEmptyObject() {
        StreamOperativeClassRegistry.registerClass(StatelessStateOperative.class);

        ActivationOutcome migrated =
                outcome(command("stateless_state_op", 0, Optional.empty(), new ActivationKind.MigrationSerialize()));
        byte[] json = ((ActivationOutcome.MigrationState) migrated).json();
        assertThat(new String(json, StandardCharsets.UTF_8)).isEqualTo("{}");

        ActivationOutcome read =
                outcome(command("stateless_state_op", 0, Optional.empty(), new ActivationKind.LiveStateRead()));
        assertThat(new String(((ActivationOutcome.StateJson) read).json(), StandardCharsets.UTF_8))
                .isEqualTo("{}");

        assertThat(outcome(command("stateless_state_op", 0, Optional.empty(), new ActivationKind.ApplyState(json))))
                .isInstanceOf(ActivationOutcome.StateApplied.class);
    }
}
