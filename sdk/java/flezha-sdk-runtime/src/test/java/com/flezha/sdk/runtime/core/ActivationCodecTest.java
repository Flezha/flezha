package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.state.TimerOp;
import com.flezha.sdk.state.TimerKind;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Cross-language byte pin for activation schema version 2: the same values
 * the Rust fixture suite ({@code flz_core/src/schema.rs}) encodes must
 * produce the same bytes here. A mismatch means the schema changed — bump
 * {@code SCHEMA_VERSION} on both sides.
 */
class ActivationCodecTest {

    private static Activation workActivation() {
        return new Activation(
                42,
                new EntityRef("op-1", 3, Optional.of("k")),
                new ActivationKind.Work(List.of(
                        new WorkItem.Input(
                                1,
                                new byte[] {(byte) 0xAA, (byte) 0xBB},
                                List.of(Map.entry("flz.offset", "7")),
                                OptionalLong.of(7),
                                true,
                                1_000),
                        new WorkItem.Timer(TimerKind.EVENT_TIME, 2_000))),
                9,
                "f");
    }

    private static ActivationResult resultFixture() {
        return new ActivationResult(
                42,
                new ActivationOutcome.Work(List.of(new ItemEffect(
                        new Verdict.Ok(),
                        List.of(new byte[] {0x01}),
                        List.of(new Output(
                                new OutputTarget.Plugin("out"),
                                Optional.of("k2"),
                                OptionalLong.empty(),
                                List.of(),
                                new byte[] {0x02})),
                        OptionalLong.of(7),
                        1_000,
                        List.of(TimerOp.cancel(TimerKind.PROCESSING_TIME, 2_000)),
                        true,
                        Optional.empty()))));
    }

    @Test
    @DisplayName("work activation bytes are pinned to the Rust fixture")
    void workActivationBytesArePinned() {
        byte[] encoded = ActivationCodec.encodeActivation(workActivation());
        assertThat(hex(encoded))
                .isEqualTo("2a046f702d310301016b0002000102aabb010a666c7a2e6f66667365740137010701e8070100d00f090166");
        assertThat(ActivationCodec.decodeActivation(encoded))
                .usingRecursiveComparison()
                .isEqualTo(workActivation());
    }

    @Test
    @DisplayName("result bytes are pinned to the Rust fixture")
    void resultBytesArePinned() {
        byte[] encoded = ActivationCodec.encodeResult(resultFixture());
        assertThat(hex(encoded)).isEqualTo("2a0001000101010101036f757401026b32000001020107e8070101d00f000100");
        assertThat(ActivationCodec.decodeResult(encoded))
                .usingRecursiveComparison()
                .isEqualTo(resultFixture());
    }

    @Test
    @DisplayName("command activation kinds are pinned and round-trip")
    void commandKindBytesArePinned() {
        List<Map.Entry<ActivationKind, String>> kinds = List.of(
                Map.entry(
                        new ActivationKind.RestoreFold(
                                Optional.of(new byte[] {0x0F}), List.of(new byte[] {0x01}, new byte[] {0x02})),
                        "0101010f0201010102000166"),
                Map.entry(new ActivationKind.SnapshotRequest(), "02000166"),
                Map.entry(new ActivationKind.LiveStateRead(), "03000166"),
                Map.entry(new ActivationKind.ApplyState("{}".getBytes()), "04027b7d000166"),
                Map.entry(new ActivationKind.MigrationSerialize(), "05000166"),
                Map.entry(
                        new ActivationKind.KeyRequest(2, List.of(new byte[] {0x70}), true, false),
                        "06020101700100000166"),
                Map.entry(new ActivationKind.BoundedRun("go".getBytes()), "0702676f000166"),
                Map.entry(new ActivationKind.CompensateRun(List.of(new byte[] {0x0A})), "0801010a000166"));
        for (Map.Entry<ActivationKind, String> entry : kinds) {
            Activation act = new Activation(1, new EntityRef("o", 0, Optional.empty()), entry.getKey(), 0, "f");
            byte[] bytes = ActivationCodec.encodeActivation(act);
            // Envelope prefix (token 1, target o/0/None) is 5 bytes.
            assertThat(hex(bytes).substring(0, 10)).isEqualTo("01016f0000");
            assertThat(hex(bytes).substring(10)).isEqualTo(entry.getValue());
            assertThat(ActivationCodec.decodeActivation(bytes))
                    .usingRecursiveComparison()
                    .isEqualTo(act);
        }
    }

    @Test
    @DisplayName("every outcome variant round-trips through encode/decode")
    void outcomeVariantsRoundTrip() {
        List<ActivationOutcome> outcomes = List.of(
                new ActivationOutcome.Restored(),
                new ActivationOutcome.Snapshot(new byte[] {0x05}),
                new ActivationOutcome.StateJson("{\"a\":1}".getBytes()),
                new ActivationOutcome.StateApplied(),
                new ActivationOutcome.MigrationState("{}".getBytes()),
                new ActivationOutcome.Prepared(List.of(
                        new PrepareResult.Ok(new PrepareAnswer(List.of("k1", "k2"), OptionalLong.of(77))),
                        new PrepareResult.Err(new WorkError("bad key", true)))),
                new ActivationOutcome.Failed(new WorkError("boom", false)),
                new ActivationOutcome.BoundedDone(true, new byte[] {0x09}),
                new ActivationOutcome.Work(List.of(new ItemEffect(
                        new Verdict.RetryableError("later"),
                        List.of(),
                        List.of(new Output(
                                new OutputTarget.Edge(3, 4),
                                Optional.empty(),
                                OptionalLong.of(500),
                                List.of(Map.entry("m", "v")),
                                new byte[] {0x01})),
                        OptionalLong.empty(),
                        0,
                        List.of(TimerOp.register(TimerKind.EVENT_TIME, 9_000)),
                        false,
                        Optional.of(new DlqEntry("ch", "poison", 3, Optional.of(new byte[] {0x0C})))))),
                new ActivationOutcome.Work(List.of(new ItemEffect(
                        new Verdict.NonRetryableError("never"),
                        List.of(new byte[] {0x02}),
                        List.of(),
                        OptionalLong.of(1),
                        2,
                        List.of(),
                        false,
                        Optional.empty()))));
        for (ActivationOutcome outcome : outcomes) {
            ActivationResult result = new ActivationResult(7, outcome);
            byte[] bytes = ActivationCodec.encodeResult(result);
            ActivationResult decoded = ActivationCodec.decodeResult(bytes);
            assertThat(decoded).usingRecursiveComparison().isEqualTo(result);
            assertThat(ActivationCodec.encodeResult(decoded)).isEqualTo(bytes);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
