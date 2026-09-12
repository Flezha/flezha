package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.runtime.state.TimerOp;
import com.flezha.sdk.state.TimerKind;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The emit funnel: the Change and plugin output an invocation produces, the
 * identity stamps a keyed channel adds, the two keyed-emit refusals, the timer
 * ops that ride the effect, and the checkpoint mark.
 */
class EmitContextTest extends DispatchTestBase {

    @Test
    @DisplayName("an input applies its Change, ships decodable change bytes, and stages the plugin output")
    void workAppliesChangesAndCollectsOutputs() {
        StreamOperativeClassRegistry.registerClass(CounterOperative.class);
        ItemEffect effect =
                soleEffect(work("counter_op", 0, Optional.empty(), 7, input(0, "x", 1_000, OptionalLong.of(9), false)));

        assertThat(effect.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(effect.consumedOffset()).hasValue(9);
        assertThat(effect.eventTime()).isEqualTo(1_000);
        assertThat(effect.checkpoint()).isFalse();
        assertThat(effect.changes()).hasSize(1);
        CountChange change = new KryoCodec(List.of(CountChange.class))
                .deserialize(effect.changes().get(0), CountChange.class);
        assertThat(change.delta).isEqualTo(1);

        assertThat(effect.outputs()).hasSize(1);
        Output output = effect.outputs().get(0);
        assertThat(output.target()).isEqualTo(new OutputTarget.Plugin("out"));
        assertThat(output.key()).isEmpty();
        assertThat(output.metadata())
                .as("non-keyed channel carries no identity stamps")
                .isEmpty();
        assertThat(new String(output.payload(), StandardCharsets.UTF_8)).isEqualTo("seen:x");
    }

    @Test
    @DisplayName("the sink names the channel the plugin output is addressed by")
    void sinkNamesTheChannel() {
        StreamOperativeClassRegistry.registerClass(SinkNamesChannelOperative.class);
        ItemEffect effect = soleEffect(work("sink_names_channel", 0, Optional.empty(), 5, input(0, "v", 1_000)));

        assertThat(effect.outputs()).hasSize(1);
        assertThat(effect.outputs().get(0).target())
                .as("`.sink(\"items_output\")` with no `.named(...)` names the channel")
                .isEqualTo(new OutputTarget.Plugin("items_output"));
    }

    @Test
    @DisplayName("a keyed plugin emit carries flz.entity / flz.seq / flz.key and the schema key")
    void keyedEmitCarriesIdentityStamps() {
        StreamOperativeClassRegistry.registerClass(KeyedEmitOperative.class);
        ItemEffect effect = soleEffect(work("keyed_op", 0, Optional.empty(), 5, input(0, "v", 1_000)));

        assertThat(effect.outputs()).hasSize(1);
        Output output = effect.outputs().get(0);
        assertThat(output.key()).contains("k-v");
        Map<String, String> meta = metaMap(output);
        assertThat(meta)
                .containsEntry("flz.entity", OP + "/keyed_proc/")
                .containsEntry("flz.seq", "5.0")
                .containsEntry("flz.key", "k-v")
                .containsEntry("user", "meta");
    }

    @Test
    @DisplayName("a key-less emit in a keyed Entity carries that Entity's own key, stamped like emitKeyed")
    void keyLessEmitInAKeyedEntityCarriesTheEntityKey() {
        StreamOperativeClassRegistry.registerClass(KeyedEmitOperative.class);
        ItemEffect effect = soleEffect(work("keyed_op", 0, Optional.of("BTC/USD"), 5, input(0, "plain:v", 1_000)));

        assertThat(effect.verdict()).isInstanceOf(Verdict.Ok.class);
        assertThat(effect.outputs()).hasSize(1);
        Output output = effect.outputs().get(0);
        assertThat(output.key()).contains("BTC/USD");
        assertThat(metaMap(output))
                .containsEntry("flz.entity", OP + "/keyed_proc/BTC/USD")
                .containsEntry("flz.seq", "5.0")
                .containsEntry("flz.key", "BTC/USD");
    }

    @Test
    @DisplayName("a key-less emit on a keyed channel is refused where the Entity has no key")
    void keyLessEmitOnAKeyedChannelIsRefusedWithoutAnEntityKey() {
        StreamOperativeClassRegistry.registerClass(KeyedEmitOperative.class);
        ItemEffect effect = soleEffect(work("keyed_op", 0, Optional.empty(), 5, input(0, "plain:v", 1_000)));

        assertThat(effect.verdict()).isInstanceOf(Verdict.RetryableError.class);
        assertThat(((Verdict.RetryableError) effect.verdict()).message()).contains("single-entity");
        assertThat(effect.outputs()).as("nothing may be emitted").isEmpty();
    }

    @Test
    @DisplayName("emitKeyed on a non-keyed channel is refused, entity key or not")
    void keyedEmitOnANonKeyedChannelIsRefused() {
        StreamOperativeClassRegistry.registerClass(CounterOperative.class);
        ItemEffect effect = soleEffect(work("counter_op", 0, Optional.of("BTC/USD"), 7, input(0, "key:x", 1_000)));

        assertThat(effect.verdict()).isInstanceOf(Verdict.RetryableError.class);
        assertThat(((Verdict.RetryableError) effect.verdict()).message()).contains("not declared keyed");
        assertThat(effect.outputs()).as("nothing may be emitted").isEmpty();
    }

    @Test
    @DisplayName("registrations ride the effect; a firing's removal leads its ops; now() equals firedAt")
    void timerOpsRideTheEffect() {
        StreamOperativeClassRegistry.registerClass(TimerOperative.class);
        ItemEffect armed = soleEffect(work("timer_op", 0, Optional.empty(), 0, input(0, "1000", 1_000)));
        assertThat(armed.timers())
                .containsExactly(
                        TimerOp.register(TimerKind.EVENT_TIME, 1_010),
                        TimerOp.register(TimerKind.PROCESSING_TIME, 1_005));

        ItemEffect fired =
                soleEffect(work("timer_op", 0, Optional.empty(), 0, new WorkItem.Timer(TimerKind.EVENT_TIME, 1_010)));
        assertThat(fired.eventTime()).isEqualTo(1_010);
        assertThat(fired.timers().get(0)).isEqualTo(TimerOp.cancel(TimerKind.EVENT_TIME, 1_010));
        assertThat(fired.timers())
                .as("the chained timerAfter anchored on firedAt")
                .contains(TimerOp.register(TimerKind.PROCESSING_TIME, 1_017));
        assertThat(TIMER_EVENTS).containsExactly("EVENT_TIME@1010 now=1010");
    }

    @Test
    @DisplayName("an event-time timer under a wall clock is refused inside the invocation")
    void eventTimeTimerWithoutEventTimeClockIsRefused() {
        StreamOperativeClassRegistry.registerClass(WallTimerOperative.class);
        ItemEffect effect = soleEffect(work("wall_timer_op", 0, Optional.empty(), 0, input(0, "x", 1)));
        assertThat(effect.verdict()).isInstanceOf(Verdict.RetryableError.class);
        assertThat(((Verdict.RetryableError) effect.verdict()).message()).contains("event");
    }

    @Test
    @DisplayName("ctx.checkpoint() marks the effect; tier outputs carry no host-side identity stamps")
    void checkpointFlagAndTierOutputs() {
        StreamOperativeClassRegistry.registerClass(CheckpointOperative.class);
        ItemEffect open = soleEffect(work("ckpt_op", 0, Optional.empty(), 0, input(0, "accumulate", 1)));
        assertThat(open.checkpoint()).isFalse();
        assertThat(open.outputs()).hasSize(1);
        assertThat(metaMap(open.outputs().get(0)))
                .as("under the tier the controller derives the stamps at flush")
                .doesNotContainKeys("flz.entity", "flz.seq", "flz.key");
        assertThat(open.outputs().get(0).key()).contains("ck");

        ItemEffect closing = soleEffect(work("ckpt_op", 0, Optional.empty(), 0, input(0, "commit", 2)));
        assertThat(closing.checkpoint()).isTrue();
    }
}
