package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.LogicalClock;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Registration-time refusals for the clock hierarchy and keyed channels.
 *
 * <p>Rust catches all of these at compile time (a {@code TimeExtractor<Input>}
 * bound, {@code BuildTopologyError::UnservableClock}, a keyed channel with no
 * plugin wire). Java's erasure precludes the compile-time form, so the same
 * defects are caught here — at registration, before any input is served and
 * before anything durable exists.
 */
class ClockAndKeyedValidationTest {

    @BeforeEach
    void reset() {
        StreamOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("EventTimeFrom without an eventTime override is refused")
    void eventTimeFromWithoutExtractorIsRefused() {
        assertThatThrownBy(() -> StreamOperativeClassRegistry.registerClass(NoExtractorOperative.class))
                .isInstanceOf(StreamOperativeConfigurationException.class)
                .hasMessageContaining("no_extractor")
                .hasMessageContaining("eventTime");
    }

    @Test
    @DisplayName("EventTimeFrom WITH an eventTime override registers")
    void eventTimeFromWithExtractorRegisters() {
        StreamOperativeClassRegistry.registerClass(ExtractorOperative.class);
        assertThat(StreamOperativeClassRegistry.getTopology("clock-extractor")).isNotNull();
    }

    @Test
    @DisplayName("LogicalClock with NoChange is refused as UnservableClock")
    void logicalClockWithNoChangeIsRefused() {
        assertThatThrownBy(() -> StreamOperativeClassRegistry.registerClass(UnservableClockOperative.class))
                .isInstanceOf(StreamOperativeConfigurationException.class)
                .hasMessageContaining("UnservableClock");
    }

    @Test
    @DisplayName("a keyed channel with no plugin wire is refused")
    void keyedChannelWithoutPluginWireIsRefused() {
        assertThatThrownBy(() -> StreamOperativeClassRegistry.registerClass(KeyedNoPluginOperative.class))
                .isInstanceOf(StreamOperativeConfigurationException.class)
                .hasMessageContaining("keyed")
                .hasMessageContaining("flz.key");
    }

    @Test
    @DisplayName("a keyed plugin-bound channel registers and is marked keyed on the descriptor")
    void keyedPluginChannelRegisters() {
        StreamOperativeClassRegistry.registerClass(KeyedPluginOperative.class);
        StreamOperativeTopology topology = StreamOperativeClassRegistry.getTopology("clock-keyed-plugin");
        assertThat(topology).isNotNull();
        ChannelDescriptor out = topology.channel(topology.lookupChannelId("out"));
        assertThat(out.keyed()).isTrue();
    }

    // --- fixtures -----------------------------------------------------------

    public static final class NoExtractorOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "clock-no-extractor";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(NoExtractorProc.class);
        }
    }

    public static final class ExtractorOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "clock-extractor";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(ExtractorProc.class);
        }
    }

    public static final class UnservableClockOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "clock-unservable";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "in").into(UnservableClockProc.class);
        }
    }

    public static final class KeyedNoPluginOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "clock-keyed-no-plugin";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sPlainProc = c.input(String.class, "in").into(PlainProc.class);
            sPlainProc.named("mid").keyed().into(SecondProc.class);
        }
    }

    public static final class KeyedPluginOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "clock-keyed-plugin";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sPlainProc = c.input(String.class, "in").into(PlainProc.class);
            sPlainProc.named("out").keyed().sink("sink");
        }
    }

    public record Unit() {}

    abstract static class Base<C extends Change, K extends com.flezha.sdk.state.Clock>
            implements Procedure<String, Unit, String, C, K, Void> {
        @Override
        public Unit initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }

        @Override
        public void run(String input, StateHandle<Unit, C> state, ProcedureCtx ctx) throws ProcedureException {}
    }

    /** Declares EventTimeFrom but never overrides eventTime — the refused case. */
    public static final class NoExtractorProc extends Base<NoChange, EventTimeFrom> {
        @Override
        public String name() {
            return "no_extractor";
        }
    }

    public static final class ExtractorProc extends Base<NoChange, EventTimeFrom> {
        @Override
        public String name() {
            return "with_extractor";
        }

        @Override
        public long eventTime(String input) {
            return Long.parseLong(input);
        }

        @Override
        public boolean eventTimeOverridden() {
            return true;
        }
    }

    /** LogicalClock + NoChange: there is no durable seq to serve. */
    public static final class UnservableClockProc extends Base<NoChange, LogicalClock> {
        @Override
        public String name() {
            return "unservable";
        }
    }

    public static class PlainProc extends Base<NoChange, WallClock> {
        @Override
        public String name() {
            return "plain_op";
        }
    }

    public static final class SecondProc extends PlainProc {
        @Override
        public String name() {
            return "second_op";
        }
    }
}
