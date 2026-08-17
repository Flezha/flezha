package com.flezha.sdk.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.flezha.sdk.IdleObserver;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The Java half of {@code expose-entry-input-idle-signal}: the episode latch,
 * the derived tick, the wall-clock mapping and observer containment.
 *
 * <p>The delivery stamp itself lives in the shared core — the Java SDK is a
 * poll host over it, so there is one delivery path and it is driven end to end
 * by the Rust suite's {@code entry_idle} integration test. What is Java's own
 * is everything below.
 */
class EntryIdleWatcherTest {

    private static final long SECOND = 1_000_000_000L;
    private static final long THRESHOLD = SECOND;

    /** Records what it was told, and how often. */
    private static final class Probe implements IdleObserver {
        private final List<Duration> idle = new ArrayList<>();
        private final List<Duration> resumed = new ArrayList<>();

        @Override
        public void idle(Duration gap) {
            idle.add(gap);
        }

        @Override
        public void resumed(Duration gap) {
            resumed.add(gap);
        }
    }

    private static EntryIdleWatcher.Watch watch(IdleObserver observer) {
        return new EntryIdleWatcher.Watch("trades_in", THRESHOLD, observer);
    }

    @Test
    @DisplayName("a cold start never fires, however long it lasts")
    void coldStartNeverFires() {
        assertThat(watch(new Probe()).evaluate(3600 * SECOND, 0)).isNull();
    }

    @Test
    @DisplayName("an episode fires once and a resume re-arms it")
    void oneEpisodeThenResumeThenAnother() {
        EntryIdleWatcher.Watch watch = watch(new Probe());

        assertThat(watch.evaluate(SECOND + SECOND / 2, SECOND))
                .as("inside the threshold")
                .isNull();

        assertThat(watch.evaluate(3 * SECOND, SECOND))
                .isEqualTo(new EntryIdleWatcher.Episode(true, Duration.ofSeconds(2)));
        assertThat(watch.evaluate(4 * SECOND, SECOND))
                .as("latched for the episode")
                .isNull();
        assertThat(watch.evaluate(60 * SECOND, SECOND)).as("still latched").isNull();

        assertThat(watch.evaluate(61 * SECOND, 60 * SECOND))
                .as("the resume reports the silence that ended")
                .isEqualTo(new EntryIdleWatcher.Episode(false, Duration.ofSeconds(59)));
        assertThat(watch.evaluate(61 * SECOND, 60 * SECOND))
                .as("re-armed, inside the threshold")
                .isNull();
        assertThat(watch.evaluate(63 * SECOND, 60 * SECOND))
                .as("a later gap fires a second episode")
                .isEqualTo(new EntryIdleWatcher.Episode(true, Duration.ofSeconds(3)));
    }

    @Test
    @DisplayName("the tick is a quarter of the threshold, within its bounds")
    void tickIsAQuarterOfTheThreshold() {
        assertThat(EntryIdleWatcher.tickPeriod(Duration.ofSeconds(60))).isEqualTo(Duration.ofSeconds(1));
        assertThat(EntryIdleWatcher.tickPeriod(Duration.ofSeconds(2))).isEqualTo(Duration.ofMillis(500));
        assertThat(EntryIdleWatcher.tickPeriod(Duration.ofMillis(10))).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    @DisplayName("a throwing observer is contained and later episodes still reach it")
    void throwingObserverIsContained() {
        List<Duration> seen = new ArrayList<>();
        IdleObserver thrower = gap -> {
            seen.add(gap);
            throw new IllegalStateException("entry-idle observer under test");
        };
        EntryIdleWatcher.Watch watch = watch(thrower);

        assertThatCode(() -> watch.deliver(new EntryIdleWatcher.Episode(true, Duration.ofSeconds(2))))
                .doesNotThrowAnyException();
        assertThatCode(() -> watch.deliver(new EntryIdleWatcher.Episode(true, Duration.ofSeconds(9))))
                .doesNotThrowAnyException();
        assertThat(seen).containsExactly(Duration.ofSeconds(2), Duration.ofSeconds(9));
    }

    @Test
    @DisplayName("an observer implementing only idle() ignores the resume")
    void resumeDefaultsToDoingNothing() {
        List<Duration> seen = new ArrayList<>();
        EntryIdleWatcher.Watch watch = watch(seen::add);

        assertThatCode(() -> watch.deliver(new EntryIdleWatcher.Episode(false, Duration.ofSeconds(3))))
                .doesNotThrowAnyException();
        assertThat(seen).isEmpty();
    }

    @Test
    @DisplayName("the last-delivery instant is absent before the first frame, else derived from the gap")
    void deliveryInstantMapsTheSentinelToAbsent() {
        Instant wallNow = Instant.parse("2026-08-17T12:00:00Z");

        assertThat(EntryIdleWatcher.deliveryInstant(0, 5 * SECOND, wallNow))
                .as("0 is the never-delivered sentinel, not a zero or start-time value")
                .isEqualTo(Optional.empty());

        assertThat(EntryIdleWatcher.deliveryInstant(2 * SECOND, 5 * SECOND, wallNow))
                .contains(wallNow.minusSeconds(3));
    }
}
