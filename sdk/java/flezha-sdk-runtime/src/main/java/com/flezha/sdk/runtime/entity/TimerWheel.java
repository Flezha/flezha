package com.flezha.sdk.runtime.entity;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One processing-time timer wheel per Operative instance. Expiry pushes a
 * {@code TimerFire} onto the target Entity's existing mailbox rather than
 * invoking {@code onTimer} itself — that is what makes firing obey the same
 * ordering, the same back-pressure and the same single-drainer exclusion an
 * input already does, with no second concurrency mechanism.
 *
 * <p>Entries hold a {@link WeakReference} to the slot, so an idle-evicted
 * Entity's timers die with it: the reference clears and the firing is dropped.
 * The reaper also discards them eagerly via {@link #discardEntity}.
 *
 * <p>Event-time timers need nothing here — the Entity's event-time high-water
 * fires them on the dispatch path — so an Operative that uses only those never
 * causes this thread to exist.
 */
public final class TimerWheel {

    private static final Logger LOG = LoggerFactory.getLogger(TimerWheel.class);

    /** Longest the thread sleeps with nothing armed, so a stop is honoured promptly. */
    private static final long IDLE_TICK_MS = 200L;

    /** Fire order is by deadline, ties broken by Entity identity so the order is total. */
    private record ArmedAt(long at, int procedure, String entityKey) implements Comparable<ArmedAt> {
        private static final Comparator<ArmedAt> ORDER = Comparator.comparingLong(ArmedAt::at)
                .thenComparingInt(ArmedAt::procedure)
                .thenComparing(ArmedAt::entityKey);

        @Override
        public int compareTo(ArmedAt other) {
            return ORDER.compare(this, other);
        }
    }

    private final TreeMap<ArmedAt, WeakReference<EntitySlot>> armed = new TreeMap<>();
    private final Object lock = new Object();
    private final BiConsumer<EntitySlot, Long> deliver;
    private volatile boolean stopped;
    private Thread thread;

    /**
     * @param deliver pushes one expired {@code (slot, scheduledAt)} onto the
     *     Entity's mailbox. Injected so the wheel stays free of the dispatcher's
     *     envelope types and is testable on its own.
     */
    public TimerWheel(@Nonnull BiConsumer<EntitySlot, Long> deliver) {
        this.deliver = deliver;
    }

    /** Start the wheel thread. Idempotent. */
    public synchronized void start() {
        if (thread != null) {
            return;
        }
        thread = new Thread(this::loop, "flezha-timer-wheel");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Arm one processing-time timer. Re-arming the same
     * {@code (procedure, key, scheduledAt)} replaces the entry — pending timers
     * are a set, so this is the idempotence the surface promises.
     */
    public void arm(int procedure, @Nonnull String entityKey, long scheduledAt, @Nonnull EntitySlot slot) {
        synchronized (lock) {
            if (armed.put(new ArmedAt(scheduledAt, procedure, entityKey), new WeakReference<>(slot)) == null) {
                TimerCounters.noteArmed(1);
            }
            lock.notifyAll();
        }
    }

    /** Disarm one timer. Idempotent — a firing that already popped the entry makes this a no-op. */
    public void disarm(int procedure, @Nonnull String entityKey, long scheduledAt) {
        synchronized (lock) {
            if (armed.remove(new ArmedAt(scheduledAt, procedure, entityKey)) != null) {
                TimerCounters.noteArmed(-1);
            }
        }
    }

    /**
     * Drop every timer armed for one Entity — what the dynamic-mode reaper calls
     * on eviction, so the wheel releases the memory then rather than at each
     * timer's deadline.
     */
    public void discardEntity(int procedure, @Nonnull String entityKey) {
        synchronized (lock) {
            Iterator<ArmedAt> it = armed.keySet().iterator();
            while (it.hasNext()) {
                ArmedAt key = it.next();
                if (key.procedure() == procedure && key.entityKey().equals(entityKey)) {
                    it.remove();
                    TimerCounters.noteArmed(-1);
                }
            }
        }
    }

    /** Number of timers currently armed on this wheel. */
    public int occupancy() {
        synchronized (lock) {
            return armed.size();
        }
    }

    /** Stop the wheel thread. Idempotent. */
    public void stop() {
        stopped = true;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /**
     * Pop every entry due at {@code now}, ascending, and deliver it. Package
     * visible so a test can drive one pass without waiting on the thread.
     *
     * @return how long to wait before asking again
     */
    long fireDue(long now) {
        List<Map.Entry<ArmedAt, EntitySlot>> due = new ArrayList<>();
        long wait;
        synchronized (lock) {
            while (!armed.isEmpty()) {
                Map.Entry<ArmedAt, WeakReference<EntitySlot>> first = armed.firstEntry();
                if (first.getKey().at() > now) {
                    break;
                }
                armed.pollFirstEntry();
                TimerCounters.noteArmed(-1);
                EntitySlot slot = first.getValue().get();
                if (slot != null) {
                    due.add(Map.entry(first.getKey(), slot));
                }
            }
            wait = armed.isEmpty()
                    ? IDLE_TICK_MS
                    : Math.max(1L, Math.min(IDLE_TICK_MS, armed.firstKey().at() - now));
        }
        for (Map.Entry<ArmedAt, EntitySlot> entry : due) {
            deliver.accept(entry.getValue(), entry.getKey().at());
        }
        return wait;
    }

    private void loop() {
        while (!stopped) {
            long wait;
            try {
                wait = fireDue(System.currentTimeMillis());
            } catch (RuntimeException e) {
                LOG.error("timer wheel pass failed: {}", e.toString());
                wait = IDLE_TICK_MS;
            }
            synchronized (lock) {
                if (stopped) {
                    return;
                }
                try {
                    lock.wait(wait);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
