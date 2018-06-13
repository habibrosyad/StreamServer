package edu.monash.streaming.streamserver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class Statistic {
    private final Map<StatisticType, LongAdder> counters;
    private final Map<Integer, Map<StatisticType, Long>> snapshots;
    private final AtomicBoolean stopped;
    private final AtomicBoolean started;
    private final Stopwatch stopwatch;
    private final Thread capture;

    public Statistic() {
        started = new AtomicBoolean();
        stopped = new AtomicBoolean();
        snapshots = new HashMap<>();
        counters = new HashMap<>();

        // Fill counters
        for (StatisticType type: StatisticType.values()) {
            counters.put(type, new LongAdder());
        }

        // Create the capture
        capture = new Thread(() -> {
            int checkpoint = 0;
            try {
                while (!stopped.get()) {
                    // Let it sleep when the counter start
                    Thread.sleep(1000);
                    // After the interval is elapsed record the current state
                    snapshots.put(checkpoint++, getSnapshot());
                }
            } catch(Exception e) {
                // Do nothing
            }
        });
        // Initiate stopwatch for calculating execution time
        stopwatch = new Stopwatch();
    }

    private Map<StatisticType, Long> getSnapshot() {
        Map<StatisticType, Long> snapshot = new HashMap<>();
        counters.forEach((type, counter) -> {
            snapshot.put(type, counter.sum());
        });
        return snapshot;
    }

    public LongAdder get(StatisticType type) {
        return counters.get(type);
    }

    /**
     * Start to record snapshot, do it from another capture
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            stopwatch.start();
            capture.start();
        }
    }

    /**
     * Stop recording snapshots
     */
    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            counters.get(StatisticType.EXECUTION_TIME).add(stopwatch.elapsed());
        }
    }

    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();

        for (StatisticType type: counters.keySet()) {
            string.append(type.toString())
                    .append(":")
                    .append(counters.get(type).sumThenReset())
                    .append("\n");
        }

        string.append("---\n");

        for (Integer time: snapshots.keySet()) {
            string.append(time + "\n");
            for (StatisticType type: snapshots.get(time).keySet()) {
                string.append(type.toString())
                        .append(":")
                        .append(snapshots.get(time).get(type))
                        .append("\n");
            }
        }

        return string.toString();
    }
}
