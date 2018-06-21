package edu.monash.streaming.streamserver;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

public class Statistic {
    private final Map<StatisticType, LongAdder> counters;
    private final Map<StatisticType, LongAccumulator> minmax;
//    private final Map<Integer, Map<StatisticType, Long>> snapshots;
    private final AtomicBoolean stopped;
    private final AtomicBoolean started;
    private final Stopwatch stopwatch;
//    private final Thread capture;

    public Statistic() {
        started = new AtomicBoolean();
        stopped = new AtomicBoolean();
//        snapshots = new HashMap<>();
        counters = new HashMap<>();
        minmax = new HashMap<>();

        // Fill counters
        for (StatisticType type: StatisticType.values()) {
            if (type.name().contains("_MIN")) {
                minmax.put(type, new LongAccumulator(Long::min, Long.MAX_VALUE));
            } else if (type.name().contains("_MAX")) {
                minmax.put(type, new LongAccumulator(Long::max, 0L));
            } else {
                counters.put(type, new LongAdder());
            }
        }

        // Create the capture
//        capture = new Thread(() -> {
//            int checkpoint = 0;
//            try {
//                while (!stopped.get()) {
//                    // Let it sleep when the counter start
//                    Thread.sleep(1000);
//                    // After the interval is elapsed record the current state
//                    snapshots.put(checkpoint++, getSnapshot());
//                }
//            } catch(Exception e) {
//                // Do nothing
//            }
//        });
        // Initiate stopwatch for calculating execution time
        stopwatch = new Stopwatch();
    }

    private Map<StatisticType, Long> getSnapshot() {
        Map<StatisticType, Long> snapshot = new HashMap<>();
        counters.forEach((type, counter) -> {
//            snapshot.put(type, counter.sum());
        });
        minmax.forEach((type, accumulator) -> {
//            snapshot.put(type, accumulator.get());
        });
        return snapshot;
    }

    public LongAdder getCounter(StatisticType type) {
        if (!type.name().contains("_MIN") && !type.name().contains("_MAX")) {
            return counters.get(type);
        }
        return null;
    }

    public LongAccumulator getMinMax(StatisticType type) {
        if (type.name().contains("_MIN") || type.name().contains("_MAX")) {
            return minmax.get(type);
        }
        return null;
    }

    /**
     * Start to record snapshot, do it from another capture
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            stopwatch.start();
//            capture.start();
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

        for (StatisticType type: minmax.keySet()) {
            string.append(type.toString())
                    .append(":")
                    .append(minmax.get(type).get())
                    .append("\n");
        }

//        string.append("---\n");
//
//        for (Integer time: snapshots.keySet()) {
//            string.append(time + "\n");
//            for (StatisticType type: snapshots.get(time).keySet()) {
//                string.append(type.toString())
//                        .append(":")
//                        .append(snapshots.get(time).get(type))
//                        .append("\n");
//            }
//        }

        return string.toString();
    }
}
