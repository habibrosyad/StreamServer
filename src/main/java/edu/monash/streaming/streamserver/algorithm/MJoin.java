package edu.monash.streaming.streamserver.algorithm;

import edu.monash.streaming.streamserver.*;

import java.util.*;
import java.util.concurrent.*;

public class MJoin implements Join {
    private final long W;
    private final Receiver receiver;
    private final Map<String, Map<String, List<Tuple>>> tables;
    private final Map<String, List<Tuple>> windows;
    private final Service service;

    public MJoin(Receiver receiver, long W) {
        // Window size
        this.W = W;
        this.receiver = receiver;


        // Setup windows and hash tables
        windows = new HashMap<>();
        tables = new HashMap<>();
        for (String stream : receiver.getStreams()) {
            windows.put(stream, new LinkedList<>());
            tables.put(stream, new HashMap<>());
        }

        service = new Service(receiver.getStreams().length);
    }

    public void start() {
        // Start taking statistic snapshot
        service.getStatistic().start();

        // Run join on all streams
        for (String stream: receiver.getStreams()) {
            service.getPool().submit(new Pipeline(stream));
        }
    }

    private void stop() throws InterruptedException {
        // Prevent multiple execution on this method
        if (service.getStopped().compareAndSet(false, true)) {
            // Stop statistic
            service.getStatistic().stop();

            // Shutdown pool
            System.out.println("Shutting down joiner...");
            service.getPool().shutdown();

            // Immediately terminate, because we already know the completion status
            service.getPool().awaitTermination(1, TimeUnit.MILLISECONDS);

            // Output all statistics
            System.out.println("Statistic:");
            System.out.println(service.getStatistic().toString());
            System.exit(0);
        }
    }

    /**
     * Whenever a new tuple s arrives on Sk (1 ≤ k ≤ m) ...
     * 1. Update all Si [Wi] (1 ≤ i ≤ m) by discarding expired tuples
     * 2. Join s with all Si [Wi ] (i ?= k)
     * 3. Add s to Sk[Wk]
     */
    private class Pipeline implements Runnable {
        private BlockingQueue<Tuple> queue;
        private final String origin;

        public Pipeline(String origin) {
            this.origin = origin;
            queue = receiver.getBuffer().get(origin);
        }

        @Override
        public void run() {
            System.out.println("Process join from stream " + origin + " on " + Thread.currentThread().getName());

            try {
                while (!service.getStopped().get()) {
                    // Whenever a new tuple arrive into the system...
                    Tuple newTuple = queue.take();

                    // Prepare shutdown when receiving poison
                    if (newTuple.equals(receiver.POISON)) {
                        service.getCompletion().increment();

                        if (service.getCompletion().sum() == receiver.getStreams().length) {
                            stop();
                        }

                        break;
                    }

                    // Increase statistic
                    service.getStatistic().getCounter(StatisticType.INPUT).increment();

                    // Do the join logic
                    synchronized (tables) {
                        // 1. Update all Si [Wi] (1 ≤ i ≤ m) by discarding expired tuples
                        windowing(newTuple);

                        // 2. Join s with all Si [Wi ] (i ?= k)
                        // 3. Add s to Sk[Wk]
                        joining(newTuple);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void windowing(Tuple newTuple) throws InterruptedException  {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();

            long expire = newTuple.getTimestamp() - W;

            for (String stream: receiver.getStreams()) {
                // Update window
                List<Tuple> window = windows.get(stream);
                List<Tuple> removed = new ArrayList<>();

                // If the window contains tuples, do the windowing
                if (window.size() > 0) {
                    Iterator<Tuple> it = window.iterator();
                    while (it.hasNext()) {
                        Tuple tuple = it.next();
                        if (tuple.getTimestamp() < expire) {
                            removed.add(tuple);
                            it.remove();
                        } else {
                            break;
                        }
                    }
                } else {
                    continue;
                }

                // Update hash tables using removed list from the windowing
                if (removed.size() > 0) {
                    for (Tuple tuple: removed) {
                        List<Tuple> matches = tables.get(stream).get(tuple.getKey());
                        while (matches.remove(tuple)) {
                            service.getStatistic().getCounter(StatisticType.REMOVE).increment();
                        }

                        if (matches.isEmpty()) {
                            tables.get(stream).remove(tuple.getKey());
                        }
                    }
                }
            }
            // Increase statistic
            long elapsed = stopwatch.elapsed();
            service.getStatistic().getCounter(StatisticType.WINDOWING).increment();
            service.getStatistic().getCounter(StatisticType.WINDOWING_TIME).add(elapsed);
            service.getStatistic().getMinMax(StatisticType.WINDOWING_TIME_MIN).accumulate(elapsed);
            service.getStatistic().getMinMax(StatisticType.WINDOWING_TIME_MAX).accumulate(elapsed);
        }

        private void joining(Tuple newTuple) {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.start();

            List<List<Tuple>> output = new ArrayList<>();
            output.add(new ArrayList<>());
            output.get(0).add(newTuple);

            boolean hasMatches = true;

            // Probe entire tables for joining, use the sequence defined in the streams variable
            for (String stream: receiver.getStreams()) {
                if (!stream.equals(origin)) {
                    // Increase statistic
                    service.getStatistic().getCounter(StatisticType.PROBE).increment();

                    // Probe the hashTable of this stream
                    if (!tables.get(stream).containsKey(newTuple.getKey())) {
                        hasMatches = false;
                        break;
                    }
                    // Build partial matches
                    output = Joiner.product(output, tables.get(stream).get(newTuple.getKey()));
                }
            }

            if (hasMatches) {
                // Output the result
                // System.out.println(output.toString());
                service.getStatistic().getCounter(StatisticType.OUTPUT).add(output.size());
            }

            // Add tuple to the window
            windows.get(origin).add(newTuple);

            // Add tuple to their hash table
            List<Tuple> tuples = tables.get(origin).getOrDefault(newTuple.getKey(), new LinkedList<>());
            tuples.add(newTuple);
            tables.get(origin).put(newTuple.getKey(), tuples);

            long elapsed = stopwatch.elapsed();
            service.getStatistic().getCounter(StatisticType.JOIN).increment();
            service.getStatistic().getCounter(StatisticType.JOIN_TIME).add(elapsed);
            service.getStatistic().getMinMax(StatisticType.JOIN_TIME_MIN).accumulate(elapsed);
            service.getStatistic().getMinMax(StatisticType.JOIN_TIME_MAX).accumulate(elapsed);
        }
    }
}

