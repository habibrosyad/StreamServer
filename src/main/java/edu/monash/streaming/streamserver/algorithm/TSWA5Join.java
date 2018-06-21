package edu.monash.streaming.streamserver.algorithm;

import edu.monash.streaming.streamserver.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class TSWA5Join implements Join {
    private final Receiver receiver;
    private final long W;
    private final long T;
    private final long Te;
    private final Map<String, Map<String, List<Tuple>>> tables;
    private final Service service;

    public TSWA5Join(Receiver receiver, long W, long T, long Te) {
        this.receiver = receiver;
        this.W = W;
        this.T = T;
        this.Te = Te;

        // Setup hash tables
        tables = new HashMap<>();
        for (String stream : receiver.getStreams()) {
            tables.put(stream, new HashMap<>());
        }

        service = new Service(receiver.getStreams().length);
    }

    public void start() {
        // Start taking statistic snapshot
        service.getStatistic().start();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        scheduler.scheduleAtFixedRate(new Runnable() {
            private final ExecutorService executor = Executors.newSingleThreadExecutor();
            private Future<?> lastExecution;
            private final LongAdder counter = new LongAdder();

            @Override
            public void run() {
                if (service.getStopped().get()) {
                    service.getStatistic().stop();
                    // Output all statistics
                    System.out.println("Statistic:");
                    System.out.println(service.getStatistic().toString());
                    System.exit(0);
                }

                // Increment for T counter
                counter.increment();

                if (lastExecution != null && !lastExecution.isDone()) {
                    service.getStatistic().getCounter(StatisticType.MISS).increment();
                    return;
                }

                lastExecution = executor.submit(() -> {
                    long now = System.currentTimeMillis();

                    joining(now);

                    // If T second expire
                    if (counter.sum() % (T / 1000) == 0) {
                        windowing(now);
                    }
                });
            }
        }, Te, Te, TimeUnit.MILLISECONDS);
    }

    private void joining(long now) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        // 1. For each tuple s arriving on Sk (1 ≤ k ≤ m) for the last Te seconds,
        for (String origin: receiver.getStreams()) {
            BlockingQueue<Tuple> queue = receiver.getBuffer().get(origin);
            Tuple newTuple;

            while ((newTuple = queue.peek()) != null && !newTuple.equals(Receiver.POISON)) {
                try {
                    // Take newTuple out from buffer
                    newTuple = queue.take();

                    if (newTuple.getTimestamp() > now-Te && newTuple.getTimestamp() <= now) {
                        // Increase statistic
                        service.getStatistic().getCounter(StatisticType.INPUT).increment();

                        // 1.1 v←f(s),and insert s to Lk
                        List<List<Tuple>> L = new ArrayList<>();
                        L.add(new ArrayList<>());
                        L.get(0).add(newTuple);

                        boolean hasMatches = true;

                        for (String stream : receiver.getStreams()) {
                            // 1.2 For each Si (1 ≤ i ≤ m, i != k)
                            if (!stream.equals(origin)) {
                                // 1.2.1 Li ←Hi.getCounter(v)
                                List<Tuple> Li = tables.get(stream).get(newTuple.getKey());

                                // Increase statistic
                                service.getStatistic().getCounter(StatisticType.PROBE).increment();

                                // 1.2.2 If Li is empty, go to Step 1.4
                                if (Li == null || Li.size() == 0) {
                                    hasMatches = false;
                                    break;
                                }
                                L.add(Li);
                            }
                        }

                        // 1.3. Output a Cartesian product of all List
                        if (hasMatches) {
                            List<List<Tuple>> output = Joiner.product(L);
                            service.getStatistic().getCounter(StatisticType.OUTPUT).add(output.size());
                        }

                        // 1.4. Add s to Hk
                        List<Tuple> tuples = tables.get(origin).getOrDefault(newTuple.getKey(), new LinkedList<>());
                        tuples.add(newTuple);
                        tables.get(origin).put(newTuple.getKey(), tuples);
                    }
                } catch (InterruptedException e) {
                    System.out.println("Failed to process tuple " + newTuple);
                }
            }

            // Prepare termination upon receiving poison
            if (queue.peek() != null && queue.peek().equals(Receiver.POISON)) {
                service.getCompletion().increment();

                if (service.getCompletion().sum() == receiver.getStreams().length) {
                    service.getStopped().set(true);
                    System.out.println("Shutting down joiner...");
                }
            }
        }

        long elapsed = stopwatch.elapsed();
        service.getStatistic().getCounter(StatisticType.JOIN).increment();
        service.getStatistic().getCounter(StatisticType.JOIN_TIME).add(elapsed);
        service.getStatistic().getMinMax(StatisticType.JOIN_TIME_MIN).accumulate(elapsed);
        service.getStatistic().getMinMax(StatisticType.JOIN_TIME_MAX).accumulate(elapsed);
    }

    private void windowing(long now) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        // 2. If T seconds expire, update all His by discarding expired tuples
        for (String stream: receiver.getStreams()) {
            Iterator<Map.Entry<String, List<Tuple>>> tableIterator = tables.get(stream).entrySet().iterator();

            while (tableIterator.hasNext()) {
                Map.Entry<String, List<Tuple>> entry = tableIterator.next();
                List<Tuple> tuples = entry.getValue();
                Iterator<Tuple> tupleIterator = tuples.iterator();

                while (tupleIterator.hasNext()) {
                    Tuple tuple = tupleIterator.next();

                    if (now-W+T > tuple.getTimestamp()) {
                        tupleIterator.remove();
                        service.getStatistic().getCounter(StatisticType.REMOVE).increment();
                    } else {
                        break;
                    }
                }

                // Remove hash entry when no tuple stored
                if (tuples.isEmpty()) {
                    tableIterator.remove();
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
}