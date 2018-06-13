package edu.monash.streaming.streamserver.algorithm;

import edu.monash.streaming.streamserver.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class TSWA6Join implements Join {
    private final Receiver receiver;
    private final long T;
    private final long Te;
    private final long n;
    private final Map<String, TreeMap<Long, Map<String, List<Tuple>>>> tables;
    private final Service service;

    public TSWA6Join(Receiver receiver, long W, long T, long Te) {
        this.receiver = receiver;
        this.T = T;
        this.Te = Te;

        // How many hash table per stream
        n = W/Te;

        // Setup hash tables
        tables = new HashMap<>();
        for (String stream : receiver.getStreams()) {
            tables.put(stream, new TreeMap<>());
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
                    service.getStatistic().get(StatisticType.MISS).increment();
                    return;
                }

                lastExecution = executor.submit(() -> {
                    long now = System.currentTimeMillis();

                    joining(now);

                    // If T second expire
                    if (counter.sum()%(T/1000) == 0) {
                        windowing(now);
                    }
                });
            }
        }, Te, Te, TimeUnit.MILLISECONDS);
    }

    private void joining(long now) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        // 1. For each tuple s ∈ Bτk (1 ≤ k ≤ m,τ = t/Te)
        long tau = now/Te;
        for (String origin: receiver.getStreams()) {
            BlockingQueue<Tuple> queue = receiver.getBuffer().get(origin);
            Tuple newTuple;

            while ((newTuple = queue.peek()) != null && !newTuple.equals(Receiver.POISON)) {
                try {
                    // Take newTuple out from buffer
                    newTuple = queue.take();

                    if (newTuple.getTimestamp() > now-Te && newTuple.getTimestamp() <= now) {

                        // Increase statistic
                        service.getStatistic().get(StatisticType.INPUT).increment();

                        // 1.1 v←f(s), clear all Lis, and insert s to Lk
                        List<List<Tuple>> L = new ArrayList<>();
                        L.add(new ArrayList<>());
                        L.get(0).add(newTuple);

                        boolean hasMatches = true;

                        for (String stream : receiver.getStreams()) {
                            // 1.2 For each Si (1 ≤ i ≤ m, i != k),
                            if (!stream.equals(origin)) {
                                List<Tuple> Li = new ArrayList<>();

                                // 1.2.1 For each Hji (τ−n< j ≤ τ)
                                Map<Long, Map<String, List<Tuple>>> subTables = tables.get(stream).subMap(tau - n, false, tau, true);

                                for (Map<String, List<Tuple>> subTable : subTables.values()) {
                                    // Increase statistic
                                    service.getStatistic().get(StatisticType.PROBE).increment();

                                    // 1.2.1.1 Li .add(Hji.get(v))
                                    if (subTable.containsKey(newTuple.getKey()))
                                        Li.addAll(subTable.get(newTuple.getKey()));
                                }

                                // 1.2.2 If Li is empty, go to Step 1.4
                                if (Li.size() == 0) {
                                    hasMatches = false;
                                    break;
                                }
                                L.add(Li);
                            }
                        }

                        // 1.3. Output a Cartesian product of all List
                        if (hasMatches) {
                            List<List<Tuple>> output = Joiner.product(L);
                            service.getStatistic().get(StatisticType.OUTPUT).add(output.size());
                        }

                        // 1.4. If Hτk does not exist, create it
                        if (!tables.get(origin).containsKey(tau)) {
                            tables.get(origin).put(tau, new HashMap<>());
                        }

                        // 1.5. Add s to Hτk
                        List<Tuple> tuples = tables.get(origin).get(tau).getOrDefault(newTuple.getKey(), new LinkedList<>());
                        tuples.add(newTuple);
                        tables.get(origin).get(tau).put(newTuple.getKey(), tuples);
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

        service.getStatistic().get(StatisticType.JOIN).increment();
        service.getStatistic().get(StatisticType.JOIN_TIME).add(stopwatch.elapsed());
    }

    private void windowing(long now) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        long tau = now/Te;

        // 2.1. Discard all Hji s(1 ≤ i ≤ m, τ −n<j ≤ τ −n+T/Te)
        for (String stream: receiver.getStreams()) {
            Iterator<Long> it = tables.get(stream).keySet().iterator();

            while (it.hasNext()) {
                long key = it.next();
                if (key > tau-n && key <= tau-n+1) {
                    it.remove();
                    service.getStatistic().get(StatisticType.REMOVE).increment();
                }
            }
        }

        // Increase statistic
        service.getStatistic().get(StatisticType.WINDOWING).increment();
        service.getStatistic().get(StatisticType.WINDOWING_TIME).add(stopwatch.elapsed());
    }
}
