package edu.monash.streaming.streamserver.algorithm;

import edu.monash.streaming.streamserver.*;

import java.util.*;

/**
 * Time-slide window join algorithm 4 with multi-table schema
 */
public class TSWA4MJoin implements Join {
    private final Receiver receiver;
    private final long T;
    private final long N;
    private final long n;
    private final Map<String, TreeMap<Long, Map<String, List<Tuple>>>> tables;
    private final Service service;

    public TSWA4MJoin(Receiver receiver, long W, long T, long N) {
        this.receiver = receiver;
        this.T = T;
        this.N = N/1000;

        // How many hash table per stream
        n = W/T;

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

        // Run join on all streams
        Thread processor = new Thread(() -> {
            // Since our system rely on actual timestamp we can't use 1 as in the algorithm for the lastBid value
            long lastBid = System.currentTimeMillis()/T;

            while (!service.getStopped().get()) {
                // This will be blocked until we got the quota
                List<Tuple> buffer = buffering();

                // For each tuple in the buffer
                for (Tuple newTuple: buffer) {
                    // Check poison
                    if (newTuple.equals(Receiver.POISON)) {
                        service.getStopped().set(true);
                        break;
                    }

                    // Windowing
                    if (newTuple.getTimestamp() > lastBid * T) {
                        windowing(newTuple);
                        lastBid = newTuple.getTimestamp()/T;
                    }

                    // Joining
                    joining(newTuple);
                }
            }

            service.getStatistic().stop();

            // Output all statistics
            System.out.println("Statistic:");
            System.out.println(service.getStatistic().toString());
            System.exit(0);
        });
        processor.start();
    }

    /**
     * Buffering the tuples until we got N tuples (we assume that N tuples are for the whole stream)
     *
     * @return List<Tuple>
     */
    private List<Tuple> buffering() {
        List<Tuple> buffer = new ArrayList<>();
        long counter = 0;

        // Keep buffering until we fulfill the quota
        try {
            while (counter != N) {
                buffer.add(receiver.getFlattenedBuffer().take());
                counter++;
            }
        } catch (InterruptedException e) {
            System.out.println("Failed to collect the buffer");
        }
        return buffer;
    }

    private void joining(Tuple newTuple) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        // Window ID
        long tau = newTuple.getTimestamp()/T;

        // Increase statistic
        service.getStatistic().get(StatisticType.INPUT).increment();

        // Insert s to Lk
        List<List<Tuple>> L = new ArrayList<>();
        L.add(new ArrayList<>());
        L.get(0).add(newTuple);

        boolean hasMatches = true;

        for (String stream: receiver.getStreams()) {
            // For each Si (1 ≤ i ≤ m, i != k),
            if (!stream.equals(newTuple.getOrigin())) {
                List<Tuple> Li = new ArrayList<>();

                // For each Hji (τ−n< j ≤ τ)
                Map<Long, Map<String, List<Tuple>>> subTables = tables.get(stream).subMap(tau-n, false, tau, true);

                for (Map<String, List<Tuple>> subTable: subTables.values()) {
                    // Increase statistic
                    service.getStatistic().get(StatisticType.PROBE).increment();

                    // Li .add(Hji.get(v))
                    if (subTable.containsKey(newTuple.getKey())) Li.addAll(subTable.get(newTuple.getKey()));
                }

                // If Li is empty, go to Step 1.4
                if (Li.size() == 0) {
                    hasMatches = false;
                    break;
                }
                L.add(Li);
            }
        }

        // Output a Cartesian product of all List
        if (hasMatches) {
            List<List<Tuple>> output = Joiner.product(L);
            service.getStatistic().get(StatisticType.OUTPUT).add(output.size());
        }

        // If Hτk does not exist, create it
        if (!tables.get(newTuple.getOrigin()).containsKey(tau)) {
            tables.get(newTuple.getOrigin()).put(tau, new HashMap<>());
        }

        // Add s to Hτk
        List<Tuple> tuples = tables.get(newTuple.getOrigin()).get(tau).getOrDefault(newTuple.getKey(), new LinkedList<>());
        tuples.add(newTuple);
        tables.get(newTuple.getOrigin()).get(tau).put(newTuple.getKey(), tuples);

        service.getStatistic().get(StatisticType.JOIN).increment();
        service.getStatistic().get(StatisticType.JOIN_TIME).add(stopwatch.elapsed());
    }

    private void windowing(Tuple newTuple) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        long tau = newTuple.getTimestamp()/T;

        // Discard all Hji s(1 ≤ i ≤ m, τ−n< j ≤ τ−n+1)
        for (String stream: receiver.getStreams()) {
            Iterator<Long> it = tables.get(stream).keySet().iterator();

            while (it.hasNext()) {
                long key = it.next();
                if (key > tau-n && key <= tau-n+1) {
                    tables.get(stream).remove(key);
                    service.getStatistic().get(StatisticType.REMOVE).increment();
                }
            }
        }

        // Increase statistic
        service.getStatistic().get(StatisticType.WINDOWING).increment();
        service.getStatistic().get(StatisticType.WINDOWING_TIME).add(stopwatch.elapsed());
    }
}
