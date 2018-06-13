package edu.monash.streaming.streamserver.algorithm;

import edu.monash.streaming.streamserver.*;

import java.util.*;

/**
 * Time-slide window join algorithm 4 with one-table schema
 *
 * Note that the implementation in here are strictly following the specification in the original publication:
 * we only maintain a single hash table which is also treated as the window extent of the algorithm
 */
public class TSWA4OJoin implements Join {
    private final Receiver receiver;
    private final long W;
    private final long T;
    private final long N;
    private final Map<String, Map<String, List<Tuple>>> tables;
    private final Service service;

    public TSWA4OJoin(Receiver receiver, long W, long T, long N) {
        this.receiver = receiver;
        this.W = W;
        this.T = T;
        this.N = N/1000;

        // Setup hash tables and windows
        tables = new HashMap<>();
        for (String stream : receiver.getStreams()) {
            tables.put(stream, new HashMap<>());
        }

        service = new Service(receiver.getStreams().length);
    }

    public void start() {
        // Start taking statistic snapshot
        service.getStatistic().start();

        // Run join on all streams
        Thread processor = new Thread(() -> {
            long checkpoint = System.currentTimeMillis();

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
                    if (newTuple.getTimestamp()-checkpoint >= T) {
                        windowing(newTuple);
                        checkpoint = newTuple.getTimestamp();
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

        // Increase statistic
        service.getStatistic().get(StatisticType.INPUT).increment();

        // Insert s to Lk
        List<List<Tuple>> L = new ArrayList<>();
        L.add(new ArrayList<>());
        L.get(0).add(newTuple);

        boolean hasMatches = true;

        for (String stream: receiver.getStreams()) {
            // For each Si (1 ≤ i ≤ m, i != k)
            if (!stream.equals(newTuple.getOrigin())) {
                // Li ←Hi.get(v)
                List<Tuple> Li = tables.get(stream).get(newTuple.getKey());
                // Increase statistic
                service.getStatistic().get(StatisticType.PROBE).increment();
                // If Li is empty, go to Step 1.4
                if (Li == null || Li.size() == 0) {
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

        // Add s to Hk
        List<Tuple> tuples = tables.get(newTuple.getOrigin()).getOrDefault(newTuple.getKey(), new LinkedList<>());
        tuples.add(newTuple);
        tables.get(newTuple.getOrigin()).put(newTuple.getKey(), tuples);

        service.getStatistic().get(StatisticType.JOIN).increment();
        service.getStatistic().get(StatisticType.JOIN_TIME).add(stopwatch.elapsed());
    }

    private void windowing(Tuple newTuple) {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        for (String stream: receiver.getStreams()) {
            Iterator<Map.Entry<String, List<Tuple>>> tableIterator = tables.get(stream).entrySet().iterator();

            while (tableIterator.hasNext()) {
                Map.Entry<String, List<Tuple>> entry = tableIterator.next();
                List<Tuple> tuples = entry.getValue();
                Iterator<Tuple> tupleIterator = tuples.iterator();

                while (tupleIterator.hasNext()) {
                    Tuple tuple = tupleIterator.next();

                    if (newTuple.getTimestamp()-W+T > tuple.getTimestamp()) {
                        tupleIterator.remove();
                        service.getStatistic().get(StatisticType.REMOVE).increment();
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
        service.getStatistic().get(StatisticType.WINDOWING).increment();
        service.getStatistic().get(StatisticType.WINDOWING_TIME).add(stopwatch.elapsed());
    }
}
