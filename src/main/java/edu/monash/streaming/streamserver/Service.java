package edu.monash.streaming.streamserver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class Service {
    private final Statistic statistic;
    private final ExecutorService pool;
    private final LongAdder completion;
    private final AtomicBoolean stopped;

    public Service(int poolSize) {
        statistic = new Statistic();
        pool = Executors.newFixedThreadPool(poolSize);
        completion = new LongAdder();
        stopped = new AtomicBoolean();
    }

    public Statistic getStatistic() {
        return statistic;
    }

    public ExecutorService getPool() {
        return pool;
    }

    public LongAdder getCompletion() {
        return completion;
    }

    public AtomicBoolean getStopped() {
        return stopped;
    }
}
