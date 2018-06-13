package edu.monash.streaming.streamserver;

public class Stopwatch {
    private long start;

    public void start() {
        start = System.nanoTime();
    }

    public long elapsed() {
        return (System.nanoTime()-start);
    }
}