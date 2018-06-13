package edu.monash.streaming.streamserver;

import java.io.Serializable;

public class Tuple implements Serializable {
    private final long timestamp;
    private final String origin;
    private final String key;
    private final String value;

    public Tuple(long timestamp, String origin, String key, String value) {
        this.timestamp = timestamp;
        this.origin = origin;
        this.key = key;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getOrigin() {
        return origin;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "(" + timestamp + ", " + origin + ", " + key + ", " + value + ")";
    }
}
