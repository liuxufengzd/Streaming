package org.liu.accumulator;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;

public class LogAccumulator extends AccumulatorV2<String, List<String>> {
    private final List<String> logs = new ArrayList<>();
    @Override
    public boolean isZero() {
        return logs.isEmpty();
    }

    @Override
    public AccumulatorV2<String, List<String>> copy() {
        LogAccumulator logAccumulator = new LogAccumulator();
        synchronized (logs){
            logAccumulator.logs.addAll(logs);
        }
        return logAccumulator;
    }

    @Override
    public void reset() {
        logs.clear();
    }

    @Override
    public void add(String v) {
        logs.add(v);
    }

    @Override
    public void merge(AccumulatorV2<String, List<String>> other) {
        synchronized (logs){
            logs.addAll(other.value());
        }
    }

    @Override
    public List<String> value() {
        return logs;
    }
}
