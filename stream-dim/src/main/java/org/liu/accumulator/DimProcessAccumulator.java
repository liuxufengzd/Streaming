package org.liu.accumulator;

import org.apache.spark.util.AccumulatorV2;
import org.liu.bean.DimTableMeta;

import java.util.HashMap;
import java.util.Map;

public class DimProcessAccumulator extends AccumulatorV2<Map.Entry<String, DimTableMeta>, Map<String, DimTableMeta>> {
    private final Map<String, DimTableMeta> map = new HashMap<>();

    @Override
    public boolean isZero() {
        return map.isEmpty();
    }

    @Override
    public AccumulatorV2<Map.Entry<String, DimTableMeta>, Map<String, DimTableMeta>> copy() {
        DimProcessAccumulator accumulator = new DimProcessAccumulator();
        synchronized (map) {
            accumulator.map.putAll(map);
        }
        return accumulator;
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void add(Map.Entry<String, DimTableMeta> v) {
        synchronized (map) {
            map.put(v.getKey(), v.getValue());
        }
    }

    @Override
    public void merge(AccumulatorV2<Map.Entry<String, DimTableMeta>, Map<String, DimTableMeta>> other) {
        if (other instanceof DimProcessAccumulator) {
            DimProcessAccumulator o = (DimProcessAccumulator) other;
            synchronized (map) {
                map.putAll(o.map);
            }
        } else {
            throw new UnsupportedOperationException("Cannot merge " + this.getClass().getName() + " with " + other.getClass().getName());
        }
    }

    @Override
    public Map<String, DimTableMeta> value() {
        synchronized (map) {
            return new HashMap<>(map);
        }
    }
}
