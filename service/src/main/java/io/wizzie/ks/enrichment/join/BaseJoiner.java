package io.wizzie.ks.enrichment.join;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Map;

public abstract class BaseJoiner<V1, V2> implements Joiner<V1, V2>, ValueJoiner<V1, V2, Map<String, Object>> {
    String name;

    @Override
    public void init(String name) {
        this.name = name;
    }

    @Override
    public Map<String, Object> apply(V1 value1, V2 value2) {
        return join(value1, value2);
    }
}
