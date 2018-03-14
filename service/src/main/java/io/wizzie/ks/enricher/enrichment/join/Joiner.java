package io.wizzie.ks.enricher.enrichment.join;

import java.util.Map;

public interface Joiner<V1, V2> {
     Map<String, Object> join(V1 stream, V2 table);
    void init(String name);
}
