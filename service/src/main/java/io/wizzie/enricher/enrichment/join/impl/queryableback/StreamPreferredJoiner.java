package io.wizzie.enricher.enrichment.join.impl.queryableback;

import io.wizzie.enricher.enrichment.join.QueryableBackJoiner;

import java.util.HashMap;
import java.util.Map;

public class StreamPreferredJoiner extends QueryableBackJoiner<Map<String, Object>> {

    @Override
    public Map<String, Object> join(Map<String, Object> stream, Map<String, Object> table) {
        Map<String, Object> joinerMap = new HashMap<>();
        if (table != null) joinerMap.putAll(table);
        if (stream != null) joinerMap.putAll(stream);
        return joinerMap;
    }
}
