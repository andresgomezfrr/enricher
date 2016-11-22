package io.wizzie.ks.enricher.enrichment.join.impl.queryableback;

import io.wizzie.ks.enricher.enrichment.join.QueryableBackJoiner;

import java.util.HashMap;
import java.util.Map;

public class TablePreferredJoiner  extends QueryableBackJoiner<Map<String, Object>> {

    @Override
    public Map<String, Object> join(Map<String, Object> stream, Map<String, Object> table) {
        Map<String, Object> joinerMap = new HashMap<>();
        if (stream != null) joinerMap.putAll(stream);
        if (table != null) joinerMap.putAll(table);
        return joinerMap;
    }
}
