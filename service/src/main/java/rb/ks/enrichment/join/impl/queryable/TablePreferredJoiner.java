package rb.ks.enrichment.join.impl.queryable;

import rb.ks.enrichment.join.QueryableJoiner;

import java.util.HashMap;
import java.util.Map;

public class TablePreferredJoiner  extends QueryableJoiner<Map<String, Object>> {

    @Override
    public Map<String, Object> join(Map<String, Object> stream, Map<String, Object> table) {
        Map<String, Object> joinerMap = new HashMap<>();
        if (stream != null) joinerMap.putAll(stream);
        if (table != null) joinerMap.putAll(table);
        return joinerMap;
    }
}
