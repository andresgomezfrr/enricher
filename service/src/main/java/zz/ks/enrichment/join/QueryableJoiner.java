package zz.ks.enrichment.join;

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.HashMap;
import java.util.Map;

public abstract class QueryableJoiner<V2> implements Joiner<Map<String, Object>, V2>,
        ValueJoiner<Map<String, Object>, V2, Map<String, Object>> {
    String name;

    @Override
    public void init(String name) {
        this.name = name;
    }

    public Map<String, Object> joinWithQuery(Map<String, Object> value1, V2 value2) {
        Map<String, Object> result = new HashMap<>();

        if (value2 == null) {
            result.put("type", "joiner-query");
            result.put("joiner", name);
            result.put("message", value1);
        } else {
            result.put("message", join(value1, value2));
        }

        return result;
    }

    @Override
    public Map<String, Object> apply(Map<String, Object> value1, V2 value2) {
        return joinWithQuery(value1, value2);
    }
}
