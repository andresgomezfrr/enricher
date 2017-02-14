package rb.ks.enrichment.join;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.omg.CORBA.Object;

import java.util.Map;

public abstract class Joiner<V1, V2> implements
        ValueJoiner<V1, V2, Map<String, Object>> {

    public abstract Map<String, Object> join(V1 stream, V2 table);

    @Override
    public Map<String, Object> apply(V1 value1, V2 value2) {
        return join(value1, value2);
    }
}
