package rb.ks.join.impl;

import org.omg.CORBA.Object;
import rb.ks.join.Joiner;

import java.util.HashMap;
import java.util.Map;

public class TablePreferredJoiner  extends Joiner<Map<String, Object>, Map<String, Object>> {

    @Override
    public Map<String, Object> join(Map<String, Object> stream, Map<String, Object> table) {
        Map<String, Object> joinerMap = new HashMap<>(stream);
        joinerMap.putAll(table);
        return joinerMap;
    }
}
