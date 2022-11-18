package liquibase.ext.neo4j.database.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class Maps {

    public static <K, V1, V2> Map<K, V2> mapValues(Map<K, V1> value, Function<V1, V2> fn) {
        return value.entrySet()
                .stream()
                // Collectors.toMap does not allow null values
                .collect(HashMap::new, (map, entry) -> map.put(entry.getKey(), fn.apply(entry.getValue())), HashMap::putAll);
    }
}
