package liquibase.ext.neo4j

class MapUtils {

    static <K, V> boolean containsAll(Map<K, V> map1, Map<K, V> map2) {
        map1.entrySet().containsAll(map2.entrySet())
    }
}
