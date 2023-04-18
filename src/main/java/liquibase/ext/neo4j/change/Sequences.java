package liquibase.ext.neo4j.change;

import java.util.Collection;

class Sequences {

    public static boolean isNullOrEmpty(String string) {
        return string == null || string.trim().isEmpty();
    }

    public static boolean isNullOrEmpty(Collection<?> items) {
        return items == null || items.isEmpty();
    }
}
