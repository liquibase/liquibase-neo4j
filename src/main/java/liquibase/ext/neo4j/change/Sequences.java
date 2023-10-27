package liquibase.ext.neo4j.change;

import java.util.Collection;

class Sequences {

    public static boolean isNullOrBlank(String string) {
        return isNullOrEmpty(string) || string.trim().isEmpty();
    }

    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }

    public static boolean isNullOrEmpty(Collection<?> items) {
        return items == null || items.isEmpty();
    }
}
