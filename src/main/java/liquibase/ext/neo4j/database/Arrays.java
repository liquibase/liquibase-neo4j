package liquibase.ext.neo4j.database;

import java.lang.reflect.Array;

public class Arrays {

    public static <T> T[] prepend(T head, T[] tail, Class<T> type) {
        @SuppressWarnings("unchecked")
        T[] result = (T[]) Array.newInstance(type, 1 + tail.length);
        result[0] = head;
        System.arraycopy(tail, 0, result, 1, tail.length);
        return result;
    }
}
