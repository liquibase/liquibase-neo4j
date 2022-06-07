package liquibase.ext.neo4j.collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Optionals {

    private Optionals() {
        throw new RuntimeException("static");
    }

    public static <T> List<T> optionalToList(Optional<T> optional) {
        return optional.map(Optionals::mutableSingletonList).orElse(new ArrayList<>(0));
    }

    private static <T> List<T> mutableSingletonList(T value) {
        List<T> result = new ArrayList<>(1);
        result.add(value);
        return result;
    }
}
