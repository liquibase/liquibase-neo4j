package liquibase.ext.neo4j.database.jdbc;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Collections.emptyList;

class QueryString {
    private final Map<String, List<String>> values;

    public QueryString(Map<String, List<String>> values) {
        this.values = values;
    }

    public <T> T getSingleOrDefault(String key, Function<String, T> mapper, T defaultValue) {
        List<String> matches = values.getOrDefault(key, emptyList());
        int valueCount = matches.size();
        if (valueCount == 0) {
            return defaultValue;
        }
        if (valueCount > 1) {
            throw new RuntimeException(String.format("expected 1 value for key %s, found: %d", key, valueCount));
        }
        try {
            return mapper.apply(matches.iterator().next());
        } catch (Exception e) {
            throw new RuntimeException("URL configuration error", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryString that = (QueryString) o;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    public void forEach(BiConsumer<String, List<String>> action) {
        values.forEach(action);
    }

    public int size() {
        return values.size();
    }
}
