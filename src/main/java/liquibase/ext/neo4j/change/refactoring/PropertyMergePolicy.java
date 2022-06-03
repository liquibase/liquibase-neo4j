package liquibase.ext.neo4j.change.refactoring;

import java.util.List;
import java.util.Objects;

public final class PropertyMergePolicy {

    private final String property;
    private final PropertyMergeStrategy strategy;

    private PropertyMergePolicy(String property, PropertyMergeStrategy strategy) {

        this.property = property;
        this.strategy = strategy;
    }

    public static PropertyMergePolicy of(String property, PropertyMergeStrategy strategy) {
        return new PropertyMergePolicy(property, strategy);
    }

    public String property() {
        return property;
    }

    public PropertyMergeStrategy strategy() {
        return strategy;
    }

    public Object apply(List<Object> values) {
        return strategy.apply(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertyMergePolicy that = (PropertyMergePolicy) o;
        return Objects.equals(property, that.property) && strategy == that.strategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(property, strategy);
    }

    @Override
    public String toString() {
        return String.format("%s on %s", strategy, property);
    }
}
