package liquibase.ext.neo4j.change.refactoring;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public final class PropertyMergePolicy {

    private final Pattern propertyMatcher;
    private final PropertyMergeStrategy strategy;

    private PropertyMergePolicy(Pattern propertyMatcher, PropertyMergeStrategy strategy) {

        this.propertyMatcher = propertyMatcher;
        this.strategy = strategy;
    }

    public static PropertyMergePolicy of(Pattern property, PropertyMergeStrategy strategy) {
        return new PropertyMergePolicy(property, strategy);
    }

    public Pattern property() {
        return propertyMatcher;
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
        return Objects.equals(propertyMatcher, that.propertyMatcher) && strategy == that.strategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyMatcher, strategy);
    }

    @Override
    public String toString() {
        return String.format("%s on %s", strategy, propertyMatcher);
    }
}
