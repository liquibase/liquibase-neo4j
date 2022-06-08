package liquibase.ext.neo4j.change.refactoring;

import liquibase.serializer.AbstractLiquibaseSerializable;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class PropertyMergePolicy extends AbstractLiquibaseSerializable {

    private String nameMatcher;

    private PropertyMergeStrategy mergeStrategy;
    private Pattern pattern;

    public static PropertyMergePolicy of(String nameMatcher, PropertyMergeStrategy strategy) {
        PropertyMergePolicy policy = new PropertyMergePolicy();
        policy.setNameMatcher(nameMatcher);
        policy.setMergeStrategy(strategy);
        return policy;
    }

    public String getNameMatcher() {
        return nameMatcher;
    }

    public void setNameMatcher(String nameMatcher) {
        this.nameMatcher = nameMatcher;
        this.pattern = Pattern.compile(nameMatcher);
    }

    public PropertyMergeStrategy getMergeStrategy() {
        return mergeStrategy;
    }

    public void setMergeStrategy(PropertyMergeStrategy mergeStrategy) {
        this.mergeStrategy = mergeStrategy;
    }

    public Object apply(List<Object> values) {
        return mergeStrategy.apply(values);
    }

    public Pattern getPropertyNamePattern() {
        return pattern;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertyMergePolicy that = (PropertyMergePolicy) o;
        return Objects.equals(nameMatcher, that.nameMatcher) && mergeStrategy == that.mergeStrategy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameMatcher, mergeStrategy);
    }

    @Override
    public String toString() {
        return String.format("%s on %s", mergeStrategy, nameMatcher);
    }

    @Override
    public String getSerializedObjectName() {
        return "propertyPolicy";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }
}
