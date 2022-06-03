package liquibase.ext.neo4j.change.refactoring;

import java.util.List;

public enum PropertyMergeStrategy {
    KEEP_ALL, KEEP_FIRST, KEEP_LAST;

    public Object apply(List<Object> values) {
        switch (this) {
            case KEEP_ALL:
                return values;
            case KEEP_FIRST:
                return values.isEmpty() ? null : values.get(0);
            case KEEP_LAST:
                return values.isEmpty() ? null : values.get(values.size() - 1);
        }
        throw new IllegalStateException(String.format("Unknown enum value for %s class: %s", this.getClass(), this));
    }
}
