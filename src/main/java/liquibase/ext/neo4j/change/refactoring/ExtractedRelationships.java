package liquibase.ext.neo4j.change.refactoring;

import liquibase.serializer.AbstractLiquibaseSerializable;

import java.util.Objects;

public class ExtractedRelationships extends AbstractLiquibaseSerializable {
    private String withType;
    private RelationshipDirection withDirection;
    private boolean merge;

    public String getWithType() {
        return withType;
    }

    public void setWithType(String withType) {
        this.withType = withType;
    }

    public RelationshipDirection getWithDirection() {
        return withDirection;
    }

    public void setWithDirection(RelationshipDirection withDirection) {
        this.withDirection = withDirection;
    }

    public boolean isMerge() {
        return merge;
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

    @Override
    public String getSerializedObjectName() {
        return "extractedRelationships";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtractedRelationships that = (ExtractedRelationships) o;
        return merge == that.merge && Objects.equals(withType, that.withType) && withDirection == that.withDirection;
    }

    @Override
    public int hashCode() {
        return Objects.hash(withType, withDirection, merge);
    }

    @Override
    public String toString() {
        return "ExtractedRelationships{" +
                "withType='" + withType + '\'' +
                ", withDirection=" + withDirection +
                ", merge=" + merge +
                '}';
    }
}
