package liquibase.ext.neo4j.change.refactoring;

import java.util.Objects;

public final class RelationshipExtraction {
    private final String relationshipType;
    private final RelationshipDirection relationshipDirection;
    private final boolean merge;

    public static RelationshipExtraction merging(String relationshipType, RelationshipDirection relationshipDirection) {
        return new RelationshipExtraction(relationshipType, relationshipDirection, true);
    }

    public static RelationshipExtraction creating(String relationshipType, RelationshipDirection relationshipDirection) {
        return new RelationshipExtraction(relationshipType, relationshipDirection, false);
    }

    private RelationshipExtraction(String relationshipType, RelationshipDirection relationshipDirection, boolean merge) {
        this.relationshipType = relationshipType;
        this.relationshipDirection = relationshipDirection;
        this.merge = merge;
    }

    public String relationshipType() {
        return relationshipType;
    }

    public RelationshipDirection relationshipDirection() {
        return relationshipDirection;
    }

    public boolean isMerge() {
        return merge;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationshipExtraction that = (RelationshipExtraction) o;
        return merge == that.merge && Objects.equals(relationshipType, that.relationshipType) && relationshipDirection == that.relationshipDirection;
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationshipType, relationshipDirection, merge);
    }

    @Override
    public String toString() {
        return "RelationshipExtraction{" +
                "relationshipType='" + relationshipType + '\'' +
                ", relationshipDirection=" + relationshipDirection +
                ", merge=" + merge +
                '}';
    }
}
