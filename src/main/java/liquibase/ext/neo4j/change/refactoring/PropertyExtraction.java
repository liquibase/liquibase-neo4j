package liquibase.ext.neo4j.change.refactoring;

import java.util.Optional;

public class PropertyExtraction {
    private final MatchPattern matchPattern;
    private final NodeExtraction nodeExtraction;
    private final RelationshipExtraction relationshipExtraction;

    public static PropertyExtraction of(MatchPattern matchPattern, NodeExtraction nodeExtraction) {
        return new PropertyExtraction(matchPattern, nodeExtraction);
    }

    public static PropertyExtraction of(MatchPattern matchPattern, NodeExtraction nodeExtraction, RelationshipExtraction relationshipPattern) {
        return new PropertyExtraction(matchPattern, nodeExtraction, relationshipPattern);
    }

    private PropertyExtraction(MatchPattern matchPattern, NodeExtraction nodeExtraction) {
        this(matchPattern, nodeExtraction, null);
    }

    private PropertyExtraction(MatchPattern matchPattern, NodeExtraction nodeExtraction, RelationshipExtraction relationshipPattern) {
        this.matchPattern = matchPattern;
        this.nodeExtraction = nodeExtraction;
        this.relationshipExtraction = relationshipPattern;
    }

    public MatchPattern matchPattern() {
        return matchPattern;
    }

    public NodeExtraction extractedNode() {
        return nodeExtraction;
    }

    public Optional<RelationshipExtraction> extractedRelationship() {
        return Optional.ofNullable(relationshipExtraction);
    }
}
