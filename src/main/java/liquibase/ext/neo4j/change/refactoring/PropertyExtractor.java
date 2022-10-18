package liquibase.ext.neo4j.change.refactoring;

import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

public class PropertyExtractor {

    public static final String NODE_VARIABLE = "_____n_____";

    public SqlStatement[] extract(PropertyExtraction extraction) {
        SqlStatement extractNodeStatement = extraction.extractedRelationship()
                .map(targetRelationship -> generateConnectedNode(extraction, targetRelationship))
                .orElse(generateDisconnectedNode(extraction));
        return new SqlStatement[]{extractNodeStatement};
    }

    private static SqlStatement generateDisconnectedNode(PropertyExtraction extraction) {
        MatchPattern matchPattern = extraction.matchPattern();
        NodeExtraction node = extraction.extractedNode();
        return new RawSqlStatement(String.format("MATCH %1$s WITH %2$s " +
                        "%7$s (%6$s:`%3$s` {`%5$s`: %2$s.`%4$s`}) " +
                        "REMOVE %2$s.`%4$s` ",
                matchPattern.cypherFragment(),
                matchPattern.outputVariable(),
                node.label(),
                node.sourcePropertyName(),
                node.targetPropertyName(),
                NODE_VARIABLE,
                node.isMerge() ? "MERGE" : "CREATE"));
    }

    private static SqlStatement generateConnectedNode(PropertyExtraction extraction, RelationshipExtraction relationship) {
        MatchPattern matchPattern = extraction.matchPattern();
        NodeExtraction node = extraction.extractedNode();
        RelationshipDirection relationshipDirection = relationship.relationshipDirection();
        return new RawSqlStatement(String.format("MATCH %1$s WITH %2$s " +
                        "MERGE (%9$s:`%3$s` {`%5$s`: %2$s.`%4$s`}) " +
                        "%10$s (%2$s)%7$s-[:`%6$s`]-%8$s(%9$s) " +
                        "REMOVE %2$s.`%4$s` ",
                matchPattern.cypherFragment(),
                matchPattern.outputVariable(),
                node.label(),
                node.sourcePropertyName(),
                node.targetPropertyName(),
                relationship.relationshipType(),
                relationshipDirection == RelationshipDirection.INCOMING ? "<" : "",
                relationshipDirection == RelationshipDirection.OUTGOING ? ">" : "",
                NODE_VARIABLE,
                relationship.isMerge() ? "MERGE" : "CREATE"));
    }
}
