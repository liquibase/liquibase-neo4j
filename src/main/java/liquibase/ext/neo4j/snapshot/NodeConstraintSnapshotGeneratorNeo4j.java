package liquibase.ext.neo4j.snapshot;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.KernelVersion;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.structure.EntityType;
import liquibase.ext.neo4j.structure.Label;
import liquibase.ext.neo4j.structure.NodeConstraint;
import liquibase.logging.Logger;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeConstraintSnapshotGeneratorNeo4j implements SnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return PRIORITY_NONE;
        }
        if (Label.class.isAssignableFrom(objectType)) {
            return PRIORITY_ADDITIONAL;
        }
        if (NodeConstraint.class.isAssignableFrom(objectType)) {
            return PRIORITY_DEFAULT;
        }
        return PRIORITY_NONE;
    }

    @Override
    public <T extends DatabaseObject> T snapshot(T example, DatabaseSnapshot snapshot, SnapshotGeneratorChain chain) throws DatabaseException, InvalidExampleException {
        Database database = snapshot.getDatabase();
        if (!(database instanceof Neo4jDatabase)) {
            return chain.snapshot(example, snapshot);
        }
        if (!snapshot.getSnapshotControl().shouldInclude(NodeConstraint.class)) {
            return chain.snapshot(example, snapshot);
        }
        if (example instanceof NodeConstraint) {
            return example;
        }
        if (!(example instanceof Label)) {
            return chain.snapshot(example, snapshot);
        }
        Neo4jDatabase neo4j = (Neo4jDatabase) database;
        KernelVersion version = neo4j.getKernelVersion();
        if (version.compareTo(KernelVersion.V4_4_0) < 0) {
            Logger log = Scope.getCurrentScope().getLog(getClass());
            log.warning(String.format("Ignoring snapshot request as Neo4j version is too old (%s): expected at least 4.4",
                    version));
            return chain.snapshot(example, snapshot);
        }

        Label label = (Label) example;
        List<NodeConstraint> indices = retrieveConstraints(neo4j, label);
        indices.forEach(label::addConstraint);
        return example;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends DatabaseObject>[] addsTo() {
        return new Class[]{Label.class};
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return null;
    }

    private static List<NodeConstraint> retrieveConstraints(Neo4jDatabase neo4j, Label label) throws DatabaseException {
        String cypher = "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties, options " +
                        "WHERE entityType = $1 AND $2 IN labelsOrTypes " +
                        "RETURN type, name, labelsOrTypes AS labels, properties, options " +
                        "ORDER BY type, name ASC";
        try {
            return neo4j.run(new RawParameterizedSqlStatement(cypher,
                            EntityType.NODE.name(),
                            label.getName()
                    ))
                    .stream()
                    .map(row -> mapConstraint(label, row))
                    .collect(Collectors.toList());
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not retrieve BTREE node indexes", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static NodeConstraint mapConstraint(Label label, Map<String, ?> row) {
        Map<String, Object> options = (Map<String, Object>) row.get("options");
        return new NodeConstraint(
                label,
                (String) row.get("type"),
                (String) row.get("name"),
                ((List<String>) row.get("labels")),
                (List<String>) row.get("properties"),
                (String) (options == null ? null : options.get("indexProvider")),
                (Map<String, Object>) (options == null ? null : options.get("indexConfig")));
    }

}
