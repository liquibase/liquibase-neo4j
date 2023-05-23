package liquibase.ext.neo4j.snapshot;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.KernelVersion;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.structure.EntityType;
import liquibase.ext.neo4j.structure.IndexType;
import liquibase.ext.neo4j.structure.Label;
import liquibase.ext.neo4j.structure.NodeBTreeIndex;
import liquibase.logging.Logger;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;

import java.util.List;
import java.util.stream.Collectors;

public class NodeBTreeIndexSnapshotGeneratorNeo4j implements SnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return PRIORITY_NONE;
        }
        if (NodeBTreeIndex.class.isAssignableFrom(objectType)) {
            return PRIORITY_DEFAULT;
        }
        if (Label.class.isAssignableFrom(objectType)) {
            return PRIORITY_ADDITIONAL;
        }
        return PRIORITY_NONE;
    }

    @Override
    public <T extends DatabaseObject> T snapshot(T example, DatabaseSnapshot snapshot, SnapshotGeneratorChain chain) throws DatabaseException, InvalidExampleException {
        Database database = snapshot.getDatabase();
        if (!(database instanceof Neo4jDatabase)) {
            return chain.snapshot(example, snapshot);
        }
        if (!snapshot.getSnapshotControl().shouldInclude(NodeBTreeIndex.class)) {
            return chain.snapshot(example, snapshot);
        }
        if (example instanceof NodeBTreeIndex) {
            return example;
        }
        if (!(example instanceof Label)) {
            return chain.snapshot(example, snapshot);
        }
        Neo4jDatabase neo4j = (Neo4jDatabase) database;
        if (neo4j.getKernelVersion().compareTo(KernelVersion.V4_4_0) < 0) {
            Logger log = Scope.getCurrentScope().getLog(getClass());
            log.warning(String.format("Ignoring snapshot request as Neo4j version is too old (%s): expected at least 4.4",
                    neo4j.getKernelVersion()));
            return chain.snapshot(example, snapshot);
        }

        Label label = (Label) example;
        retrieveIndices(neo4j, label).forEach(label::addIndex);
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

    @SuppressWarnings("unchecked")
    private static List<NodeBTreeIndex> retrieveIndices(Neo4jDatabase neo4j, Label label) throws DatabaseException {
        // TODO: add fields like options / createStatement?
        String cypher = "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties " +
                        "WHERE entityType = $1 AND type = $2 AND $3 IN labelsOrTypes " +
                        "RETURN name, labelsOrTypes AS labels, properties";
        try {
            return neo4j.run(new RawParameterizedSqlStatement(cypher,
                            EntityType.NODE.name(),
                            IndexType.BTREE.name(),
                            label.getName()
                    ))
                    .stream()
                    .map(row -> new NodeBTreeIndex(
                            label.getCatalog(),
                            (String) row.get("name"),
                            (List<String>) row.get("labels"),
                            (List<String>) row.get("properties")))
                    .collect(Collectors.toList());
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not retrieve BTREE node indexes", e);
        }
    }
}
