package liquibase.ext.neo4j.snapshot;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.KernelVersion;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.structure.Index;
import liquibase.logging.Logger;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;

import java.util.List;
import java.util.stream.Collectors;

import static liquibase.ext.neo4j.database.KernelVersion.V4_4_0;
import static liquibase.ext.neo4j.database.KernelVersion.V5_0_0;

public class IndexSnapshotGenerator implements SnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return PRIORITY_NONE;
        }
        if (Index.class.isAssignableFrom(objectType)) {
            return PRIORITY_DEFAULT;
        }
        if (Catalog.class.isAssignableFrom(objectType)) {
            return PRIORITY_ADDITIONAL;
        }
        return PRIORITY_NONE;
    }

    @Override
    public <T extends DatabaseObject> T snapshot(T example, DatabaseSnapshot snapshot, SnapshotGeneratorChain chain) throws DatabaseException, InvalidExampleException {
        Database database = snapshot.getDatabase();
        if (!(database instanceof Neo4jDatabase neo4j)) {
            return chain.snapshot(example, snapshot);
        }
        if (!snapshot.getSnapshotControl().shouldInclude(Index.class)) {
            return chain.snapshot(example, snapshot);
        }
        if (example instanceof Index) {
            return example;
        }
        if (!(example instanceof Catalog catalog)) {
            return chain.snapshot(example, snapshot);
        }

        if (!supportsShowIndexes(neo4j)) {
            return chain.snapshot(example, snapshot);
        }

        retrieveIndexes(neo4j, catalog).forEach(catalog::addDatabaseObject);
        return example;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends DatabaseObject>[] addsTo() {
        return new Class[] { Catalog.class };
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return null;
    }

    private static boolean supportsShowIndexes(Neo4jDatabase neo4j) {
        KernelVersion version = neo4j.getKernelVersion();
        if (version.compareTo(V4_4_0) >= 0) {
            return true;
        }
        Logger log = Scope.getCurrentScope().getLog(IndexSnapshotGenerator.class);
        log.warning(String.format("Ignoring snapshot request as Neo4j version is too old (%s): expected at least 4.4",
                version));
        return false;
    }

    private static List<Index> retrieveIndexes(Neo4jDatabase neo4j, Catalog catalog) throws DatabaseException {
        var predicate = "owningConstraint IS NULL";
        if (neo4j.getKernelVersion().compareTo(V5_0_0) < 0) {
            predicate = "uniqueness = 'NONUNIQUE'";
        }
        try {
            return neo4j.run(catalog, new RawParameterizedSqlStatement("SHOW INDEXES YIELD * WHERE %s RETURN * ORDER BY entityType, type, name ASC".formatted(predicate), new Object[]{}))
                    .stream()
                    .map(row -> new Index(catalog, row))
                    .collect(Collectors.toList());
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not retrieve Neo4j indexes", e);
        }
    }

}
