package liquibase.ext.neo4j.snapshot;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.KernelVersion;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.structure.Constraint;
import liquibase.logging.Logger;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;

import java.util.List;

import static liquibase.ext.neo4j.database.KernelVersion.V4_4_0;

public class ConstraintSnapshotGenerator implements SnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return PRIORITY_NONE;
        }
        if (Constraint.class.isAssignableFrom(objectType)) {
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
        if (!snapshot.getSnapshotControl().shouldInclude(Constraint.class)) {
            return chain.snapshot(example, snapshot);
        }
        if (example instanceof Constraint) {
            return example;
        }
        if (!(example instanceof Catalog catalog)) {
            return chain.snapshot(example, snapshot);
        }
        if (!supportsShowConstraints(neo4j) || GraphTypeSupport.supportsGraphTypes(neo4j)) {
            // graph type is always on and aggregates all constraints
            // ... including constraints created "outside" the graph type
            return chain.snapshot(example, snapshot);
        }

        retrieveConstraints(catalog, neo4j).forEach(catalog::addDatabaseObject);
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

    private static boolean supportsShowConstraints(Neo4jDatabase neo4j) {
        KernelVersion version = neo4j.getKernelVersion();
        if (version.compareTo(V4_4_0) >= 0) {
            return true;
        }
        Logger log = Scope.getCurrentScope().getLog(ConstraintSnapshotGenerator.class);
        log.warning(String.format("Ignoring snapshot request as Neo4j version is too old (%s): expected at least 4.4",
                version));
        return false;
    }

    private static List<Constraint> retrieveConstraints(Catalog catalog, Neo4jDatabase neo4j) throws DatabaseException {
        try {
            return neo4j.run(catalog, new RawParameterizedSqlStatement("SHOW CONSTRAINTS YIELD * RETURN * ORDER BY type, name ASC"))
                    .stream()
                    .map(row -> new Constraint(catalog, row))
                    .toList();
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not retrieve Neo4j constraints", e);
        }
    }

}
