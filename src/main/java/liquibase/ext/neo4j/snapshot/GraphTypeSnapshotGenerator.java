package liquibase.ext.neo4j.snapshot;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.structure.GraphType;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;

import java.util.Optional;

public class GraphTypeSnapshotGenerator implements SnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return PRIORITY_NONE;
        }
        if (GraphType.class.isAssignableFrom(objectType)) {
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
        if (!(database instanceof Neo4jDatabase)) {
            return chain.snapshot(example, snapshot);
        }
        if (!snapshot.getSnapshotControl().shouldInclude(GraphType.class)) {
            return chain.snapshot(example, snapshot);
        }
        if (example instanceof GraphType) {
            return example;
        }
        if (!(example instanceof Catalog catalog)) {
            return chain.snapshot(example, snapshot);
        }

        retrieveGraphType((Neo4jDatabase) database, catalog)
                .ifPresent(catalog::addDatabaseObject);
        return example;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends DatabaseObject>[] addsTo() {
        return new Class[] {Catalog.class};
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return null;
    }

    private static Optional<GraphType> retrieveGraphType(Neo4jDatabase neo4j, Catalog catalog) throws DatabaseException {
        return GraphTypeSupport.load(catalog, neo4j);
    }

}
