package liquibase.ext.neo4j.snapshot;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.structure.Type;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.snapshot.jvm.CatalogSnapshotGenerator;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static liquibase.ext.neo4j.database.KernelVersion.V4_4_0;
import static liquibase.ext.neo4j.database.KernelVersion.V5_0_0;

public class TypeSnapshotGenerator implements SnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return PRIORITY_NONE;
        }
        if (Type.class.isAssignableFrom(objectType)) {
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
        if (!snapshot.getSnapshotControl().shouldInclude(Type.class)) {
            return chain.snapshot(example, snapshot);
        }
        if (example instanceof Type) {
            return example;
        }
        if (!(example instanceof Catalog catalog)) {
            return chain.snapshot(example, snapshot);
        }
        if (neo4j.getKernelVersion().compareTo(V4_4_0) < 0) {
            return chain.snapshot(example, snapshot);
        }
        retrieveTypes(neo4j, catalog)
                .forEach(catalog::addDatabaseObject);
        return example;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends DatabaseObject>[] addsTo() {
        return new Class[]{Catalog.class};
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[] {CatalogSnapshotGenerator.class};
    }

    private static List<Type> retrieveTypes(Neo4jDatabase database, Catalog catalog) throws DatabaseException {
        try {
            return Stream.concat(
                            retrieveGraphTypes(database, catalog).stream(),
                            schemaTypes(database, catalog).stream()
                    )
                    .distinct()
                    .sorted()
                    .map(type -> new Type(catalog, type))
                    .collect(Collectors.toList());
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not retrieve relationship types during type snapshot", e);
        }
    }

    private static List<String> retrieveGraphTypes(Neo4jDatabase database, Catalog catalog) throws LiquibaseException {
        return database.run(catalog, new RawParameterizedSqlStatement("MATCH ()-[r]->() RETURN DISTINCT type(r) AS type ORDER BY type"))
                .stream()
                .map(row -> (String) row.get("type"))
                .collect(Collectors.toList());
    }

    private static Set<String> schemaTypes(Neo4jDatabase database, Catalog catalog) throws LiquibaseException {
        return Stream.concat(
                        indexTypes(database, catalog),
                        constraintTypes(database, catalog)
                )
                .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    private static Stream<String> indexTypes(Neo4jDatabase neo4j, Catalog catalog) throws LiquibaseException {
        var predicate = "owningConstraint IS NULL";
        if (neo4j.getKernelVersion().compareTo(V5_0_0) < 0) {
            predicate = "uniqueness = 'NONUNIQUE'";
        }
        return neo4j.run(catalog, new RawParameterizedSqlStatement("SHOW INDEXES YIELD * " +
                                                                      "WHERE entityType = 'RELATIONSHIP' AND labelsOrTypes IS NOT NULL AND %s ".formatted(predicate) +
                                                                      "RETURN labelsOrTypes AS types"))
                .stream()
                .flatMap(row -> ((List<String>) row.get("types")).stream());
    }

    @SuppressWarnings("unchecked")
    private static Stream<String> constraintTypes(Neo4jDatabase neo4j, Catalog catalog) throws LiquibaseException {
        return neo4j.run(catalog, new RawParameterizedSqlStatement("SHOW CONSTRAINTS YIELD entityType, labelsOrTypes " +
                                                                      "WHERE entityType = 'RELATIONSHIP' AND labelsOrTypes IS NOT NULL " +
                                                                      "RETURN labelsOrTypes AS types"))
                .stream()
                .flatMap(row -> ((List<String>) row.get("types")).stream());
    }
}
