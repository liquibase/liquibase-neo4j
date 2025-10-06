package liquibase.ext.neo4j.snapshot;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.structure.Label;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.SnapshotGeneratorChain;
import liquibase.snapshot.jvm.CatalogSnapshotGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;

import java.util.List;
import java.util.stream.Collectors;

public class LabelSnapshotGeneratorNeo4j implements SnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return PRIORITY_NONE;
        }
        if (Label.class.isAssignableFrom(objectType)) {
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
        if (!snapshot.getSnapshotControl().shouldInclude(Label.class)) {
            return chain.snapshot(example, snapshot);
        }
        if (example instanceof Label) {
            return example;
        }
        if (!(example instanceof Catalog)) {
            return chain.snapshot(example, snapshot);
        }
        Catalog catalog = (Catalog) example;
        Neo4jDatabase neo4j = (Neo4jDatabase) snapshot.getDatabase();
        retrieveLabels(neo4j, catalog)
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

    private static List<Label> retrieveLabels(Neo4jDatabase database, Catalog catalog) throws DatabaseException {
        try {
            return database.run(new RawSqlStatement("CALL db.labels() YIELD label RETURN label"))
                    .stream()
                    .map(row -> new Label(catalog, (String) row.get("label")))
                    .collect(Collectors.toList());
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not retrieve node labels during label snapshot", e);
        }
    }
}
