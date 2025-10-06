package liquibase.ext.neo4j.structure;

import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.CatalogLevelObject;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

public class Type extends AbstractDatabaseObject implements CatalogLevelObject {

    private Catalog catalog;
    private String database;
    private String value;

    public Type() {
    }

    public Type(Catalog catalog, String value) {
        this.catalog = catalog;
        this.database = catalog.getName();
        this.value = value;
    }

    @Override
    public DatabaseObject[] getContainingObjects() {
        return null;
    }

    @Override
    public String getName() {
        return value;
    }

    @Override
    public DatabaseObject setName(String name) {
        this.value = name;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
