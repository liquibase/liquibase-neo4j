package liquibase.ext.neo4j.structure;

import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.CatalogLevelObject;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

import java.util.LinkedHashSet;
import java.util.Set;

public class Label extends AbstractDatabaseObject implements CatalogLevelObject {

    private Catalog catalog;
    private String database;

    public Label() {
    }

    public Label(Catalog catalog, String value) {
        this.catalog = catalog;
        this.database = catalog.getName();
        this.setAttribute("value", value);
    }

    @Override
    public DatabaseObject[] getContainingObjects() {
        return null;
    }

    @Override
    public String getName() {
        return this.getAttribute("value", String.class);
    }

    @Override
    public DatabaseObject setName(String name) {
        this.setAttribute("value", name);
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public void addIndex(NodeIndex nodeIndex) {
        Set<DatabaseObject> objects = getAttribute("indices", new LinkedHashSet<>());
        objects.add(nodeIndex);
        setAttribute("indices", objects);
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
