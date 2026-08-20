package liquibase.ext.neo4j.structure;

import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.CatalogLevelObject;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

import java.util.Map;

public class Index extends AbstractDatabaseObject implements CatalogLevelObject {

    private Catalog catalog;
    private DatabaseObject container;
    private String name;

    public Index() {
    }

    public Index(Label label, Map<String, ?> row) {
        this(label.getCatalog(), label, row);
    }

    public Index(Type type, Map<String, ?> row) {
        this(type.getCatalog(), type, row);
    }

    public Index(GraphType graphType, Map<String, ?> row) {
        this(graphType.getCatalog(), graphType, row);
    }

    public Index(Catalog catalog, Map<String, ?> row) {
        this(catalog, catalog, row);
    }

    private Index(Catalog catalog, DatabaseObject container, Map<String, ?> row) {
        this.catalog = catalog;
        this.container = container;
        this.name = (String) row.get("name");
        setAttribute("type", row.get("type"));
        setAttribute("entityType", row.get("entityType"));
        setAttribute("labelsOrTypes", row.get("labelsOrTypes"));
        setAttribute("properties", row.get("properties"));
        setAttribute("indexProvider", row.get("indexProvider"));
        setAttribute("indexConfig", getIndexConfig(row));
        setAttribute("uniqueness", row.get("uniqueness"));
        setAttribute("owningConstraint", row.get("owningConstraint"));
        setAttribute("options", row.get("options"));
        setAttribute("createStatement", row.get("createStatement"));
    }

    @Override
    public DatabaseObject[] getContainingObjects() {
        if (container == null || container instanceof Catalog) {
            return null;
        }
        return new DatabaseObject[] { container };
    }

    @Override
    public DatabaseObject setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public Schema getSchema() {
        return null;
    }

    private static Object getIndexConfig(Map<String, ?> row) {
        Object options = row.get("options");
        if (!(options instanceof Map<?, ?>)) {
            return null;
        }
        return ((Map<?, ?>) options).get("indexConfig");
    }
}
