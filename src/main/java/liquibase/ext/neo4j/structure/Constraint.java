package liquibase.ext.neo4j.structure;

import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.CatalogLevelObject;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

import java.util.Map;

public class Constraint extends AbstractDatabaseObject implements CatalogLevelObject {

    private Catalog catalog;
    private DatabaseObject container;
    private String name;

    public Constraint() {
    }

    public Constraint(Label label, Map<String, ?> row) {
        this(label.getCatalog(), label, row);
    }

    public Constraint(Type type, Map<String, ?> row) {
        this(type.getCatalog(), type, row);
    }

    public Constraint(GraphType graphType, Map<String, ?> row) {
        this(graphType.getCatalog(), graphType, row);
    }

    public Constraint(Catalog catalog, Map<String, ?> row) {
        this(catalog, null, row);
    }

    private Constraint(Catalog catalog, DatabaseObject container, Map<String, ?> row) {
        this.catalog = catalog;
        this.container = container;
        this.name = (String) row.get("name");
        setAttribute("type", row.get("type"));
        setAttribute("entityType", row.get("entityType"));
        setAttribute("labelsOrTypes", row.get("labelsOrTypes"));
        setAttribute("properties", row.get("properties"));
        setAttribute("enforcedLabel", row.get("enforcedLabel"));
        setAttribute("ownedIndex", firstNonNull(row.get("ownedIndex"), row.get("ownedIndexId")));
        setAttribute("propertyType", row.get("propertyType"));
        setAttribute("classification", row.get("classification"));
        setAttribute("indexProvider", getOption(row, "indexProvider"));
        setAttribute("indexConfig", getOption(row, "indexConfig"));
        setAttribute("options", row.get("options"));
        setAttribute("createStatement", row.get("createStatement"));
    }

    @Override
    public DatabaseObject[] getContainingObjects() {
        if (container == null) {
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

    private static Object getOption(Map<String, ?> row, String option) {
        Object options = row.get("options");
        if (!(options instanceof Map<?, ?>)) {
            return null;
        }
        return ((Map<?, ?>) options).get(option);
    }

    private static Object firstNonNull(Object first, Object second) {
        return first == null ? second : first;
    }
}
