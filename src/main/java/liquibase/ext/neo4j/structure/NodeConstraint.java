package liquibase.ext.neo4j.structure;

import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.CatalogLevelObject;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

import java.util.List;
import java.util.Map;

public class NodeConstraint extends AbstractDatabaseObject implements CatalogLevelObject {

    private Label label;
    private String name;

    public NodeConstraint() {
    }

    public NodeConstraint(Label label, String type, String name, List<String> labels, List<String> properties, String indexProvider, Map<String, Object> indexConfig) {
        this.label = label;
        this.name = name;
        this.setAttribute("type", type);
        this.setAttribute("labels", labels);
        this.setAttribute("properties", properties);
        this.setAttribute("indexProvider", indexProvider);
        this.setAttribute("indexConfig", indexConfig);
    }

    @Override
    public DatabaseObject[] getContainingObjects() {
        return null;
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
        return label.getCatalog();
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
