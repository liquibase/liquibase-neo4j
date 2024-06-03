package liquibase.ext.neo4j.structure;

import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.CatalogLevelObject;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

import java.util.List;
import java.util.Map;

public class NodeFullTextIndex extends AbstractDatabaseObject implements CatalogLevelObject, NodeIndex {

    private Catalog catalog;
    private String name;

    public NodeFullTextIndex() {
    }

    public NodeFullTextIndex(Catalog catalog, String name, List<String> labels, List<String> properties, String indexProvider, Map<String, Object> indexConfig) {
        this.catalog = catalog;
        this.name = name;
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
        return catalog;
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
