package liquibase.ext.neo4j.structure;

import liquibase.structure.AbstractDatabaseObject;
import liquibase.structure.CatalogLevelObject;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Schema;

public class GraphType extends AbstractDatabaseObject implements CatalogLevelObject {

    private Catalog catalog;

    private String name;

    public GraphType() {
    }

    public GraphType(Catalog catalog, String specification) {
        this.catalog = catalog;
        this.name = GraphTypeTruncator.truncate(specification);
        setAttribute("specification", specification);
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
