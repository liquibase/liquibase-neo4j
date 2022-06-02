package liquibase.ext.neo4j.change;

import liquibase.serializer.AbstractLiquibaseSerializable;

public class MergeNodeProperty extends AbstractLiquibaseSerializable {

    private String name;

    private MergePolicy policy;

    @Override
    public String getSerializedObjectName() {
        return "nodeProperty";
    }

    @Override
    public String getSerializedObjectNamespace() {
        // TODO: http://www.liquibase.org/xml/ns/dbchangelog-ext instead?
        return "neo4j";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public MergePolicy getPolicy() {
        return policy;
    }

    public void setPolicy(MergePolicy policy) {
        this.policy = policy;
    }
}
