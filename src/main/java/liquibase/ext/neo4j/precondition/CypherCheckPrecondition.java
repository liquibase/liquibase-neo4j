package liquibase.ext.neo4j.precondition;

import liquibase.precondition.core.SqlPrecondition;

import java.util.HashSet;
import java.util.Set;

public class CypherCheckPrecondition extends SqlPrecondition {

    @Override
    public String getName() {
        return "cypherCheck";
    }

    public void setCypher(String cypher) {
        setSql(cypher);
    }

    public String getCypher() {
        return getSql();
    }

    @Override
    public Set<String> getSerializableFields() {
        Set<String> fields = new HashSet<>(super.getSerializableFields());
        fields.add("cypher");
        return fields;
    }

    @Override
    protected Class<?> getSerializableFieldDataTypeClass(String field) {
        if (field.equals("cypher")) {
            return String.class;
        }
        return super.getSerializableFieldDataTypeClass(field);
    }
}
