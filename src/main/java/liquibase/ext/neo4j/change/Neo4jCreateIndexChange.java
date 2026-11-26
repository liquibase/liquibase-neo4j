package liquibase.ext.neo4j.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.AddColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.core.CreateIndexChange;
import liquibase.database.Database;
import liquibase.exception.RollbackImpossibleException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@DatabaseChange(name = "createIndex", priority = ChangeMetaData.PRIORITY_DATABASE,
        description = "Creates a range index on a node label (labelName) or a relationship type (relationshipType).\n" +
                "The indexed properties are defined with columns. Several columns define a composite index.")
public class Neo4jCreateIndexChange extends CreateIndexChange {

    private String labelName;

    private String relationshipType;

    private Boolean ifNotExists;

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return super.validate(database);
        }
        ValidationErrors errors = new ValidationErrors(this);
        boolean hasLabel = !Sequences.isNullOrBlank(labelName);
        boolean hasType = !Sequences.isNullOrBlank(relationshipType);
        if (hasLabel == hasType) {
            errors.addError("exactly one of labelName or relationshipType must be specified and not blank");
        }
        List<AddColumnConfig> columns = getColumns();
        if (columns == null || columns.isEmpty()) {
            errors.addError("at least one column (indexed property) must be specified");
        } else if (columns.stream().anyMatch(c -> Sequences.isNullOrBlank(c.getName()))) {
            errors.addError("column names must be specified and not blank");
        }
        if (getIndexName() != null && Sequences.isNullOrBlank(getIndexName())) {
            errors.addError("indexName, if set, must not be blank");
        }
        return errors;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return super.generateStatements(database);
        }
        StringBuilder cypher = new StringBuilder("CREATE INDEX");
        if (getIndexName() != null) {
            cypher.append(' ').append(escape(getIndexName()));
        }
        if (Boolean.TRUE.equals(ifNotExists)) {
            cypher.append(" IF NOT EXISTS");
        }
        if (!Sequences.isNullOrBlank(labelName)) {
            cypher.append(" FOR (n:").append(escape(labelName)).append(")");
        } else {
            cypher.append(" FOR ()-[n:").append(escape(relationshipType)).append("]-()");
        }
        String properties = getColumns().stream()
                .map(column -> "n." + escape(column.getName()))
                .collect(Collectors.joining(", "));
        cypher.append(" ON (").append(properties).append(")");
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher.toString())};
    }

    @Override
    public boolean supportsRollback(Database database) {
        return database instanceof Neo4jDatabase && getIndexName() != null;
    }

    @Override
    public SqlStatement[] generateRollbackStatements(Database database) throws RollbackImpossibleException {
        if (!(database instanceof Neo4jDatabase)) {
            return super.generateRollbackStatements(database);
        }
        if (getIndexName() == null) {
            throw new RollbackImpossibleException("indexName must be set to roll back createIndex");
        }
        return new SqlStatement[]{
                new RawParameterizedSqlStatement(String.format("DROP INDEX %s IF EXISTS", escape(getIndexName())))
        };
    }

    @Override
    public String getConfirmationMessage() {
        return getIndexName() == null ? "Index created" : String.format("Index %s created", getIndexName());
    }

    @Override
    public Set<String> getSerializableFields() {
        Set<String> fields = new HashSet<>(super.getSerializableFields());
        fields.remove("tableName");
        fields.add("labelName");
        fields.add("relationshipType");
        fields.add("ifNotExists");
        return fields;
    }

    @Override
    public void setTableName(String tableName) {
        super.setTableName(tableName);
        this.labelName = tableName;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.setTableName(labelName);
    }

    public String getRelationshipType() {
        return relationshipType;
    }

    public void setRelationshipType(String relationshipType) {
        this.relationshipType = relationshipType;
    }

    public Boolean getIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(Boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    private static String escape(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
