package liquibase.ext.neo4j.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.core.InsertDataChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static liquibase.ext.neo4j.change.ColumnMapper.mapValue;

@DatabaseChange(name = "insert", priority = ChangeMetaData.PRIORITY_DATABASE,
        description = "Inserts a node to the graph")
public class InsertNodeChange extends InsertDataChange {

    private String labelName;

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase || super.supports(database);
    }

    @Override
    public ValidationErrors validate(Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return super.validate(database);
        }
        ValidationErrors errors = new ValidationErrors(this);
        if (Sequences.isNullOrEmpty(labelName)) {
            errors.addError("label name for insert must be specified and not blank");
        }
        return errors;
    }

    @Override
    public void setTableName(String tableName) {
        super.setTableName(tableName);
        this.labelName = tableName;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        if (!(database instanceof Neo4jDatabase)) {
            return super.generateStatements(database);
        }
        return new SqlStatement[]{
                new RawParameterizedSqlStatement(
                        String.format("CREATE (node:`%s`) SET node = $0", this.getTableName()),
                        propertyMap(this.getColumns())
                )
        };
    }

    @Override
    public Set<String> getSerializableFields() {
        Set<String> fields = new HashSet<>(super.getSerializableFields());
        fields.remove("tableName");
        return fields;
    }

    public void setLabelName(String labelName) {
        this.setTableName(labelName);
    }

    public String getLabelName() {
        return labelName;
    }

    private Map<String, Object> propertyMap(List<ColumnConfig> columns) {
        Map<String, Object> result = new HashMap<>(columns.size());
        for (ColumnConfig column : columns) {
            result.put(column.getName(), mapValue(column));
        }
        return result;
    }
}
