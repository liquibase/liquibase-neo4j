package liquibase.ext.neo4j.generator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import liquibase.change.ChangeMetaData;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.InsertStatement;

import java.util.Map;

public class InsertGenerator extends AbstractSqlGenerator<InsertStatement> {

    @Override
    public ValidationErrors validate(InsertStatement statement, Database database, SqlGeneratorChain<InsertStatement> sqlGeneratorChain) {
        return null;
    }

    @Override
    public Sql[] generateSql(InsertStatement statement, Database database, SqlGeneratorChain<InsertStatement> sqlGeneratorChain) {
        String label = statement.getTableName();
        Map<String, Object> properties = statement.getColumnValues();
        String propertiesJson = serializeAsJson(properties);
        String cypher = String.format("CREATE (node:`%s`) SET node += %s", label, propertiesJson);
        return new Sql[]{new UnparsedSql(cypher)};
    }

    private String serializeAsJson(Map<String, Object> properties) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(JsonWriteFeature.QUOTE_FIELD_NAMES.mappedFeature(), false);
            return mapper.writeValueAsString(properties);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getPriority() {
        return ChangeMetaData.PRIORITY_DEFAULT;
    }
}
