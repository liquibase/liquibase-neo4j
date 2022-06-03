package liquibase.ext.neo4j.statement;

import liquibase.database.PreparedStatementFactory;
import liquibase.exception.DatabaseException;
import liquibase.statement.ExecutablePreparedStatement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class CypherPreparedStatement implements ExecutablePreparedStatement {

    private final String cypher;
    private final List<Object> parameters;

    public CypherPreparedStatement(String cypher, List<Object> parameters) {
        this.cypher = cypher;
        this.parameters = parameters;
    }

    @Override
    public void execute(PreparedStatementFactory factory) throws DatabaseException {
        PreparedStatement preparedStatement = factory.create(cypher);
        for (int i = 0; i < parameters.size(); i++) {
            setParameter(preparedStatement, i);
        }
    }

    @Override
    public boolean skipOnUnsupported() {
        return false;
    }

    @Override
    public boolean continueOnError() {
        return false;
    }

    public String getCypher() {
        return cypher;
    }

    public List<Object> getParameters() {
        return parameters;
    }

    private void setParameter(PreparedStatement preparedStatement, int i) throws DatabaseException {
        try {
            preparedStatement.setObject(i, parameters.get(i));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
