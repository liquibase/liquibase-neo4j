package liquibase.ext.neo4j.statement;

import liquibase.database.PreparedStatementFactory;
import liquibase.exception.DatabaseException;
import liquibase.executor.jvm.ResultSetExtractor;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.QueryablePreparedStatement;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class ParameterizedCypherStatement implements QueryablePreparedStatement {

    private final String cypher;
    private final List<?> parameters;

    public ParameterizedCypherStatement(String cypher, List<?> parameters) {
        this.cypher = cypher;
        this.parameters = parameters;
    }

    @Override
    public Object query(PreparedStatementFactory factory, ResultSetExtractor rse, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        PreparedStatement preparedStatement = factory.create(cypher);
        for (int i = 0; i < parameters.size(); i++) {
            setParameter(preparedStatement, i);
        }
        return run(rse, preparedStatement);
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

    public List<?> getParameters() {
        return parameters;
    }

    private Object run(ResultSetExtractor rse, PreparedStatement preparedStatement) throws DatabaseException {
        try {
            return rse.extractData(preparedStatement.executeQuery());
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    private void setParameter(PreparedStatement preparedStatement, int i) throws DatabaseException {
        try {
            preparedStatement.setObject(i, parameters.get(i));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
