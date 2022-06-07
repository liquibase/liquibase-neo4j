package liquibase.ext.neo4j.statement;

import liquibase.database.PreparedStatementFactory;
import liquibase.exception.DatabaseException;
import liquibase.executor.jvm.ResultSetExtractor;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.ExecutablePreparedStatement;
import liquibase.statement.QueryablePreparedStatement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class ParameterizedCypherStatement implements QueryablePreparedStatement, ExecutablePreparedStatement {

    private final String cypher;
    private final List<?> parameters;

    public ParameterizedCypherStatement(String cypher, List<?> parameters) {
        this.cypher = cypher;
        this.parameters = parameters;
    }

    @Override
    public Object query(PreparedStatementFactory factory, ResultSetExtractor rse, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        PreparedStatement preparedStatement = createPreparedStatement(factory);
        return run(preparedStatement, rse);
    }

    @Override
    public void execute(PreparedStatementFactory factory) throws DatabaseException {
        PreparedStatement preparedStatement = createPreparedStatement(factory);
        run(preparedStatement);
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

    private Object run(PreparedStatement preparedStatement, ResultSetExtractor extractor) throws DatabaseException {
        try {
            return extractor.extractData(run(preparedStatement));
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

    private PreparedStatement createPreparedStatement(PreparedStatementFactory factory) throws DatabaseException {
        PreparedStatement preparedStatement = factory.create(cypher);
        for (int i = 0; i < parameters.size(); i++) {
            setParameter(preparedStatement, i);
        }
        return preparedStatement;
    }

    private ResultSet run(PreparedStatement preparedStatement) throws DatabaseException {
        try {
            return preparedStatement.executeQuery();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
