package liquibase.ext.neo4j.database.jdbc;

import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static liquibase.ext.neo4j.database.jdbc.EmptyResultPlanPredicate.HAS_EMPTY_RESULT;

class Neo4jStatement implements Statement, PreparedStatement {

    private final Neo4jConnection connection;
    private String cypher;
    private final Map<String, Object> parameters;
    private Neo4jResultSet resultSet;
    private boolean closed;

    public Neo4jStatement(Neo4jConnection connection) {
        this(connection, null);
    }

    public Neo4jStatement(Neo4jConnection connection, String cypher) {
        this.connection = connection;
        this.cypher = cypher;
        this.parameters = new HashMap<>();
    }

    @Override
    public ResultSet executeQuery() {
        return doExecute();
    }

    @Override
    public int executeUpdate() throws SQLException {
        Neo4jResultSet resultSet = doExecute();
        resultSet.close();
        return resultSet.getUpdateCount();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("null is not a valid parameter type");
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        parameters.put(String.valueOf(parameters), x.doubleValue());
    }

    @Override
    public void setString(int parameterIndex, String x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) {
        parameters.put(String.valueOf(parameters), x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        parameters.put(String.valueOf(parameters), x.toLocalDate());
    }

    @Override
    public void setTime(int parameterIndex, Time x) {
        parameters.put(String.valueOf(parameters), x.toLocalTime());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        parameters.put(String.valueOf(parameters), x.toLocalDateTime());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearParameters() throws SQLException {
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x) {
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public boolean execute() throws SQLException {
        resultSet = doExecute();
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        cypher = sql;
        return doExecute();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        cypher = sql;
        Neo4jResultSet resultSet = doExecute();
        resultSet.close();
        return resultSet.getUpdateCount();
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close();
        }
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        cypher = sql;
        resultSet = doExecute();
        return hasResults();
    }

    @Override
    public ResultSet getResultSet() {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (resultSet.isClosed()) {
            return -1;
        }
        return resultSet.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        boolean resultSetIsOpen = !resultSet.isClosed();
        if (resultSetIsOpen) {
            resultSet.close();
        }
        return resultSetIsOpen;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Neo4jConnection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() {
        if (isAutocommit()) {
            return HOLD_CURSORS_OVER_COMMIT;
        }
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    private BiFunction<String, Map<String, Object>, Result> queryRunner() {
        if (isAutocommit()) {
            return connection.getSession()::run;
        }
        return connection.getOrBeginTransaction()::run;
    }

    boolean isAutocommit() {
        return connection.isAutocommit();
    }


    private boolean hasResults() throws SQLException {
        if (profiledQuery()) {
            return resultSet.hasResults();
        }
        try (Session session = connection.openSession();
             Transaction transaction = session.beginTransaction()) {
            return HAS_EMPTY_RESULT.test(transaction
                    .run("EXPLAIN " + cypher)
                    .consume()
                    .plan());
        } catch (Exception e) {
            return false;
        }
    }

    private boolean profiledQuery() {
        String cql = cypher.trim().toUpperCase(Locale.ENGLISH);
        return cql.startsWith("EXPLAIN") || cql.startsWith("PROFILE");
    }

    private Neo4jResultSet doExecute() {
        return new Neo4jResultSet(this, this.connection.getTypeSystem(), queryRunner().apply(cypher, parameters));
    }
}
