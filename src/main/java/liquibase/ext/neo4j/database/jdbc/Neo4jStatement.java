package liquibase.ext.neo4j.database.jdbc;

import org.neo4j.driver.QueryRunner;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static liquibase.ext.neo4j.database.jdbc.EmptyResultPlanPredicate.HAS_EMPTY_RESULT;
import static liquibase.ext.neo4j.database.jdbc.ResultSets.rsConcurrencyName;
import static liquibase.ext.neo4j.database.jdbc.ResultSets.rsHoldabilityName;
import static liquibase.ext.neo4j.database.jdbc.ResultSets.rsTypeName;

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
        this(connection, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, connection.isAutocommit() ? HOLD_CURSORS_OVER_COMMIT : CLOSE_CURSORS_AT_COMMIT);
        this.cypher = cypher;
    }

    public Neo4jStatement(Neo4jConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        if (!validResultSetParams(connection, resultSetType, resultSetConcurrency, resultSetHoldability)) {
            throw new RuntimeException(String.format("Expected the following result set parameters: \n" +
                            " - type: ResultSet.TYPE_FORWARD_ONLY\n" +
                            " - concurrency: ResultSet.CONCUR_READ_ONLY\n" +
                            " - holdability: ResultSet.%s\n" +
                            "but got:\n" +
                            " - type: ResultSet.%s\n" +
                            " - concurrency: ResultSet.%s\n" +
                            " - holdability: ResultSet.%s\n",
                    rsHoldabilityName(connection.isAutocommit() ? HOLD_CURSORS_OVER_COMMIT : CLOSE_CURSORS_AT_COMMIT),
                    rsTypeName(resultSetType),
                    rsConcurrencyName(resultSetConcurrency),
                    rsHoldabilityName(resultSetHoldability)));
        }
        this.connection = connection;
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
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x.doubleValue());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x.toLocalDate());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x.toLocalTime());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x.toLocalDateTime());
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
    public void clearParameters() {
        parameters.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkIndex(parameterIndex);
        parameters.put(String.valueOf(parameterIndex), x);
    }

    @Override
    public boolean execute() {
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
    public ResultSetMetaData getMetaData() {
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
    public ParameterMetaData getParameterMetaData() {
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
    public ResultSet executeQuery(String sql) {
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
    public int getMaxFieldSize() {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) {

    }

    @Override
    public int getMaxRows() {
        return 0;
    }

    @Override
    public void setMaxRows(int max) {

    }

    @Override
    public void setEscapeProcessing(boolean enable) {

    }

    @Override
    public int getQueryTimeout() {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) {

    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {

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
    public int getUpdateCount() {
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
    public void setFetchDirection(int direction) {

    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {

    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() {
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
    public Neo4jConnection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() {
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
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) {

    }

    @Override
    public boolean isPoolable() {
        return false;
    }

    @Override
    public void closeOnCompletion() {

    }

    @Override
    public boolean isCloseOnCompletion() {
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

    private QueryRunner queryRunner() {
        if (isAutocommit()) {
            return connection.getSession();
        }
        return connection.getOrBeginTransaction();
    }

    boolean isAutocommit() {
        return connection.isAutocommit();
    }

    // visible for testing
    Map<String, Object> getParameters() {
        return Collections.unmodifiableMap(parameters);
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
        return new Neo4jResultSet(
                this,
                this.connection.getTypeSystem(),
                queryRunner().run(cypher, parameters)
        );
    }

    private static boolean validResultSetParams(Neo4jConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        if (resultSetType != TYPE_FORWARD_ONLY) {
            return false;
        }
        if (resultSetConcurrency != CONCUR_READ_ONLY) {
            return false;
        }
        boolean autocommit = connection.isAutocommit();
        return (autocommit && resultSetHoldability == HOLD_CURSORS_OVER_COMMIT) ||
                (!autocommit && resultSetHoldability == CLOSE_CURSORS_AT_COMMIT);
    }

    private static void checkIndex(int parameterIndex) throws SQLException {
        // TODO: change to <= 0 once Liquibase JdbcExecutor is patched
        if (parameterIndex < 0) {
            throw new SQLException(String.format("only strictly positive parameter indices are allowed, got: %d", parameterIndex));
        }
    }
}
