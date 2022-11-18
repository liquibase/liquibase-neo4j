package liquibase.ext.neo4j.database.jdbc;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.TypeSystem;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

class Neo4jConnection implements Connection {

    private final String url;
    private final Driver driver;
    private final Session session;
    private Transaction transaction;
    private boolean autocommit;
    private boolean closed;

    public Neo4jConnection(String url, Properties info) {
        this.url = url;
        this.driver = GraphDatabase.driver(url, newAuthToken(info));
        this.session = driver.session();
    }

    @Override
    public Statement createStatement() {
        return new Neo4jStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String cypher) {
        return new Neo4jStatement(this, cypher);
    }

    @Override
    public CallableStatement prepareCall(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autocommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() {
        return this.autocommit;
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return new Neo4jDatabaseMetadata(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLFeatureNotSupportedException("read-only is not supported");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) {

    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != TRANSACTION_READ_COMMITTED) {
            throw new SQLFeatureNotSupportedException("only TRANSACTION_READ_COMMITTED is supported");
        }
    }

    @Override
    public int getTransactionIsolation() {
        return TRANSACTION_READ_COMMITTED;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        ensureResultSetConcurrency(resultSetConcurrency);
        return new Neo4jStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        ensureResultSetType(resultSetType);
        ensureResultSetConcurrency(resultSetConcurrency);
        return new Neo4jStatement(this);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (isAutocommit() && holdability != HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLException("only HOLD_CURSORS_OVER_COMMIT in autocommit mode");
        }
        if (!isAutocommit() && holdability != CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLException("only CLOSE_CURSORS_AT_COMMIT in non-autocommit mode");
        }
    }

    @Override
    public int getHoldability() {
        return isAutocommit() ? HOLD_CURSORS_OVER_COMMIT : CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new Neo4jStatement(this);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PreparedStatement prepareStatement(String cypher, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return new Neo4jStatement(this, cypher);
    }

    @Override
    public PreparedStatement prepareStatement(String cypher, int autoGeneratedKeys) throws SQLException {
        return new Neo4jStatement(this, cypher);
    }

    @Override
    public PreparedStatement prepareStatement(String cypher, int[] columnIndexes) throws SQLException {
        return new Neo4jStatement(this, cypher);
    }

    @Override
    public PreparedStatement prepareStatement(String cypher, String[] columnNames) throws SQLException {
        return new Neo4jStatement(this, cypher);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) {

    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void commit() throws SQLException {
        if (!transaction.isOpen()) {
            return;
        }
        transaction.commit();
        transaction.close();
    }

    @Override
    public void rollback() throws SQLException {
        if (!transaction.isOpen()) {
            return;
        }
        transaction.rollback();
        transaction.close();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        transaction.close();
        session.close();
        driver.close();
        closed = true;
    }

    public Session getSession() {
        return session;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public Transaction getOrBeginTransaction() {
        if (transaction == null || !transaction.isOpen()) {
            transaction = session.beginTransaction();
        }
        return transaction;
    }

    Session openSession() {
        return driver.session();
    }

    TypeSystem getTypeSystem() {
        return driver.defaultTypeSystem();
    }

    public String getUrl() {
        return url;
    }
    private static AuthToken newAuthToken(Properties info) {
        if (!info.containsKey("user")) {
            return AuthTokens.none();
        }
        return AuthTokens.basic(info.getProperty("user"), info.getProperty("password"));
    }

    private static void ensureResultSetType(int resultSetType) throws SQLFeatureNotSupportedException {
        if (resultSetType != TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("only TYPE_FORWARD_ONLY is supported");
        }
    }

    private static void ensureResultSetConcurrency(int resultSetConcurrency) throws SQLFeatureNotSupportedException {
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("only CONCUR_READ_ONLY is supported");
        }
    }
}
