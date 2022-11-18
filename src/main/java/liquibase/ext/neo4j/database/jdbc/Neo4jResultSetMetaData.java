package liquibase.ext.neo4j.database.jdbc;

import org.neo4j.driver.Result;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class Neo4jResultSetMetaData implements ResultSetMetaData {
    private final Neo4jResultSet resultSet;
    private final Result result;

    public Neo4jResultSetMetaData(Neo4jResultSet resultSet) {
        this.resultSet = resultSet;
        this.result = resultSet.getResult();
    }

    @Override
    public int getColumnCount() {
        return result.keys().size();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return resultSet.getStatement().isAutocommit();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return this.result.keys().get(column-1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return this.result.keys().get(column-1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return false;
    }
}
