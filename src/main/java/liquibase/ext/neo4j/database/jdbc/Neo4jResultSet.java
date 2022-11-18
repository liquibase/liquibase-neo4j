package liquibase.ext.neo4j.database.jdbc;

import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.exceptions.value.ValueException;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.types.TypeSystem;

import java.io.InputStream;
import java.io.Reader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;
import static liquibase.ext.neo4j.database.jdbc.EmptyResultPlanPredicate.HAS_EMPTY_RESULT;

class Neo4jResultSet implements ResultSet, ResultSetMetaData {

    private final Neo4jStatement statement;
    private final TypeSystem typeSystem;
    private final Result result;
    private org.neo4j.driver.Record row;
    private boolean wasNull;
    private boolean closed;
    private ResultSummary summary;

    public Neo4jResultSet(Neo4jStatement neo4jStatement, TypeSystem typeSystem, Result result) {
        this.statement = neo4jStatement;
        this.typeSystem = typeSystem;
        this.result = result;
    }

    @Override
    public boolean next() throws SQLException {
        ensureOpen();
        try {
            row = result.next();
            return true;
        } catch (NoSuchRecordException e) {
            return false;
        } catch (Exception e) {
            throw new SQLException("cannot move to next row", e);
        }
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        try {
            summary = result.consume();
        } catch (Exception e) {
            throw new SQLException("cannot close result set", e);
        }
        closed = true;
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, this::stringify, () -> null);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, this::stringify, () -> null);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, this::booleanify, () -> false);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, this::booleanify, () -> false);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, (value) -> asByte(value, () -> outOfRangeException(columnIndex)), () -> (byte) 0);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, (value) -> asByte(value, () -> outOfRangeException(columnLabel)), () -> (byte) 0);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, (value) -> asShort(value, () -> outOfRangeException(columnIndex)), () -> (short) 0);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, (value) -> asShort(value, () -> outOfRangeException(columnLabel)), () -> (short) 0);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, Value::asInt, () -> 0);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, Value::asInt, () -> 0);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, Value::asLong, () -> 0L);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, Value::asLong, () -> 0L);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, Value::asFloat, () -> 0.0F);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, Value::asFloat, () -> 0.0F);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, Value::asDouble, () -> 0.0);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, Value::asDouble, () -> 0.0);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return tryConvert(columnIndex, (value) -> BigDecimal.valueOf(value.asLong(), scale), () -> null);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return tryConvert(columnLabel, (value) -> BigDecimal.valueOf(value.asLong(), scale), () -> null);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, (value) -> BigDecimal.valueOf(value.asDouble()), () -> null);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, (value) -> BigDecimal.valueOf(value.asDouble()), () -> null);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, Value::asByteArray, () -> null);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, Value::asByteArray, () -> null);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, (value) -> Date.valueOf(value.asLocalDate()), () -> null);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, (value) -> Date.valueOf(value.asLocalDate()), () -> null);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, (value) -> Time.valueOf(value.asLocalTime()), () -> null);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, (value) -> Time.valueOf(value.asLocalTime()), () -> null);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, (value) -> Timestamp.valueOf(value.asLocalDateTime()), () -> null);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, (value) -> Timestamp.valueOf(value.asLocalDateTime()), () -> null);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return tryConvert(columnIndex, Neo4jResultSet::convertObject, () -> null);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return tryConvert(columnLabel, Neo4jResultSet::convertObject, () -> null);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
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
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return this;
    }

    @Override
    // note: only column names are actually inspected
    // parsing column aliases would require profiling the query and parsing the execution plan for projections
    public int findColumn(String columnLabel) throws SQLException {
        ensureOpen();
        List<String> keys = getResultKeys();
        int index = keys.indexOf(columnLabel);
        if (index == -1) {
            String allKeys = String.join(", ", keys);
            throw new SQLException(
                    String.format("could not find column named %s, existing columns are: %s", columnLabel, allKeys)
            );
        }
        return index;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        ensureOpen();
        return row == null;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        ensureOpen();
        return !result.hasNext();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureOpen();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("only ResultSet.FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        ensureOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Neo4jStatement getStatement() {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        ensureOpen();
        if (statement.isAutocommit()) {
            return HOLD_CURSORS_OVER_COMMIT;
        }
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    int getUpdateCount() {
        if (summary == null) {
            summary = result.consume();
        }
        SummaryCounters counters = summary.counters();
        return sumAll(counters);
    }

    boolean hasResults() throws SQLException {
        this.close();
        return HAS_EMPTY_RESULT.test(summary.plan());
    }

    private static int sumAll(SummaryCounters counters) {
        if (!counters.containsUpdates() && !counters.containsSystemUpdates()) {
            return 0;
        }
        return counters.constraintsAdded()
                + counters.constraintsRemoved()
                + counters.indexesAdded()
                + counters.indexesRemoved()
                + counters.labelsAdded()
                + counters.labelsRemoved()
                + counters.nodesCreated()
                + counters.nodesDeleted()
                + counters.propertiesSet()
                + counters.propertiesSet()
                + counters.relationshipsCreated()
                + counters.relationshipsDeleted()
                + counters.systemUpdates();
    }

    private <T> T tryConvert(int columnIndex, SqlConversionFunction<Value, T> converter, Supplier<T> valueForNull) throws SQLException {
        ensureOpen();
        try {
            Value value = row.get(columnIndex - 1);
            wasNull = value.isNull();
            if (wasNull) {
                return valueForNull.get();
            }
            return converter.apply(value);
        } catch (Uncoercible | LossyCoercion e) {
            throw conversionException(columnIndex, e);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(String.format("cannot get column %d", columnIndex), e);
        }
    }

    private <T> T tryConvert(String columnName, SqlConversionFunction<Value, T> converter, Supplier<T> valueForNull) throws SQLException {
        ensureOpen();
        try {
            Value value = row.get(columnName);
            wasNull = value.isNull();
            if (wasNull) {
                return valueForNull.get();
            }
            return converter.apply(value);
        } catch (Uncoercible | LossyCoercion e) {
            throw conversionException(columnName, e);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(String.format("cannot get column \"%s\"", columnName), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Object convertObject(Object value) {
        if (value instanceof Value) {
            return convertObject(((Value) value).asObject());
        }
        if (value instanceof Point) {
            return pointToMap((Point) value);
        }
        if (value instanceof List<?>) {
            return ((List<?>) value).stream().map(Neo4jResultSet::convertObject).collect(toList());
        }
        if (value instanceof Map<?, ?>) {
            return Maps.mapValues((Map<String, ?>) value, Neo4jResultSet::convertObject);
        }
        if (value instanceof Entity) {
            return graphEntityToMap((Entity) value);
        }
        if (value instanceof Path) {
            return pathToList((Path) value);
        }
        if (value instanceof ZonedDateTime) {
            return Timestamp.from(((ZonedDateTime) value).toInstant());
        }
        return value;
    }

    private static Map<String, Object> pointToMap(Point point) {
        Map<String, Object> map = new HashMap<>(16);
        map.put("srid", point.srid());
        map.put("x", point.x());
        map.put("y", point.y());

        switch (point.srid()) {
            case 7203:
                map.put("crs", "cartesian");
                break;
            case 9157:
                map.put("crs", "cartesian-3d");
                map.put("z", point.z());
                break;
            case 4326:
                map.put("crs", "wgs-84");
                map.put("longitude", point.x());
                map.put("latitude", point.y());
                break;
            case 4979:
                map.put("crs", "wgs-84-3d");
                map.put("longitude", point.x());
                map.put("latitude", point.y());
                map.put("height", point.z());
                map.put("z", point.z());
                break;
        }

        return map;
    }

    private static List<Map<String, Object>> pathToList(Path value) {
        List<Map<String, Object>> result = new ArrayList<>(1 + value.length() * 2);
        result.add(graphEntityToMap(value.start()));
        for (Path.Segment segment : value) {
            result.add(graphEntityToMap(segment.relationship()));
            result.add(graphEntityToMap(segment.end()));
        }
        return result;
    }

    private static Map<String, Object> graphEntityToMap(Entity value) {
        Map<String, Object> result = new HashMap<>(5);
        result.put("_id", value.id());
        handleElementId(result, value);
        result.put("_properties", Maps.mapValues(value.asMap(), Neo4jResultSet::convertObject));
        if (value instanceof Node) {
            Node node = (Node) value;
            result.put("_labels", node.labels());
        } else if (value instanceof Relationship) {
            Relationship relationship = (Relationship) value;
            result.put("_startId", relationship.startNodeId());
            result.put("_endId", relationship.endNodeId());
            result.put("_type", relationship.type());
        }
        return result;
    }

    private void ensureOpen() throws SQLException {
        if (this.isClosed()) {
            throw new SQLException("cannot perform operation: result set is closed");
        }
    }

    private String stringify(Value value) {
        if (value.hasType(typeSystem.BOOLEAN())) {
            return String.valueOf(value.asBoolean());
        }
        if (value.hasType(typeSystem.INTEGER())) {
            return String.valueOf(value.asLong());
        }
        if (value.hasType(typeSystem.FLOAT())) {
            return String.valueOf(value.asDouble());
        }
        if (value.hasType(typeSystem.DATE())) {
            return value.asLocalDate().toString();
        }
        if (value.hasType(typeSystem.TIME())) {
            return value.asOffsetTime().toString();
        }
        if (value.hasType(typeSystem.LOCAL_TIME())) {
            return value.asLocalTime().toString();
        }
        if (value.hasType(typeSystem.DATE_TIME())) {
            return value.asZonedDateTime().toString();
        }
        if (value.hasType(typeSystem.LOCAL_DATE_TIME())) {
            return value.asLocalDateTime().toString();
        }
        if (value.hasType(typeSystem.DURATION())) {
            return value.asIsoDuration().toString();
        }
        return value.asString();
    }

    private boolean booleanify(Value value) {
        if (value.hasType(typeSystem.INTEGER())) {
            long integer = value.asLong();
            if (integer == 0) {
                return false;
            }
            if (integer == 1) {
                return true;
            }
        }
        if (value.hasType(typeSystem.STRING())) {
            String string = value.asString();
            if (string.equals("0")) {
                return false;
            }
            if (string.equals("1")) {
                return true;
            }
        }
        return value.asBoolean();
    }

    private List<String> getResultKeys() throws SQLException {
        try {
            return result.keys();
        } catch (Exception e) {
            throw new SQLException("cannot access result keys", e);
        }
    }

    private static byte asByte(Value value, Supplier<SQLException> outOfRangeException) throws SQLException {
        int i = value.asInt();
        if (outOfRange(i, Byte.MIN_VALUE, Byte.MAX_VALUE)) {
            throw outOfRangeException.get();
        }
        return (byte) i;
    }

    private static short asShort(Value value, Supplier<SQLException> outOfRangeException) throws SQLException {
        int i = value.asInt();
        if (outOfRange(i, Short.MIN_VALUE, Short.MAX_VALUE)) {
            throw outOfRangeException.get();
        }
        return (short) i;
    }

    private static boolean outOfRange(int value, int min, int max) {
        return value < min || value > max;
    }

    private static SQLException conversionException(int columnIndex, ValueException e) {
        return new SQLException(conversionErrorMessage(columnIndex), e);
    }

    private static SQLException conversionException(String columnName, ValueException e) {
        return new SQLException(conversionErrorMessage(columnName), e);
    }

    private static SQLException outOfRangeException(int columnIndex) {
        return new SQLException(outOfRangeErrorMessage(columnIndex));
    }

    private static SQLException outOfRangeException(String columnName) {
        return new SQLException(outOfRangeErrorMessage(columnName));
    }

    private static String conversionErrorMessage(int columnIndex) {
        return String.format("could not safely cast column %d to expected type", columnIndex);
    }

    private static String conversionErrorMessage(String columnName) {
        return String.format("could not safely cast column named %s to expected type", columnName);
    }

    private static String outOfRangeErrorMessage(int columnIndex) {
        return String.format("could not safely cast column %d to expected type: value is out of range", columnIndex);
    }

    private static String outOfRangeErrorMessage(String columnName) {
        return String.format("could not safely cast column named %s to expected type: value is out of range", columnName);
    }

    @Override
    public int getColumnCount() throws SQLException {
        try {
            return result.keys().size();
        } catch (Exception e) {
            throw new SQLException("cannot get column count", e);
        }
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        // note: only column names are actually inspected
        // parsing column aliases would require profiling the query and parsing the execution plan for projections
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        List<String> keys = getResultKeys();
        int count = keys.size();
        if (column <= 0 || column > count) {
            throw new SQLException(String.format(
                    "invalid column index %d, index must be between 1 (incl.) and %d (incl.)",
                    column, count));
        }
        return keys.get(column - 1);
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return false;
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 0;
    }

    @Override
    public String getSchemaName(int column) {
        return null;
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) {
        return null;
    }

    @Override
    public String getCatalogName(int column) {
        return null;
    }

    @Override
    public int getColumnType(int column) {
        return 0;
    }

    @Override
    public String getColumnTypeName(int column) {
        return null;
    }

    @Override
    public boolean isReadOnly(int column) {
        return false;
    }

    @Override
    public boolean isWritable(int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        return null;
    }

    private static void handleElementId(Map<String, Object> result, Entity value) {
        try {
            Lookup lookup = MethodHandles.publicLookup();
            MethodHandle getElementId = lookup.findVirtual(value.getClass(), "elementId", MethodType.methodType(String.class));
            String elementId = (String) getElementId.invoke(value);
            result.put("_elementId", elementId);
        } catch (NoSuchMethodException ignored) {
            // happens with Java driver < 5
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}

@FunctionalInterface
interface SqlConversionFunction<I, O> {
    O apply(I input) throws SQLException;
}
