package liquibase.ext.neo4j.change;

import liquibase.change.ColumnConfig;
import liquibase.change.ColumnConfig.ValueNumeric;
import liquibase.change.core.LoadDataChange.LOAD_DATA_TYPE;
import liquibase.change.core.LoadDataColumnConfig;
import liquibase.ext.neo4j.exception.UnsupportedLoadDataTypeException;
import liquibase.statement.DatabaseFunction;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Base64;

class ColumnMapper {

    public static Object mapValue(ColumnConfig column) {
        Object value = column.getValueObject();
        if (value instanceof ValueNumeric) {
            return mapNumericValue((ValueNumeric) value);
        }
        String type = column.getType();
        if (type == null) {
            if (column.getValueSequenceNext() != null || column.getValueSequenceCurrent() != null) {
                throw new IllegalArgumentException(
                        "sequence values are currently not supported by the Neo4j plugin"
                );
            }
            if (column.getValueComputed() != null) {
                throw new IllegalArgumentException(
                        "computed values are currently not supported by the Neo4j plugin"
                );
            }
            return value;
        }
        switch (type) {
            case "date":
                return mapTemporalValue(column.getValueObject());
            case "blob":
                return mapBlobValue(column);
            case "clob":
                return mapClobValue(column);
            default:
                return value;
        }
    }

    public static Object mapValue(LoadDataColumnConfig column) {
        Object value = column.getValueObject();
        LOAD_DATA_TYPE type = column.getTypeEnum();
        if (value instanceof ValueNumeric) {
            return mapNumericValue((ValueNumeric) value);
        }
        if (type == null) {
            return value;
        }
        switch (type) {
            case DATE:
                return mapTemporalValue(value);
            case BLOB:
                return mapBlobValue(column);
            case CLOB:
                return mapClobValue(column);
            case SEQUENCE:
            case COMPUTED:
            case OTHER:
            case UNKNOWN:
                throw new IllegalArgumentException(
                        String.format("value type %s is currently not supported by the Neo4j plugin", type)
                );
            default:
                return value;
        }
    }

    public static Temporal mapTemporalValue(Object value) {
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        }
        if (value instanceof java.sql.Time) {
            return ((java.sql.Time) value).toLocalTime();
        }
        if (value instanceof java.util.Date) {
            return ((java.util.Date) value).toInstant().atZone(ZoneOffset.UTC);
        }
        if (value instanceof DatabaseFunction) {
            String rawValue = ((DatabaseFunction) value).getValue();
            return ZonedDateTime.parse(rawValue);
        }
        throw new UnsupportedLoadDataTypeException("Date value type %s is not supported", value.getClass());
    }

    private static Number mapNumericValue(ValueNumeric value) {
        return value.getDelegate();
    }

    private static byte[] mapBlobValue(ColumnConfig column) {
        if (column.getValueBlobFile() != null) {
            throw new UnsupportedLoadDataTypeException(
                    "Loading BLOB files is not supported (see https://github.com/liquibase/liquibase-neo4j/issues/304)"
            );
        }
        return Base64.getDecoder().decode(column.getValue());
    }

    private static Object mapClobValue(ColumnConfig column) {
        if (column.getValueClobFile() != null) {
            throw new UnsupportedLoadDataTypeException(
                    "Loading CLOB files is not supported (see https://github.com/liquibase/liquibase-neo4j/issues/304)"
            );
        }
        return column.getValueObject();
    }
}
