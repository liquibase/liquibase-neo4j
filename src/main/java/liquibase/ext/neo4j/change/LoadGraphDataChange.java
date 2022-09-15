package liquibase.ext.neo4j.change;

import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.change.core.LoadDataChange;
import liquibase.change.core.LoadDataColumnConfig;
import liquibase.database.Database;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.exception.UnsupportedLoadDataTypeException;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

@DatabaseChange(name = "loadData", priority = PrioritizedService.PRIORITY_DATABASE, description = "Loads data from a CSV file into a graph. Each row is loaded as a node, whose label is the configured table name.\n" +
        "A value of NULL in a cell will be skipped.\n" +
        "Lines starting with # (hash) sign are treated as comments. You can change comment pattern by " +
        "specifying 'commentLineStartsWith' attribute." +
        "To disable comments set 'commentLineStartsWith' to empty value'\n" +
        "\n" +
        "If the data type for a load column is set to NUMERIC, numbers are parsed in US locale (e.g. 123.45)." +
        "\n" +
        "Date/Time values included in the CSV file should be in ISO format " +
        "http://en.wikipedia.org/wiki/ISO_8601 in order to be parsed correctly by Liquibase. Liquibase will " +
        "initially set the date format to be 'yyyy-MM-dd'T'HH:mm:ss' and then it checks for two special " +
        "cases which will override the data format string.\n" +
        "\n" +
        "If the string representing the date/time includes a '.', then the date format is changed to " +
        "'yyyy-MM-dd'T'HH:mm:ss.SSS'\n" +
        "If the string representing the date/time includes a space, then the date format is changed " +
        "to 'yyyy-MM-dd HH:mm:ss'\n" +
        "Once the date format string is set, Liquibase will then call the SimpleDateFormat.parse() method " +
        "attempting to parse the input string so that it can return a Date/Time. If problems occur, " +
        "then a ParseException is thrown and the input string is treated as a String for the INSERT command " +
        "to be generated.\n" +
        "If UUID type is used UUID value is stored as string and NULL in cell is supported.")
public class LoadGraphDataChange extends LoadDataChange {

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    protected SqlStatement[] generateStatementsFromRows(Database database, List<LoadDataRowConfig> rows) {
        String cypher = String.format("UNWIND $0 AS row CREATE (n:`%s`) SET n += row", escapeLabel(getTableName()));
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, keyValuePairs(database, rows))};
    }

    private List<Map<String, Object>> keyValuePairs(Database database, List<LoadDataRowConfig> rows) {
        return rows.stream()
                .map(row -> row.getColumns().stream()
                        .flatMap(LoadGraphDataChange::keyValuePair)
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .collect(Collectors.toList());
    }

    private static Stream<AbstractMap.SimpleEntry<String, Object>> keyValuePair(LoadDataColumnConfig column) {
        Object value = column.getValueObject();
        if (value == null) {
            return Stream.empty();
        }
        AbstractMap.SimpleEntry<String, Object> entry = new AbstractMap.SimpleEntry<>(column.getName(), mapValue(column, value));
        return Stream.of(entry);
    }

    private static Object mapValue(LoadDataColumnConfig column, Object value) {
        LOAD_DATA_TYPE type = column.getTypeEnum();
        // FIXME: ideally switching on type should cover everything but integers/boolean have a null type
        if (value instanceof ColumnConfig.ValueNumeric) {
            return ((ColumnConfig.ValueNumeric) value).getDelegate();
        }
        if (type != null) {
            switch (type) {
                case DATE:
                    return mapTemporal(value);
                case BLOB:
                    if (column.getValueBlobFile() != null) {
                        throw new UnsupportedLoadDataTypeException(
                                "Loading BLOB files is not supported (see https://github.com/neo4j-contrib/neo4j-jdbc/issues/347)"
                        );
                    }
                    value = Base64.getDecoder().decode((String) value);
                    break;
                case CLOB:
                    if (column.getValueClobFile() != null) {
                        throw new UnsupportedLoadDataTypeException(
                                "Loading CLOB files is not supported (see https://github.com/neo4j-contrib/neo4j-jdbc/issues/348)"
                        );
                    }
                    break;
                case SEQUENCE:
                case COMPUTED:
                case OTHER:
                case UNKNOWN:
                    throw new IllegalArgumentException(
                            String.format("value type %s is currently not supported by the Neo4j plugin", type)
                    );
            }
        }
        return value;
    }

    private static Temporal mapTemporal(Object value) {
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        }
        if (value instanceof java.sql.Date) {
            return ((java.sql.Date) value).toLocalDate();
        }
        if (value instanceof java.sql.Time) {
            return ((java.sql.Time) value).toLocalTime();
        }
        if (value instanceof DatabaseFunction) {
            String rawValue = ((DatabaseFunction) value).getValue();
            return ZonedDateTime.parse(rawValue);
        }
        throw new UnsupportedLoadDataTypeException("Date value type %s is not supported", value.getClass());
    }


    private static String escapeLabel(String label) {
        return label.replace("`", "\\`");
    }
}
