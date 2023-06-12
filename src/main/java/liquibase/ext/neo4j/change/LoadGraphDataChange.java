package liquibase.ext.neo4j.change;

import liquibase.change.DatabaseChange;
import liquibase.change.core.LoadDataChange;
import liquibase.change.core.LoadDataColumnConfig;
import liquibase.database.Database;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.servicelocator.PrioritizedService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static liquibase.ext.neo4j.change.ColumnMapper.mapValue;

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
        return database instanceof Neo4jDatabase || super.supports(database);
    }

    @Override
    protected SqlStatement[] generateStatementsFromRows(Database database, List<LoadDataRowConfig> rows) {
        if (!(database instanceof Neo4jDatabase)) {
            return super.generateStatementsFromRows(database, rows);
        }
        String cypher = String.format("UNWIND $0 AS row CREATE (n:`%s`) SET n += row", escapeLabel(getTableName()));
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, keyValuePairs(rows))};
    }

    private List<Map<String, Object>> keyValuePairs(List<LoadDataRowConfig> rows) {
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
        AbstractMap.SimpleEntry<String, Object> entry = new AbstractMap.SimpleEntry<>(column.getName(), mapValue(column));
        return Stream.of(entry);
    }


    private static String escapeLabel(String label) {
        return label.replace("`", "\\`");
    }
}
