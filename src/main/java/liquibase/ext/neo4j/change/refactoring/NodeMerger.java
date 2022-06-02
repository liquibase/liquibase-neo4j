package liquibase.ext.neo4j.change.refactoring;

import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.core.RawSqlStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class NodeMerger {

    private final Neo4jDatabase database;

    public NodeMerger(Neo4jDatabase database) {
        this.database = database;
    }

    public RawSqlStatement[] merge(MergePattern pattern, List<PropertyMergePolicy> policies) throws LiquibaseException {
        List<Long> ids = getNodeIds(pattern);
        if (ids.size() < 2) {
            return new RawSqlStatement[0];
        }
        List<RawSqlStatement> statements = new ArrayList<>(); // TODO: size properly
        generateLabelCopyStatement(ids).ifPresent(statements::add);
        generateNodeDeletion(ids).ifPresent(statements::add);
        return statements.toArray(new RawSqlStatement[0]);
    }

    private Optional<RawSqlStatement> generateLabelCopyStatement(List<Long> ids) throws LiquibaseException {
        List<Map<String, ?>> rows = database.runCypher("MATCH (n) WHERE ID(n) IN %s\n" +
                "UNWIND labels(n) AS label\n" +
                "WITH DISTINCT label\n" +
                "ORDER BY label ASC\n" +
                "RETURN collect(label) AS LABELS", cypherIdList(tailOf(ids)));

        StringJoiner labelsCypherValue = new StringJoiner(":", ":", "");
        Map<String, ?> row = rows.get(0);
        for (String label : (List<String>) row.get("LABELS")) {
            labelsCypherValue.add(label);
        }
        String query = String.format("MATCH (n) WHERE ID(n) = %d SET n%s", ids.get(0), labelsCypherValue.toString());
        return Optional.of(new RawSqlStatement(query));
    }

    private List<Long> getNodeIds(MergePattern pattern) throws LiquibaseException {
        String query = String.format("MATCH %s RETURN id(%s) AS ID", pattern.cypherFragment(), pattern.outputVariable());
        List<Map<String, ?>> rows = database.runCypher(query);
        return rows.stream().mapToLong(row -> (long) row.get("ID")).boxed().collect(Collectors.toList());
    }

    private Optional<RawSqlStatement> generateNodeDeletion(List<Long> ids) {
        String query = String.format("MATCH (n) WHERE id(n) IN %s DETACH DELETE n", cypherIdList(tailOf(ids)));
        return Optional.of(new RawSqlStatement(query));
    }

    private static <T> List<T> tailOf(List<T> values) {
        int size = values.size();
        List<T> result = new ArrayList<>(size - 1);
        for (int i = 1; i < size; i++) {
            result.add(values.get(i));
        }
        return result;
    }

    private static String cypherIdList(List<Long> ids) {
        return ids.stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }
}
