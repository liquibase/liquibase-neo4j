package liquibase.ext.neo4j.change.refactoring;

import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.statement.ParameterizedCypherStatement;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class NodeMerger {

    private final Neo4jDatabase database;

    public NodeMerger(Neo4jDatabase database) {
        this.database = database;
    }

    public SqlStatement[] merge(MergePattern pattern, List<PropertyMergePolicy> policies) throws LiquibaseException {
        List<Long> ids = getNodeIds(pattern);
        if (ids.size() < 2) {
            return new RawSqlStatement[0];
        }
        List<SqlStatement> statements = new ArrayList<>(); // TODO: size properly
        generateLabelCopyStatement(ids).ifPresent(statements::add);
        generatePropertyCopyStatement(ids, policies).ifPresent(statements::add);
        generateNodeDeletion(ids).ifPresent(statements::add);
        return statements.toArray(new SqlStatement[0]);
    }

    private List<Long> getNodeIds(MergePattern pattern) throws LiquibaseException {
        String query = String.format("MATCH %s RETURN id(%s) AS ID", pattern.cypherFragment(), pattern.outputVariable());
        List<Map<String, ?>> rows = database.runCypher(query);
        return rows.stream().mapToLong(row -> (long) row.get("ID")).boxed().collect(Collectors.toList());
    }

    private Optional<SqlStatement> generateLabelCopyStatement(List<Long> ids) throws LiquibaseException {
        List<Map<String, ?>> rows = database.run(new ParameterizedCypherStatement(
                "MATCH (n) WHERE ID(n) IN $0\n" +
                        "UNWIND labels(n) AS label\n" +
                        "WITH DISTINCT label\n" +
                        "ORDER BY label ASC\n" +
                        "RETURN collect(label) AS LABELS",
                singletonList(tailOf(ids))));

        StringJoiner labelLiterals = new StringJoiner("`:`", ":`", "`");
        Map<String, ?> row = rows.get(0);
        @SuppressWarnings("unchecked")
        List<String> labels = (List<String>) row.get("LABELS");
        for (String label : labels) {
            labelLiterals.add(label);
        }
        return Optional.of(new ParameterizedCypherStatement(
                String.format("MATCH (n) WHERE ID(n) = $0 SET n%s", labelLiterals.toString()),
                singletonList(ids.get(0))));
    }

    private Optional<SqlStatement> generatePropertyCopyStatement(List<Long> ids, List<PropertyMergePolicy> policies) throws LiquibaseException {
        List<Map<String, ?>> rows = database.run(new ParameterizedCypherStatement("UNWIND $0 AS id\n" +
                "MATCH (n) WHERE id(n) = id\n" +
                "UNWIND keys(n) AS key\n" +
                "WITH {key: key, values: collect(n[key])} AS property\n" +
                "RETURN property", singletonList(ids)));

        if (rows.isEmpty()) {
            return Optional.empty();
        }
        // using insertion order for maps so that query generation is deterministic
        Map<String, Object> combinedProperties = new LinkedHashMap<>(rows.size());
        for (Map<String, ?> row : rows) {
            for (Map.Entry<String, ?> entry : row.entrySet()) {
                Map<String, Object> property = (Map<String, Object>) entry.getValue();
                String propertyName = (String) property.get("key");
                List<Object> aggregatedPropertyValues = (List<Object>) property.get("values");
                Object value = findPolicy(policies, propertyName)
                        .orElseThrow(() -> new LiquibaseException(String.format("could not find merge policy for node property %s", property)))
                        .apply(aggregatedPropertyValues);
                combinedProperties.put(propertyName, value);
            }
        }

        int parameterIndex = 0;
        List<Object> parameters = new ArrayList<>(1 + combinedProperties.size());
        parameters.add(parameterIndex, ids.get(0));
        parameters.addAll(combinedProperties.values());

        StringBuilder builder = new StringBuilder();
        builder.append("MATCH (n) WHERE id(n) = $");
        builder.append(parameterIndex);
        builder.append(" SET ");
        parameterIndex++;
        for (Map.Entry<String, Object> entry : combinedProperties.entrySet()) {
            builder.append("n.`");
            builder.append(entry.getKey());
            builder.append("` = $");
            builder.append(parameterIndex++);
        }
        return Optional.of(new ParameterizedCypherStatement(builder.toString(), parameters));
    }

    private Optional<SqlStatement> generateNodeDeletion(List<Long> ids) {
        return Optional.of(new ParameterizedCypherStatement(
                "MATCH (n) WHERE id(n) IN $0 DETACH DELETE n",
                singletonList(tailOf(ids))
        ));
    }

    private static <T> List<T> tailOf(List<T> values) {
        int size = values.size();
        List<T> result = new ArrayList<>(size - 1);
        for (int i = 1; i < size; i++) {
            result.add(values.get(i));
        }
        return result;
    }

    private static Optional<PropertyMergePolicy> findPolicy(List<PropertyMergePolicy> policies, String propertyName) {
        return policies.stream().filter(policy -> policy.property().matcher(propertyName).find()).findFirst();
    }
}
