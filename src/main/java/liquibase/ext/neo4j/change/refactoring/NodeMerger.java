package liquibase.ext.neo4j.change.refactoring;

import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class NodeMerger {

    private final Neo4jDatabase database;

    public NodeMerger(Neo4jDatabase database) {
        this.database = database;
    }

    public SqlStatement[] merge(MatchPattern pattern, List<PropertyMergePolicy> policies) throws LiquibaseException {
        List<Long> ids = getNodeIds(pattern);
        if (ids.size() < 2) {
            return new SqlStatement[0];
        }
        List<SqlStatement> statements = new ArrayList<>(4);
        generateLabelCopyStatement(ids).ifPresent(statements::add);
        generatePropertyCopyStatement(ids, policies).ifPresent(statements::add);
        generateRelationshipCopyStatements(ids).ifPresent(statements::add);
        generateNodeDeletion(ids).ifPresent(statements::add);
        return statements.toArray(new SqlStatement[0]);
    }

    private List<Long> getNodeIds(MatchPattern pattern) throws LiquibaseException {
        String query = String.format("MATCH %s RETURN id(%s) AS id", pattern.cypherFragment(), pattern.outputVariable());
        List<Map<String, ?>> rows = database.run(new RawParameterizedSqlStatement(query));
        return rows.stream().map(row -> (Long) row.get("id"))
                // note: this is explicitly NOT using an ArrayList because of a regression introduced in core v4.29.2
                // (see commit 631b7d42dd32d67f59f8294dc40d20d3c01085ab, JdbcExecutor#setParameters)
                // ArrayList parameters get flattened instead of being treated as a whole list
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private Optional<SqlStatement> generateLabelCopyStatement(List<Long> ids) throws LiquibaseException {
        List<Map<String, ?>> rows = database.run(new RawParameterizedSqlStatement(
                "MATCH (n) WHERE ID(n) IN $1\n" +
                        "UNWIND labels(n) AS label\n" +
                        "WITH DISTINCT label\n" +
                        "ORDER BY label ASC\n" +
                        "RETURN collect(label) AS labels",
                tailOf(ids)));

        StringJoiner labelLiterals = new StringJoiner("`:`", ":`", "`");
        Map<String, ?> row = rows.get(0);
        @SuppressWarnings("unchecked")
        List<String> labels = (List<String>) row.get("labels");
        for (String label : labels) {
            labelLiterals.add(label);
        }
        return Optional.of(new RawParameterizedSqlStatement(
                String.format("MATCH (n) WHERE ID(n) = $1 SET n%s", labelLiterals),
                ids.get(0)));
    }

    private Optional<SqlStatement> generatePropertyCopyStatement(List<Long> ids, List<PropertyMergePolicy> policies) throws LiquibaseException {
        List<Map<String, ?>> rows = database.run(new RawParameterizedSqlStatement(
                "UNWIND $1 AS id\n" +
                        "MATCH (n) WHERE id(n) = id\n" +
                        "UNWIND keys(n) AS key\n" +
                        "WITH key, n[key] as value\n" +
                        "WITH key, collect(value) AS values\n" +
                        "RETURN {key: key, values: values} AS property\n" +
                        "ORDER BY property.key ASC", ids));

        if (rows.isEmpty()) {
            return Optional.empty();
        }
        // using insertion order for maps so that query generation is deterministic
        Map<String, Object> combinedProperties = new LinkedHashMap<>(rows.size());
        for (Map<String, ?> row : rows) {
            for (Map.Entry<String, ?> entry : row.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> property = (Map<String, Object>) entry.getValue();
                String propertyName = (String) property.get("key");
                @SuppressWarnings("unchecked")
                List<Object> aggregatedPropertyValues = (List<Object>) property.get("values");
                Object value = findPolicy(policies, propertyName)
                        .orElseThrow(() -> new LiquibaseException(String.format("could not find merge policy for node property %s", propertyName)))
                        .apply(aggregatedPropertyValues);
                combinedProperties.put(propertyName, value);
            }
        }

        return Optional.of(new RawParameterizedSqlStatement(
                "MATCH (n) WHERE id(n) = $1 SET n = $2",
                asList(ids.get(0), combinedProperties).toArray()));
    }

    private Optional<SqlStatement> generateRelationshipCopyStatements(List<Long> ids) throws LiquibaseException {
        Set<Long> nodeIdTail = tailOf(ids);
        List<Map<String, ?>> rows = database.run(new RawParameterizedSqlStatement(
                "MATCH (n) WHERE id(n) IN $1\n" +
                        "WITH [ (n)-[r]-() | r ] AS rels\n" +
                        "UNWIND rels AS rel\n" +
                        "RETURN DISTINCT rel\n" +
                        "ORDER BY type(rel) ASC, id(rel) ASC",
                nodeIdTail
        ));
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        int parameterIndex = 1;
        StringBuilder query = new StringBuilder();
        List<Object> parameters = new ArrayList<>();
        query.append(String.format("MATCH (target) WHERE id(target) = $%d ", parameterIndex));
        parameters.add(parameterIndex-1, ids.get(0));
        parameterIndex++;
        for (Map<String, ?> row : rows) {
            @SuppressWarnings("unchecked")
            Map<String, Object> relation = (Map<String, Object>) row.get("rel");
            parameters.add(parameterIndex-1, relProperties(relation));
            long startId = (long) relation.get("_startId");
            long endId = (long) relation.get("_endId");
            if (nodeIdTail.contains(startId) && nodeIdTail.contains(endId)) { // current or post-merge self-rel
                query.append(String.format("WITH target CREATE (target)-[rel_%1$d:`%2$s`]->(target) SET rel_%1$d = $%3$d ", parameterIndex, relation.get("_type"), parameterIndex));
                parameterIndex++;
                continue;
            }
            if (nodeIdTail.contains(endId)) { // incoming
                parameters.add(parameterIndex, startId);
                query.append(String.format("WITH target MATCH (n_%1$d) WHERE id(n_%1$d) = $%1$d ", parameterIndex + 1));
                query.append(String.format("CREATE (n_%1$d)-[rel_%1$d:`%2$s`]->(target) SET rel_%1$d = $%3$d ", parameterIndex + 1, relation.get("_type"), parameterIndex));
            } else { // outgoing
                parameters.add(parameterIndex, endId);
                query.append(String.format("WITH target MATCH (n_%1$d) WHERE id(n_%1$d) = $%1$d ", parameterIndex + 1));
                query.append(String.format("CREATE (n_%1$d)<-[rel_%1$d:`%2$s`]-(target) SET rel_%1$d = $%3$d ", parameterIndex + 1, relation.get("_type"), parameterIndex));
            }
            parameterIndex += 2;
        }
        return Optional.of(new RawParameterizedSqlStatement(query.toString(), parameters.toArray()));
    }

    private Optional<SqlStatement> generateNodeDeletion(List<Long> ids) {
        return Optional.of(new RawParameterizedSqlStatement(
                "MATCH (n) WHERE id(n) IN $1 DETACH DELETE n",
                tailOf(ids)
        ));
    }

    private static <T> Set<T> tailOf(List<T> values) {
        int size = values.size();
        Set<T> result = new LinkedHashSet<>(size - 1);
        for (int i = 1; i < size; i++) {
            result.add(values.get(i));
        }
        return result;
    }

    private static Optional<PropertyMergePolicy> findPolicy(List<PropertyMergePolicy> policies, String propertyName) {
        return policies.stream().filter(policy -> policy.getPropertyNamePattern().matcher(propertyName).find()).findFirst();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> relProperties(Map<String, Object> relationship) {
        return (Map<String, Object>) relationship.get("_properties");
    }
}
