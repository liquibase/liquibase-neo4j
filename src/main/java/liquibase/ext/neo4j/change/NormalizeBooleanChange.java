package liquibase.ext.neo4j.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@DatabaseChange(name = "normalizeBoolean", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "Converts string property values into booleans using configured trueValues and falseValues lists.")
public class NormalizeBooleanChange extends BatchableChange {

    private String property;
    private String trueValues;
    private String falseValues;

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors errors = new ValidationErrors(this);
        if (Sequences.isNullOrEmpty(property)) {
            errors.addError("missing property name");
            return errors;
        }
        errors.addAll(super.validate(database));
        return errors;
    }

    @Override
    public String getConfirmationMessage() {
        return String.format("property %s has been normalized as boolean", property);
    }

    @Override
    protected SqlStatement[] generateBatchedStatements(Neo4jDatabase database) {
        return generateStatements(cypherBatchSpec());
    }

    @Override
    protected SqlStatement[] generateUnbatchedStatements(Neo4jDatabase database) {
        return generateStatements("");
    }

    private SqlStatement[] generateStatements(String batchSpec) {
        String quotedProperty = property.replace("`", "\\`");
        String entitySource = """
                CALL {
                  MATCH (n) RETURN n AS t
                  UNION ALL
                  MATCH ()-[r]->() RETURN r AS t
                } WITH t AS e
                """;
        String cypher = constructCypher(batchSpec, quotedProperty, entitySource);

        return new SqlStatement[]{
                new RawParameterizedSqlStatement(cypher, parameterList(parsedTrueValues()), parameterList(parsedFalseValues()))
        };
    }

    private static String constructCypher(String batchSpec, String quotedProperty, String entitySource) {
        String propertyAccess = "e.`" + quotedProperty + "`";
        String setCase = ("SET %s = CASE"
                + " WHEN %s IN $1 THEN true"
                + " WHEN %s IN $2 THEN false"
                + " WHEN %s IN [true, false] THEN %s"
                + " ELSE null END").formatted(propertyAccess, propertyAccess, propertyAccess, propertyAccess, propertyAccess);
        String mutation = batchSpec.isEmpty()
                ? setCase
                : "CALL { WITH e " + setCase + " }" + batchSpec;
        return entitySource + "WHERE " + propertyAccess + " IS NOT NULL\n" + mutation;
    }

    private static List<Object> parameterList(List<String> values) {
        return values.isEmpty() ? new LinkedList<>() : new LinkedList<>(values);
    }

    private List<String> parsedTrueValues() {
        return splitCommaSeparated(trueValues);
    }

    private List<String> parsedFalseValues() {
        return splitCommaSeparated(falseValues);
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public String getTrueValues() {
        return trueValues;
    }

    public void setTrueValues(String trueValues) {
        this.trueValues = trueValues;
    }

    public String getFalseValues() {
        return falseValues;
    }

    public void setFalseValues(String falseValues) {
        this.falseValues = falseValues;
    }

    private static List<String> splitCommaSeparated(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.isBlank()) {
            return List.of();
        }
        return Arrays.stream(commaSeparated.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toList());
    }
}
