package liquibase.ext.neo4j.change;

import liquibase.Scope;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.logging.Logger;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

@DatabaseChange(name = "invertDirection", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'invertDirection' tag allows you to invert the direction of relationships.\n" +
                "The relationships to update are defined by the 'type' attributed, and optionally refined with the" +
                "'fragment' attribute, which defines the Cypher pattern to match relationships against.\n" +
                "'invertDirection' also defines the 'outputVariable' attribute. This attribute denotes the variable used in the pattern for\n" +
                "the relationships to merge. If the fragment is '(:Movie)<-[d:DIRECTED_BY]-(:Director {name: 'John Woo'})-[a:ACTED_IN]->(:Movie)', " +
                "the output variable is either 'd' or 'a' depending on the relationships the inversion should affect.")

public class InvertDirectionChange extends BatchableChange {

    private String fragment;

    private String outputVariable;

    private String type;

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validation = new ValidationErrors(this);
        if (Sequences.isNullOrEmpty(type)) {
            validation.addError("missing type");
        }
        if ((fragment == null) ^ (outputVariable == null)) {
            String setAttribute = fragment != null ? "fragment" : "outputVariable";
            String error = String.format("both fragment and outputVariable must be set (only %s is currently set), or both must be unset", setAttribute);
            validation.addError(error);
        }
        if ("__rel__".equals(outputVariable)) {
            validation.addError(String.format("outputVariable %s clashes with the reserved variable name: __rel__. outputVariable must be renamed and fragment accordingly updated", outputVariable));
        }
        validation.addAll(super.validate(database));
        return validation;
    }

    @Override
    public String getConfirmationMessage() {
        if (fragment != null) {
            return String.format("the direction of relationships with type %s, also denoted by %s in %s, has been inverted", type, outputVariable, fragment);
        }
        return String.format("the direction of relationships with type %s has been inverted", type);    }

    @Override
    protected SqlStatement[] generateBatchedStatements(Database database) {
        String cypher = String.format("%s " +
                "CALL { " +
                "   WITH __rel__ " +
                "   MATCH (__start__) WHERE id(__start__) = id(startNode(__rel__)) " +
                "   MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                "   CREATE (__start__)<-[__newrel__:`%s`]-(__end__) " +
                "   SET __newrel__ = properties(__rel__) " +
                "   DELETE __rel__ " +
                "} IN TRANSACTIONS%s", queryStart(), type, cypherBatchSpec());
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, type)};
    }

    @Override
    protected SqlStatement[] generateUnbatchedStatements(Database database) {
        String cypher = String.format("%s " +
                "MATCH (__start__) WHERE id(__start__) = id(startNode(__rel__)) " +
                "MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                "CREATE (__start__)<-[__newrel__:`%s`]-(__end__) " +
                "SET __newrel__ = properties(__rel__) " +
                "DELETE __rel__", queryStart(), type);
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, type)};
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFragment() {
        return fragment;
    }

    public void setFragment(String fragment) {
        this.fragment = fragment;
    }

    public String getOutputVariable() {
        return outputVariable;
    }

    public void setOutputVariable(String outputVariable) {
        this.outputVariable = outputVariable;
    }
    private String queryStart() {
        if (fragment != null) {
            return String.format("MATCH %s WITH %s AS __rel__ WHERE type(__rel__) = $1", fragment, outputVariable);
        }
        return String.format("MATCH ()-[__rel__:`%s`]->()", type);
    }
}
