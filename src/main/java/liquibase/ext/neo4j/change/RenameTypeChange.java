package liquibase.ext.neo4j.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import static liquibase.ext.neo4j.database.KernelVersion.V5_24_0;
import static liquibase.ext.neo4j.database.KernelVersion.V5_26_0;

@DatabaseChange(name = "renameType", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'renameType' tag allows you to rename the type ('from' attribute) of relationships to another type value ('to' attribute).\n" +
                "By default, all matching relationships will have their matching type renamed.\n" +
                "Restricting the set of affected relationships is done with the Cypher 'fragment' attribute, which defines the pattern " +
                "to match relationships against.\n" +
                "'renameType' also defines the 'outputVariable' attribute. This attribute denotes the variable used in the pattern for\n" +
                "the relationships to merge. If the fragment is '(:Movie)<-[d:DIRECTED_BY]-(:Director {name: 'John Woo'})-[a:ACTED_IN]->(:Movie)', " +
                "the output variable is either 'd' or 'a' depending on the relationships the rename should affect.")
public class RenameTypeChange extends BatchableChange {

    private String from;

    private String to;

    private String fragment;

    private String outputVariable;

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validation = new ValidationErrors(this);
        if (Sequences.isNullOrEmpty(from)) {
            validation.addError("missing type (from)");
        }
        if (Sequences.isNullOrEmpty(to)) {
            validation.addError("missing type (to)");
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
            return String.format("type %s, for relationships denoted by %s in %s, has been renamed to %s", from, outputVariable, fragment, to);
        }
        return String.format("type %s has been renamed to %s", from, to);
    }

    @Override
    protected SqlStatement[] generateBatchedStatements(Neo4jDatabase neo4j) {
        if (supportsDynamicTypes(neo4j)) {
            String cypher = String.format("%s CALL {WITH __rel__ MATCH (__start__) " +
                                          "WHERE id(__start__) = id(startNode(__rel__)) " +
                                          "MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                                          "CREATE (__start__)-[__newrel__:$($2)]->(__end__) " +
                                          "SET __newrel__ = properties(__rel__) " +
                                          "DELETE __rel__ } " +
                                          "IN TRANSACTIONS%s", queryStart(neo4j), cypherBatchSpec());
            return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, from, to)};
        }

        String cypher = String.format("%s CALL {WITH __rel__ MATCH (__start__) " +
                "WHERE id(__start__) = id(startNode(__rel__)) " +
                "MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                "CREATE (__start__)-[__newrel__:`%s`]->(__end__) " +
                "SET __newrel__ = properties(__rel__) " +
                "DELETE __rel__ } " +
                "IN TRANSACTIONS%s", queryStart(neo4j), to, cypherBatchSpec());
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, from)};
    }

    @Override
    protected SqlStatement[] generateUnbatchedStatements(Neo4jDatabase neo4j) {
        if (supportsDynamicTypes(neo4j)) {
            String cypher = String.format("%s MATCH (__start__) " +
                                          "WHERE id(__start__) = id(startNode(__rel__)) " +
                                          "MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                                          "CREATE (__start__)-[__newrel__:$($2)]->(__end__) " +
                                          "SET __newrel__ = properties(__rel__) " +
                                          "DELETE __rel__", queryStart(neo4j));
            return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, from, to)};
        }
        String cypher = String.format("%s MATCH (__start__) " +
                "WHERE id(__start__) = id(startNode(__rel__)) " +
                "MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                "CREATE (__start__)-[__newrel__:`%s`]->(__end__) " +
                "SET __newrel__ = properties(__rel__) " +
                "DELETE __rel__", queryStart(neo4j), to);
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, from)};
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
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
    private String queryStart(Neo4jDatabase neo4j) {
        if (fragment != null) {
            return String.format("MATCH %s WITH %s AS __rel__ WHERE type(__rel__) = $1", fragment, outputVariable);
        }
        if (supportsDynamicTypes(neo4j)) {
            return "MATCH ()-[__rel__:$($1)]->()";
        }
        return String.format("MATCH ()-[__rel__:`%s`]->()", from);
    }

    private static boolean supportsDynamicTypes(Neo4jDatabase neo4j) {
        // 5.24: dynamic labels/properties in SET and REMOVE
        // 5.26: dynamic labels/types/properties in CREATE, MATCH and MERGE
        return neo4j.getKernelVersion().compareTo(V5_26_0) >= 0;
    }
}
