package liquibase.ext.neo4j.change;

import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.logging.Logger;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

@DatabaseChange(name = "renameLabel", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'renameLabel' tag allows you to rename the label ('from' attribute) of nodes to another label value ('to' attribute).\n" +
                "By default, all matching nodes will have their matching label renamed.\n" +
                "Restricting the set of affected nodes is done with the Cypher 'fragment' attribute, which defines the pattern" +
                "to match nodes against.\n" +
                "'renameLabel' also defines the 'outputVariable' attribute. This attribute denotes the variable used in the pattern for\n" +
                "the nodes to merge. If the fragment is '(m:Movie)<-[:DIRECTED_BY]-(d:Director {name: 'John Woo'})', " +
                "the output variable is either 'm' or 'd' depending on the nodes the rename should affect.")
public class RenameLabelChange extends BatchableChange {

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
            validation.addError("missing label (from)");
        }
        if (Sequences.isNullOrEmpty(to)) {
            validation.addError("missing label (to)");
        }
        if ((fragment == null) ^ (outputVariable == null)) {
            String setAttribute = fragment != null ? "fragment" : "outputVariable";
            String error = String.format("both fragment and outputVariable must be set (only %s is currently set), or both must be unset", setAttribute);
            validation.addError(error);
        }
        if ("__node__".equals(outputVariable)) {
            validation.addError("__node__ is a reserved variable name, outputVariable must be renamed and fragment accordingly updated");
        }
        validation.addAll(super.validate(database));
        return validation;
    }

    @Override
    public String getConfirmationMessage() {
        if (fragment != null) {
            return String.format("label %s, for nodes denoted by %s in %s, has been renamed to %s", from, outputVariable, fragment, to);
        }
        return String.format("label %s has been renamed to %s", from, to);
    }

    @Override
    protected SqlStatement[] generateBatchedStatements(Database database) {
        String cypher = String.format("%s CALL {WITH __node__ SET __node__:`%s` REMOVE __node__:`%s`} IN TRANSACTIONS%s",
                queryStart(), to, from, cypherBatchSpec());
        return new SqlStatement[]{new RawSqlStatement(cypher)};
    }

    @Override
    protected SqlStatement[] generateUnbatchedStatements(Database database) {
        String cypher = String.format("%s SET __node__:`%s` REMOVE __node__:`%s`", queryStart(), to, from);
        return new SqlStatement[]{new RawSqlStatement(cypher)};
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
    private String queryStart() {
        if (fragment != null) {
            return String.format("MATCH %s WITH %s AS __node__ WHERE __node__:`%s`", fragment, outputVariable, from);
        }
        return String.format("MATCH (__node__:`%s`)", from);
    }
}
