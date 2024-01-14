package liquibase.ext.neo4j.change;

import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.change.Sequences;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.logging.Logger;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.statement.core.RawSqlStatement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

@DatabaseChange(name = "renameType", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'renameType' tag allows you to rename the type ('from' attribute) of relationships to another type value ('to' attribute).\n" +
                "By default, all matching relationships will have their matching type renamed.\n" +
                "Restricting the set of affected relationships is done with the Cypher 'fragment' attribute, which defines the pattern" +
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
    public SqlStatement[] generateStatements(Database database) {
        Logger log = Scope.getCurrentScope().getLog(getClass());
        boolean supportsCallInTransactions = ((Neo4jDatabase) database).supportsCallInTransactions();
        if (supportsCallInTransactions && enableBatchImport) {
            log.info("Running type rename in CALL {} IN TRANSACTIONS");

        }
        if (!supportsCallInTransactions) {
            log.warning("This version of Neo4j does not support CALL {} IN TRANSACTIONS, the type rename is going to run in a single, possibly large and slow, transaction.\n" +
                    "Note: upgrade the Neo4j server or set the runInTransaction attribute of the enclosing change set to true to make this warning disappear.");
        } else {
            log.info("Running type rename in single transaction (set enableBatchImport to true to switch to CALL {} IN TRANSACTIONS)");
        }
        String cypher = String.format("%s MATCH (__start__) WHERE id(__start__) = id(startNode(__rel__)) MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) CREATE (__start__)-[__newrel__:`%s`]->(__end__) SET __newrel__ = properties(__rel__) DELETE __rel__", queryStart(), to);
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, from)};
    }

    @Override
    protected SqlStatement[] generateBatchedStatements(Database database) {
        String cypher = String.format("%s CALL {WITH __rel__ MATCH (__start__) " +
                "WHERE id(__start__) = id(startNode(__rel__)) " +
                "MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                "CREATE (__start__)-[__newrel__:`%s`]->(__end__) " +
                "SET __newrel__ = properties(__rel__) " +
                "DELETE __rel__ } " +
                "IN TRANSACTIONS%s", queryStart(), to, cypherBatchSpec());
        return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, from)};
    }

    @Override
    protected SqlStatement[] generateUnbatchedStatements(Database database) {
        String cypher = String.format("%s MATCH (__start__) " +
                "WHERE id(__start__) = id(startNode(__rel__)) " +
                "MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                "CREATE (__start__)-[__newrel__:`%s`]->(__end__) " +
                "SET __newrel__ = properties(__rel__) " +
                "DELETE __rel__", queryStart(), to);
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
    private String queryStart() {
        if (fragment != null) {
            return String.format("MATCH %s WITH %s AS __rel__ WHERE type(__rel__) = $1", fragment, outputVariable);
        }
        return String.format("MATCH ()-[__rel__:`%s`]->()", from);
    }
}
