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
public class RenameLabelChange extends AbstractChange {

    private String from;

    private String to;

    private Long batchSize;

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
        if (batchSize != null && batchSize <= 0) {
            validation.addError("batch size, if set, must be strictly positive");
        }
        if (getChangeSet().isRunInTransaction() && batchSize != null) {
            validation.addError("batch size must be set only if the enclosing change set's runInTransaction attribute is set to false");
        }
        Neo4jDatabase neo4j = (Neo4jDatabase) database;
        if (!neo4j.supportsCallInTransactions() && batchSize != null) {
            validation.addWarning("this version of Neo4j does not support CALL {} IN TRANSACTIONS, batch size is going to be ignored");
        }
        validation.addAll(super.validate(database));
        return validation;
    }

    @Override
    public String getConfirmationMessage() {
        return String.format("label %s has been renamed to %s", from, to);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        Logger log = Scope.getCurrentScope().getLog(getClass());
        boolean supportsCallInTransactions = ((Neo4jDatabase) database).supportsCallInTransactions();
        if (supportsCallInTransactions && !getChangeSet().isRunInTransaction()) {
            log.info("Running label rename in CALL {} IN TRANSACTIONS");
            String batchSpec = batchSize != null ? String.format(" OF %d ROWS", batchSize) : "";
            String cypher = String.format("MATCH (n:`%s`) CALL {WITH n SET n:`%s` REMOVE n:`%1$s`} IN TRANSACTIONS%s", from, to, batchSpec);
            return new SqlStatement[]{new RawSqlStatement(cypher)};
        }
        if (!supportsCallInTransactions) {
            log.warning("This version of Neo4j does not support CALL {} IN TRANSACTIONS, the label rename is going to run in a single, possibly large and slow, transaction");
        } else {
            log.info("Running label rename in single transaction (set the enclosing change set's runInTransaction to false to switch to CALL {} IN TRANSACTIONS)");
        }
        String cypher = String.format("MATCH (n:`%s`) SET n:`%s` REMOVE n:`%1$s`", from, to);
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

    public Long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Long batchSize) {
        this.batchSize = batchSize;
    }
}
