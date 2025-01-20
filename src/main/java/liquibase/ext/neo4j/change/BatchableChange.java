package liquibase.ext.neo4j.change;

import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.logging.Logger;
import liquibase.statement.SqlStatement;

abstract class BatchableChange extends AbstractChange {

    protected Boolean enableBatchImport = Boolean.FALSE;

    protected Long batchSize;

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validation = new ValidationErrors(this);
        if (enableBatchImport && getChangeSet().isRunInTransaction()) {
            validation.addError("enableBatchImport can be true only if the enclosing change set's runInTransaction attribute is set to false");
        }
        if (!enableBatchImport && batchSize != null) {
            validation.addError("batch size must be set only if enableBatchImport is set to true");
        }
        if (batchSize != null && batchSize <= 0) {
            validation.addError("batch size, if set, must be strictly positive");
        }
        Neo4jDatabase neo4j = (Neo4jDatabase) database;
        if (enableBatchImport && !neo4j.supportsCallInTransactions()) {
            validation.addWarning("this version of Neo4j does not support CALL {} IN TRANSACTIONS, all batch import settings are ignored");
        }
        validation.addAll(super.validate(database));
        return validation;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        Logger log = Scope.getCurrentScope().getLog(getClass());
        Neo4jDatabase neo4j = (Neo4jDatabase) database;
        boolean supportsCallInTransactions = neo4j.supportsCallInTransactions();
        if (supportsCallInTransactions && enableBatchImport) {
            log.info("Running change in CALL {} IN TRANSACTIONS");
            return generateBatchedStatements(neo4j);
        } else if (!supportsCallInTransactions) {
            log.warning("This version of Neo4j does not support CALL {} IN TRANSACTIONS, the change is going to run in a single, possibly large and slow, transaction.\n" +
                    "Note: upgrade the Neo4j server or set the runInTransaction attribute of the enclosing change set to true to make this warning disappear.");
        } else {
            log.info("Running change in single transaction (set enableBatchImport to true to switch to CALL {} IN TRANSACTIONS)");
        }
        return generateUnbatchedStatements(neo4j);
    }

    protected abstract SqlStatement[] generateBatchedStatements(Neo4jDatabase database);

    protected abstract SqlStatement[] generateUnbatchedStatements(Neo4jDatabase database);


    public Boolean getEnableBatchImport() {
        return enableBatchImport;
    }

    public void setEnableBatchImport(Boolean enableBatchImport) {
        this.enableBatchImport = enableBatchImport;
    }

    public Long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Long batchSize) {
        this.batchSize = batchSize;
    }

    protected String cypherBatchSpec() {
        return batchSize != null ? String.format(" OF %d ROWS", batchSize) : "";
    }
}
