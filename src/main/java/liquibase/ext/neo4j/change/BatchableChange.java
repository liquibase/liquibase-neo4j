package liquibase.ext.neo4j.change;

import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.KernelVersion;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.logging.Logger;
import liquibase.statement.SqlStatement;

abstract class BatchableChange extends AbstractChange {

    protected Boolean enableBatchImport = Boolean.FALSE;

    protected Long batchSize;

    private Boolean concurrent;

    private BatchErrorPolicy batchErrorPolicy;

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validation = new ValidationErrors(this);
        if (enableBatchImport && getChangeSet().isRunInTransaction()) {
            validation.addError("enableBatchImport can be true only if the enclosing change set's runInTransaction attribute is set to false");
        }
        if (!enableBatchImport && batchSize != null) {
            validation.addError("batch size must be set only if enableBatchImport is set to true");
        }
        if (!enableBatchImport && concurrent != null) {
            validation.addError("concurrent must be set only if enableBatchImport is set to true");
        }
        if (!enableBatchImport && batchErrorPolicy != null) {
            validation.addError("batchErrorPolicy must be set only if enableBatchImport is set to true");
        }
        Neo4jDatabase neo4j = (Neo4jDatabase) database;
        if (enableBatchImport) {
            KernelVersion version = neo4j.getKernelVersion();
            if (version.compareTo(KernelVersion.V4_4_0) < 0) {
                validation.addWarning("this version of Neo4j does not support CALL {} IN TRANSACTIONS, all batch import settings are ignored");
            }
            if (batchSize != null && batchSize <= 0) {
                validation.addError("batch size, if set, must be strictly positive");
            }
            if (batchErrorPolicy != null && version.compareTo(KernelVersion.V5_7_0) < 0) {
                validation.addError("this version of Neo4j does not support the configuration of CALL {} IN TRANSACTIONS error behavior (ON ERROR), Neo4j 5.7 or later is required");
            }
            if (concurrent != null && concurrent && version.compareTo(KernelVersion.V5_21_0) < 0) {
                validation.addError("this version of Neo4j does not support CALL {} IN CONCURRENT TRANSACTIONS, Neo4j 5.21 or later is required");
            }
        }
        validation.addAll(super.validate(database));
        return validation;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        Logger log = Scope.getCurrentScope().getLog(getClass());
        Neo4jDatabase neo4j = (Neo4jDatabase) database;
        boolean supportsCallInTransactions = neo4j.getKernelVersion().compareTo(KernelVersion.V4_4_0) >= 0;
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

    public Boolean getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(Boolean concurrent) {
        this.concurrent = concurrent;
    }

    public BatchErrorPolicy getBatchErrorPolicy() {
        return batchErrorPolicy;
    }

    public void setBatchErrorPolicy(BatchErrorPolicy batchErrorPolicy) {
        this.batchErrorPolicy = batchErrorPolicy;
    }

    protected String cypherBatchSpec() {
        StringBuilder builder = new StringBuilder();
        builder.append(" IN");
        if (concurrent != null && concurrent) {
            builder.append(" CONCURRENT");
        }
        builder.append(" TRANSACTIONS");
        if (batchSize != null) {
            builder.append(String.format(" OF %d ROWS", batchSize));
        }
        if (batchErrorPolicy != null) {
            builder.append(String.format(" ON ERROR %s", batchErrorPolicy));
        }
        return builder.toString();
    }

}
