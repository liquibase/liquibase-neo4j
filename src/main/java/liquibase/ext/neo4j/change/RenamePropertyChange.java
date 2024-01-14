package liquibase.ext.neo4j.change;

import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.change.refactoring.TargetEntityType;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.logging.Logger;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.util.ArrayList;
import java.util.List;

@DatabaseChange(name = "renameProperty", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'renameProperty' tag allows you to rename the property name ('from' attribute) to another value ('to' attribute).\n" +
                "By default, all nodes and relationships defining that property will have their matching property renamed.")
public class RenamePropertyChange extends AbstractChange {

    private String from;

    private String to;

    private TargetEntityType entityType = TargetEntityType.ALL;

    private Boolean enableBatchImport = Boolean.FALSE;

    private Long batchSize;

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validation = new ValidationErrors(this);
        if (Sequences.isNullOrEmpty(from)) {
            validation.addError("missing name (from)");
        }
        if (Sequences.isNullOrEmpty(to)) {
            validation.addError("missing name (to)");
        }
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
    public String getConfirmationMessage() {
        String qualifier = "";
        switch (entityType) {
            case ALL:
                qualifier = " for all nodes and relationships";
                break;
            case NODE:
                qualifier = " for all nodes";
                break;
            case RELATIONSHIP:
                qualifier = " for all relationships";
                break;
        }
        return String.format("property %s has been renamed to %s%s", from, to, qualifier);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        Logger log = Scope.getCurrentScope().getLog(getClass());
        boolean supportsCallInTransactions = ((Neo4jDatabase) database).supportsCallInTransactions();
        if (supportsCallInTransactions && enableBatchImport) {
            log.info("Running property rename in CALL {} IN TRANSACTIONS");
            String batchSpec = batchSize != null ? String.format(" OF %d ROWS", batchSize) : "";
            String nodeRename = String.format("MATCH (n) WHERE n[$1] IS NOT NULL CALL { WITH n SET n.`%2$s` = n[$1] REMOVE n.`%1$s` } IN TRANSACTIONS%3$s", from, to, batchSpec);
            String relRename = String.format("MATCH ()-[r]->() WHERE r[$1] IS NOT NULL CALL { WITH r SET r.`%2$s` = r[$1] REMOVE r.`%1$s` } IN TRANSACTIONS%3$s", from, to, batchSpec);
            return filterStatements(nodeRename, relRename);
        }
        if (!supportsCallInTransactions) {
            log.warning("This version of Neo4j does not support CALL {} IN TRANSACTIONS, the type rename is going to run in a single, possibly large and slow, transaction.\n" +
                    "Note: upgrade the Neo4j server or set the runInTransaction attribute of the enclosing change set to true to make this warning disappear.");
        } else {
            log.info("Running type rename in single transaction (set enableBatchImport to true to switch to CALL {} IN TRANSACTIONS)");
        }
        String nodeRename = String.format("MATCH (n) WHERE n[$1] IS NOT NULL SET n.`%2$s` = n[$1] REMOVE n.`%1$s` ", from, to);
        String relRename = String.format("MATCH ()-[r]->() WHERE r[$1] IS NOT NULL SET r.`%2$s` = r[$1] REMOVE r.`%1$s`", from, to);
        return filterStatements(nodeRename, relRename);
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

    public TargetEntityType getEntityType() {
        return entityType;
    }

    public void setEntityType(TargetEntityType entityType) {
        this.entityType = entityType;
    }

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

    private SqlStatement[] filterStatements(String nodeRename, String relRename) {
        List<SqlStatement> statements = new ArrayList<>(2);
        switch (entityType) {
            case ALL:
                statements.add(new RawParameterizedSqlStatement(nodeRename, from));
                statements.add(new RawParameterizedSqlStatement(relRename, from));
                break;
            case NODE:
                statements.add(new RawParameterizedSqlStatement(nodeRename, from));
                break;
            case RELATIONSHIP:
                statements.add(new RawParameterizedSqlStatement(relRename, from));
                break;
        }
        return statements.toArray(new SqlStatement[0]);
    }
}
