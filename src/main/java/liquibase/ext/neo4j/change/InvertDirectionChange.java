package liquibase.ext.neo4j.change;

import liquibase.Scope;
import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.logging.Logger;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.statement.core.RawSqlStatement;

import java.util.List;
import java.util.Map;

@DatabaseChange(name = "invertDirection", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'invertDirection' tag allows you to invert the direction of relationships.\n" +
                "The relationships to update are defined by the 'type' attributed, and optionally refined with the" +
                "'fragment' attribute, which defines the Cypher pattern to match relationships against.\n" +
                "'invertDirection' also defines the 'outputVariable' attribute. This attribute denotes the variable used in the pattern for\n" +
                "the relationships to merge. If the fragment is '(:Movie)<-[d:DIRECTED_BY]-(:Director {name: 'John Woo'})-[a:ACTED_IN]->(:Movie)', " +
                "the output variable is either 'd' or 'a' depending on the relationships the inversion should affect.")

public class InvertDirectionChange extends AbstractChange {

    private String fragment;

    private String outputVariable;

    private String type;

    private Boolean enableBatchImport = Boolean.FALSE;

    private Long batchSize;

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
        if (fragment != null) {
            return String.format("the direction of relationships with type %s, also denoted by %s in %s, has been inverted", type, outputVariable, fragment);
        }
        return String.format("the direction of relationships with type %s has been inverted", type);    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        Logger log = Scope.getCurrentScope().getLog(getClass());
        boolean supportsCallInTransactions = ((Neo4jDatabase) database).supportsCallInTransactions();
        if (supportsCallInTransactions && enableBatchImport) {
            log.info("Running type rename in CALL {} IN TRANSACTIONS");
            String batchSpec = batchSize != null ? String.format(" OF %d ROWS", batchSize) : "";
            String cypher = String.format("%s " +
                    "CALL { " +
                    "   WITH __rel__ " +
                    "   MATCH (__start__) WHERE id(__start__) = id(startNode(__rel__)) " +
                    "   MATCH (__end__) WHERE id(__end__) = id(endNode(__rel__)) " +
                    "   CREATE (__start__)<-[__newrel__:`%s`]-(__end__) " +
                    "   SET __newrel__ = properties(__rel__) " +
                    "   DELETE __rel__ " +
                    "} IN TRANSACTIONS%s", queryStart(), type, batchSpec);
            return new SqlStatement[]{new RawParameterizedSqlStatement(cypher, type)};
        }
        if (!supportsCallInTransactions) {
            log.warning("This version of Neo4j does not support CALL {} IN TRANSACTIONS, the type rename is going to run in a single, possibly large and slow, transaction.\n" +
                    "Note: upgrade the Neo4j server or set the runInTransaction attribute of the enclosing change set to true to make this warning disappear.");
        } else {
            log.info("Running type rename in single transaction (set enableBatchImport to true to switch to CALL {} IN TRANSACTIONS)");
        }
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

    private String queryStart() {
        if (fragment != null) {
            return String.format("MATCH %s WITH %s AS __rel__ WHERE type(__rel__) = $1", fragment, outputVariable);
        }
        return String.format("MATCH ()-[__rel__:`%s`]->()", type);
    }
}
