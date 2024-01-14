package liquibase.ext.neo4j.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.change.refactoring.TargetEntityType;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.util.ArrayList;
import java.util.List;

@DatabaseChange(name = "renameProperty", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'renameProperty' tag allows you to rename the property name ('from' attribute) to another value ('to' attribute).\n" +
                "By default, all nodes and relationships defining that property will have their matching property renamed.")
public class RenamePropertyChange extends BatchableChange {

    private String from;

    private String to;

    private TargetEntityType entityType = TargetEntityType.ALL;

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
    protected SqlStatement[] generateBatchedStatements(Database database) {
        String batchSpec = cypherBatchSpec();
        String nodeRename = String.format("MATCH (n) WHERE n[$1] IS NOT NULL CALL { WITH n SET n.`%2$s` = n[$1] REMOVE n.`%1$s` } IN TRANSACTIONS%3$s", from, to, batchSpec);
        String relRename = String.format("MATCH ()-[r]->() WHERE r[$1] IS NOT NULL CALL { WITH r SET r.`%2$s` = r[$1] REMOVE r.`%1$s` } IN TRANSACTIONS%3$s", from, to, batchSpec);
        return filterStatements(nodeRename, relRename);
    }

    @Override
    protected SqlStatement[] generateUnbatchedStatements(Database database) {
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
