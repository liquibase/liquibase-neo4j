package liquibase.ext.neo4j.precondition;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.precondition.AbstractPrecondition;
import liquibase.precondition.FailedPrecondition;
import liquibase.statement.core.RawParameterizedSqlStatement;

import java.util.List;
import java.util.Map;

import static liquibase.ext.neo4j.precondition.Neo4jVersionPrecondition.wrongDatabaseError;

public class ProcedureExistsPrecondition extends AbstractPrecondition {

    private static final String PROCEDURE_EXISTS_CHECK_QUERY =
            "SHOW PROCEDURES YIELD name WHERE name = $1 RETURN count(name) AS count";

    private String procedureName;

    public String getProcedureName() {
        return procedureName;
    }

    public void setProcedureName(String procedureName) {
        this.procedureName = procedureName;
    }

    @Override
    public Warnings warn(Database database) {
        return new Warnings();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors errors = new ValidationErrors();
        if (!(database instanceof Neo4jDatabase)) {
            errors.addError(wrongDatabaseError(database));
        }
        if (getProcedureName() == null || getProcedureName().trim().isEmpty()) {
            errors.addError("procedureName must be set and not blank");
        }
        return errors;
    }

    @Override
    public void check(Database database, DatabaseChangeLog changeLog, ChangeSet changeSet, ChangeExecListener changeExecListener)
            throws PreconditionFailedException, PreconditionErrorException {
        if (!(database instanceof Neo4jDatabase)) {
            throw new PreconditionFailedException(new FailedPrecondition(
                    wrongDatabaseError(database),
                    changeLog, this)
            );
        }
        try {
            if (!procedureExists((Neo4jDatabase) database, getProcedureName())) {
                throw new PreconditionFailedException(new FailedPrecondition(
                        String.format("Procedure %s does not exist", getProcedureName()),
                        changeLog, this)
                );
            }
        } catch (LiquibaseException e) {
            throw new PreconditionErrorException(e, changeLog, this);
        }
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    @Override
    public String getName() {
        return "procedureExists";
    }

    private static boolean procedureExists(Neo4jDatabase database, String procedureName) throws LiquibaseException {
        List<Map<String, ?>> rows = database.run(new RawParameterizedSqlStatement(PROCEDURE_EXISTS_CHECK_QUERY, procedureName));
        Object count = rows.get(0).get("count");
        return count instanceof Number && ((Number) count).longValue() > 0;
    }
}
