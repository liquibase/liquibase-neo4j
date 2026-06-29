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

public class FunctionExistsPrecondition extends AbstractPrecondition {

    private static final String FUNCTION_EXISTS_CHECK_QUERY =
            "SHOW FUNCTIONS YIELD name WHERE name = $1 RETURN count(name) AS count";

    private String functionName;

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
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
        if (getFunctionName() == null || getFunctionName().trim().isEmpty()) {
            errors.addError("function name must be set and not blank");
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
            if (!functionExists((Neo4jDatabase) database, getFunctionName())) {
                throw new PreconditionFailedException(new FailedPrecondition(
                        String.format("Function %s does not exist", getFunctionName()),
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
        return "functionExists";
    }

    private static boolean functionExists(Neo4jDatabase database, String functionName) throws LiquibaseException {
        List<Map<String, ?>> rows = database.run(new RawParameterizedSqlStatement(FUNCTION_EXISTS_CHECK_QUERY, functionName));
        if (rows.isEmpty()) {
            return false;
        }
        Object count = rows.get(0).get("count");
        return count instanceof Number && ((Number) count).longValue() > 0;
    }
}
