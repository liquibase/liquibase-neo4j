package liquibase.ext.neo4j.precondition;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.database.Database;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.precondition.AbstractPrecondition;
import liquibase.precondition.FailedPrecondition;

import static liquibase.ext.neo4j.precondition.Neo4jVersionPrecondition.wrongDatabaseError;

public class Neo4jEditionPrecondition extends AbstractPrecondition {

    private boolean enterprise;

    public boolean isEnterprise() {
        return enterprise;
    }

    public void setEnterprise(boolean enterprise) {
        this.enterprise = enterprise;
    }

    @Override
    public Warnings warn(Database database) {
        return new Warnings();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors result = new ValidationErrors();
        if (!(database instanceof Neo4jDatabase)) {
            result.addError(wrongDatabaseError(database));
        }
        return result;
    }

    @Override
    public void check(Database database, DatabaseChangeLog changeLog, ChangeSet changeSet, ChangeExecListener changeExecListener) throws PreconditionFailedException, PreconditionErrorException {
        if (!(database instanceof Neo4jDatabase)) {
            throw new PreconditionFailedException(new FailedPrecondition(
                    wrongDatabaseError(database),
                    changeLog, this)
            );
        }
        boolean actualEnterpriseEdition = ((Neo4jDatabase) database).isEnterprise();
        if (actualEnterpriseEdition != enterprise) {
            throw new PreconditionFailedException(new FailedPrecondition(
                    String.format("expected %s edition but got %s edition", editionName(enterprise), editionName(actualEnterpriseEdition)),
                    changeLog, this)
            );
        }
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    @Override
    public String getName() {
        return "edition";
    }

    private static String editionName(boolean enterprise) {
        if (enterprise) {
            return "enterprise";
        }
        return "community";
    }
}
