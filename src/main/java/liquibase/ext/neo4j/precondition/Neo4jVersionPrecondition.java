package liquibase.ext.neo4j.precondition;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.database.Database;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.ext.neo4j.database.KernelVersion;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.precondition.AbstractPrecondition;
import liquibase.precondition.FailedPrecondition;

public class Neo4jVersionPrecondition extends AbstractPrecondition {

    private String matches;

    public String getMatches() {
        return matches;
    }

    public void setMatches(String matches) {
        this.matches = matches;
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
        if (matches == null || matches.trim().isEmpty()) {
            errors.addError("version must be set and not blank");
        }
        return errors;
    }

    @Override
    public void check(Database database, DatabaseChangeLog changeLog, ChangeSet changeSet, ChangeExecListener changeExecListener) throws PreconditionFailedException, PreconditionErrorException {
        if (!(database instanceof Neo4jDatabase)) {
            throw new PreconditionFailedException(new FailedPrecondition(
                    wrongDatabaseError(database),
                    changeLog, this)
            );
        }
        KernelVersion neo4jVersion = ((Neo4jDatabase) database).getKernelVersion();
        if (!versionMatches(matches, neo4jVersion.versionString())) {
            throw new PreconditionFailedException(new FailedPrecondition(
                    String.format("expected %s version but got %s", matches, neo4jVersion),
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
        return "version";
    }

    private static boolean versionMatches(String expectedVersion, String actualVersion) {
        return actualVersion.startsWith(expectedVersion);
    }

    static String wrongDatabaseError(Database database) {
        return String.format("this precondition applies only to Neo4j but got %s", database == null ? "" : database.getShortName());
    }
}
