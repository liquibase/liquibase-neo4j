package liquibase.ext.neo4j.changelog;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.changelog.ChangeLogHistoryService;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.DatabaseHistoryException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;

import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Neo4jChangelogHistoryService implements ChangeLogHistoryService {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    public void setDatabase(Database database) {

    }

    @Override
    public void reset() {

    }

    @Override
    public void init() throws DatabaseException {

    }

    @Override
    public void upgradeChecksums(DatabaseChangeLog databaseChangeLog, Contexts contexts, LabelExpression labels) throws DatabaseException {

    }

    @Override
    public List<RanChangeSet> getRanChangeSets() throws DatabaseException {
        return Collections.emptyList();
    }

    @Override
    public RanChangeSet getRanChangeSet(ChangeSet changeSet) throws DatabaseException, DatabaseHistoryException {
        return null;
    }

    @Override
    public ChangeSet.RunStatus getRunStatus(ChangeSet changeSet) throws DatabaseException, DatabaseHistoryException {
        return ChangeSet.RunStatus.NOT_RAN;
    }

    @Override
    public Date getRanDate(ChangeSet changeSet) throws DatabaseException, DatabaseHistoryException {
        return null;
    }

    @Override
    public void setExecType(ChangeSet changeSet, ChangeSet.ExecType execType) throws DatabaseException {

    }

    @Override
    public void removeFromHistory(ChangeSet changeSet) throws DatabaseException {

    }

    @Override // secondary ordering
    public int getNextSequenceValue() throws LiquibaseException {
        return 0;
    }

    @Override // ~ like Git tags
    public void tag(String tagString) throws DatabaseException {

    }

    @Override
    public boolean tagExists(String tag) throws DatabaseException {
        return false;
    }

    @Override
    public void clearAllCheckSums() throws LiquibaseException {

    }

    @Override
    public void destroy() throws DatabaseException {

    }

    @Override // random number assigned when an update is run
    public String getDeploymentId() {
        return null;
    }

    @Override
    public void resetDeploymentId() {

    }

    @Override
    public void generateDeploymentId() {

    }
}
