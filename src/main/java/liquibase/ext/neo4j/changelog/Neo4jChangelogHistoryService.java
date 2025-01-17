package liquibase.ext.neo4j.changelog;

import liquibase.ChecksumVersion;
import liquibase.ContextExpression;
import liquibase.Labels;
import liquibase.Scope;
import liquibase.Scope.Attr;
import liquibase.change.CheckSum;
import liquibase.change.core.TagDatabaseChange;
import liquibase.changelog.AbstractChangeLogHistoryService;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.util.LiquibaseUtil;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class Neo4jChangelogHistoryService extends AbstractChangeLogHistoryService {

    public static final String TAG_CONSTRAINT_NAME = "unique_liquibase_tag";

    public static final String CONTEXT_CONSTRAINT_NAME = "unique_liquibase_context";

    public static final String LABEL_CONSTRAINT_NAME = "unique_liquibase_label";

    public static final String CHANGE_SET_CONSTRAINT_NAME = "node_key_liquibase_change_set";

    public static final String CHANGE_SET_CHECK_SUM_INDEX_NAME = "index_liquibase_change_set_check_sum";

    private Neo4jDatabase database;

    private List<RanChangeSet> ranChangeSets;

    private Integer lastChangeSetSequenceValue;

    private boolean containsOlderCheckSums;

    private ChecksumVersion currentCheckSumVersion;

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
        this.database = (Neo4jDatabase) database;
    }

    @Override
    public Neo4jDatabase getDatabase() {
        return database;
    }

    @Override
    public void init() throws DatabaseException {
        currentCheckSumVersion = Scope.getCurrentScope().get(Attr.checksumVersion, ChecksumVersion.class);
        cleanUpGraph();
        createConstraints();
        createIndices();
        initializeHistory();
        readCheckSums();
    }

    @Override
    public void destroy() throws DatabaseException {
        removeHistory();
        removeIndices();
        removeConstraints();
        reset();
        currentCheckSumVersion = null;
    }

    @Override
    public boolean isDatabaseChecksumsCompatible() {
        return !containsOlderCheckSums;
    }

    @Override
    public List<RanChangeSet> getRanChangeSets(boolean ignore) throws DatabaseException {
        return getRanChangeSets();
    }

    @Override
    public List<RanChangeSet> getRanChangeSets() throws DatabaseException {
        if (ranChangeSets != null) {
            return ranChangeSets;
        }
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        try {
            List<Map<String, Object>> results = readChangeSets(executor);
            database.rollback();
            ranChangeSets = mapRanChangeSets(results);
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not read ran change sets", e);
        }
        return ranChangeSets;
    }

    // visible for Liquigraph
    @Override
    public void replaceChecksum(ChangeSet changeSet) throws DatabaseException {
        try {
            database.execute(new RawParameterizedSqlStatement(
                    "MATCH (changeSet:__LiquibaseChangeSet {id: $1, author: $2, changeLog: $3})-[:IN_CHANGELOG]->(changeLog:__LiquibaseChangeLog) " +
                            "SET changeLog.dateUpdated = datetime() SET changeSet.checkSum = $4",
                    changeSet.getId(),
                    changeSet.getAuthor(),
                    changeSet.getFilePath(),
                    changeSet.generateCheckSum(currentCheckSumVersion).toString()
            ));
            database.commit();
        } catch (LiquibaseException e) {
            throw new DatabaseException(String.format("Could not replace checksum of change set %s", changeSet), e);
        }
        reset();
    }

    @Override
    public void reset() {
        ranChangeSets = null;
        lastChangeSetSequenceValue = null;
    }

    @Override
    public void setExecType(ChangeSet changeSet, ChangeSet.ExecType execType) throws DatabaseException {
        if (execType == ChangeSet.ExecType.FAILED || execType == ChangeSet.ExecType.SKIPPED) {
            return;
        }
        try {
            int nextSequenceValue = getNextSequenceValue();
            if (execType == ChangeSet.ExecType.RERAN) {
                updateChangeSet(changeSet, execType, nextSequenceValue);
            } else {
                insertChangeSet(changeSet, execType, nextSequenceValue);
            }
            reLinkChangeSet(changeSet);
            database.commit();
        } catch (LiquibaseException e) {
            database.rollback();
            throw new DatabaseException(String.format("Could not persist change set %s with execution type %s", changeSet, execType), e);
        }
    }

    @Override
    public void removeFromHistory(ChangeSet changeSet) throws DatabaseException {
        try {
            database.execute(new RawParameterizedSqlStatement(
                    "MATCH (changeSet:__LiquibaseChangeSet {id: $1, author: $2, changeLog: $3 })-[:IN_CHANGELOG]->(changeLog:__LiquibaseChangeLog) " +
                    "SET changeLog.dateUpdated = datetime() DETACH DELETE changeSet",
                    changeSet.getId(),
                    changeSet.getAuthor(),
                    changeSet.getFilePath()
            ));
            database.commit();
        } catch (LiquibaseException e) {
            database.rollback();
            throw new DatabaseException(String.format("Could not remove change set %s from history", changeSet), e);
        }
        if (ranChangeSets != null) {
            ranChangeSets.remove(new RanChangeSet(changeSet));
        }
    }

    @Override
    public int getNextSequenceValue() throws LiquibaseException {
        if (lastChangeSetSequenceValue == null) {
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
            int value = (int) executor
                    .queryForLong(new RawSqlStatement(
                            "MATCH (:__LiquibaseChangeSet)-[execution:IN_CHANGELOG]->(:__LiquibaseChangeLog) "
                            + "RETURN MAX(execution.orderExecuted) AS value"));
            database.rollback();
            lastChangeSetSequenceValue = value;
        }
        return ++lastChangeSetSequenceValue;
    }

    @Override
    public void tag(String tagString) throws DatabaseException {
        Map<String, Object> changeSetIds = mergeTag(tagString);
        if (this.ranChangeSets != null && changeSetIds != null) {
            this.ranChangeSets.stream()
                    .filter(ranChangeSet -> ranChangeSet.getId().equals(changeSetIds.get("id"))
                            && ranChangeSet.getAuthor().equals(changeSetIds.get("author"))
                            && ranChangeSet.getChangeLog().equals(changeSetIds.get("changeLog")))
                    .findFirst()
                    .ifPresent(ranChangeSet -> ranChangeSet.setTag(tagString));
        }
    }

    @Override
    public boolean tagExists(String tag) throws DatabaseException {
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        long count = executor.queryForLong(new RawParameterizedSqlStatement("MATCH (t:__LiquibaseTag {tag: $1}) RETURN count(t) AS count", tag));
        database.rollback();
        return count > 0;
    }

    @Override
    public void clearAllCheckSums() throws LiquibaseException {
        database.execute(new RawSqlStatement(
                "MATCH (changeSet:__LiquibaseChangeSet)-[:IN_CHANGELOG]->(changeLog:__LiquibaseChangeLog) " +
                "SET changeLog.dateUpdated = datetime() " +
                "REMOVE changeSet.checkSum"));
        database.commit();
    }

    private List<RanChangeSet> mapRanChangeSets(List<Map<String, Object>> results) {
        return results.stream().map(this::mapRanChangeSet).collect(toList());
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readChangeSets(Executor executor) throws DatabaseException {
        return (List<Map<String, Object>>) executor.queryForList(
                new RawSqlStatement(
                        "MATCH (changeLog:__LiquibaseChangeLog) " +
                                "MATCH (changeSet:__LiquibaseChangeSet)-[changeSetExecution:IN_CHANGELOG]->(changeLog) " +
                                "OPTIONAL MATCH (tag:__LiquibaseTag)-[:TAGS]->(changeSet) " +
                                "OPTIONAL MATCH (label:__LiquibaseLabel)-[:LABELS]->(changeSet) " +
                                "OPTIONAL MATCH (context:__LiquibaseContext)-[:CONTEXTUALIZES]->(changeSet) " +
                                "WITH changeSet, changeSetExecution, tag.tag AS tag, collect(label.label) AS labels, collect(context.context) AS contexts " +
                                "ORDER BY changeSetExecution.dateExecuted ASC, changeSetExecution.orderExecuted ASC " +
                                "RETURN changeSet { " +
                                "   .changeLog, " +
                                "   .id, " +
                                "   .author, " +
                                "   .checkSum, " +
                                "   .execType, " +
                                "   .description, " +
                                "   .comments, " +
                                "   .deploymentId, " +
                                "   .storedChangeLog, " +
                                "   .liquibaseVersion, " +
                                "   orderExecuted: changeSetExecution.orderExecuted, " +
                                "   dateExecuted: changeSetExecution.dateExecuted, " +
                                "   tag: tag, " +
                                "   labels: labels, " +
                                "   contexts: contexts " +
                                "}"
                ),
                Map.class
        );
    }

    private void updateChangeSet(ChangeSet changeSet, ChangeSet.ExecType execType, int nextSequenceValue) throws LiquibaseException {
        database.execute(new RawParameterizedSqlStatement(
                "MATCH (changeLog:__LiquibaseChangeLog) " +
                        "SET changeLog.dateUpdated = datetime() " +
                        "WITH changeLog " +
                        "MATCH (changeSet:__LiquibaseChangeSet {id: $1, author: $2, changeLog: $3 })-[changeSetExecution:IN_CHANGELOG]->(changeLog) " +
                        "SET changeSetExecution.dateExecuted = datetime() " +
                        "SET changeSetExecution.orderExecuted = $4 " +
                        "SET changeSet.checkSum = $5 " +
                        "SET changeSet.execType = $6 " +
                        "SET changeSet.deploymentId = $7 ",
                changeSet.getId(),
                changeSet.getAuthor(),
                changeSet.getFilePath(),
                nextSequenceValue,
                changeSet.generateCheckSum(currentCheckSumVersion).toString(),
                execType.value,
                Scope.getCurrentScope().getDeploymentId()
        ));
    }

    private void insertChangeSet(ChangeSet changeSet, ChangeSet.ExecType execType, int nextSequenceValue) throws LiquibaseException {
        database.execute(new RawParameterizedSqlStatement(
                "MATCH (changeLog:__LiquibaseChangeLog) " +
                        "SET changeLog.dateUpdated = datetime() " +
                        "CREATE (changeSet:__LiquibaseChangeSet {" +
                        "   changeLog: $1, " +
                        "   id: $2," +
                        "   author: $3," +
                        "   checkSum: $4," +
                        "   execType: $5, " +
                        "   description: $6, " +
                        "   comments: $7, " +
                        "   deploymentId: $8, " +
                        "   storedChangeLog: $9, " +
                        "   liquibaseVersion: $10 " +
                        "})-[:IN_CHANGELOG {" +
                        "   dateExecuted: datetime(), " +
                        "   orderExecuted: $11 " +
                        "}]->(changeLog)",
                changeSet.getFilePath(),
                changeSet.getId(),
                changeSet.getAuthor(),
                changeSet.generateCheckSum(currentCheckSumVersion).toString(),
                execType.value,
                changeSet.getDescription(),
                changeSet.getComments(),
                Scope.getCurrentScope().getDeploymentId(),
                changeSet.getStoredFilePath(),
                getLiquibaseVersion(),
                nextSequenceValue
        ));
    }

    private void reLinkChangeSet(ChangeSet changeSet) throws LiquibaseException {
        database.execute(new RawParameterizedSqlStatement(
                "MATCH (changeSet:__LiquibaseChangeSet {id: $1, author: $2, changeLog: $3 })-[:IN_CHANGELOG]->(:__LiquibaseChangeLog) " +
                        "OPTIONAL MATCH (changeSet)<-[c:CONTEXTUALIZES]-(:__LiquibaseContext) DELETE c " +
                        "WITH changeSet " +
                        "OPTIONAL MATCH (changeSet)<-[l:LABELS]-(:__LiquibaseLabel) DELETE l ",
                changeSet.getId(),
                changeSet.getAuthor(),
                changeSet.getFilePath()
        ));
        linkContexts(changeSet);
        linkLabels(changeSet);
        linkTag(changeSet);
    }

    private void linkContexts(ChangeSet changeSet) throws LiquibaseException {
        ContextExpression contexts = changeSet.getContextFilter();
        if (contexts == null) {
            return;
        }
        for (String context : contexts.getContexts()) {
            database.execute(new RawParameterizedSqlStatement(
                    "MATCH (changeSet:__LiquibaseChangeSet {id: $1, author: $2, changeLog: $3 }) " +
                            "MERGE (context:__LiquibaseContext{ context: $4 }) " +
                            "   ON CREATE SET context.dateCreated = datetime() " +
                            "   ON MATCH SET context.dateUpdated = datetime() " +
                            "CREATE (context)-[:CONTEXTUALIZES]->(changeSet)",
                    changeSet.getId(),
                    changeSet.getAuthor(),
                    changeSet.getFilePath(),
                    context
            ));
        }
    }

    private void linkLabels(ChangeSet changeSet) throws LiquibaseException {
        Labels labels = changeSet.getLabels();
        if (labels == null) {
            return;
        }
        for (String label : labels.getLabels()) {
            database.execute(new RawParameterizedSqlStatement(
                    "MATCH (changeSet:__LiquibaseChangeSet {id: $1, author: $2, changeLog: $3 }) " +
                            "MERGE (label:__LiquibaseLabel{ label: $4 }) " +
                            "   ON CREATE SET label.dateCreated = datetime() " +
                            "   ON MATCH SET label.dateUpdated = datetime() " +
                            "CREATE (label)-[:LABELS]->(changeSet)",
                    changeSet.getId(),
                    changeSet.getAuthor(),
                    changeSet.getFilePath(),
                    label
            ));
        }
    }

    /**
     * Upserts the tag change of the change set, if any
     * If the tag already exists, it is disconnected from any prior change set
     * If the change set is already tagged, it is first untagged
     * @param changeSet the change set to possibly tag
     * @throws LiquibaseException if more than one tag change is found or if the query execution goes wrong
     */
    private void linkTag(ChangeSet changeSet) throws LiquibaseException {
        List<String> tagValues = changeSet.getChanges().stream()
                .filter(TagDatabaseChange.class::isInstance)
                .map(change -> ((TagDatabaseChange) change).getTag())
                .collect(toList());
        if (tagValues.size() > 1) {
            throw new LiquibaseException(String.format("A change set can only declare one tag, but found [%s]", String.join(", ", tagValues)));
        }
        if (tagValues.isEmpty()) {
            return;
        }

        String tag = tagValues.iterator().next();
        database.execute(new RawParameterizedSqlStatement(
                "MERGE (tag:__LiquibaseTag {tag: $1}) " +
                        "   ON CREATE SET tag.dateCreated = datetime()" +
                        "   ON MATCH SET tag.dateUpdated = datetime() " +
                        "WITH tag " +
                        "OPTIONAL MATCH (tag)-[r:TAGS]->(:__LiquibaseChangeSet) DELETE r " +
                        "WITH tag " +
                        "MATCH (changeSet:__LiquibaseChangeSet {id: $2, author: $3, changeLog: $4 }) " +
                        "OPTIONAL MATCH (changeSet)<-[r:TAGS]-(:__LiquibaseTag) DELETE r " +
                        "CREATE (tag)-[:TAGS]->(changeSet)",
                tag,
                changeSet.getId(),
                changeSet.getAuthor(),
                changeSet.getFilePath()
        ));
    }

    /**
     * Upserts the tag and links to the most recently persisted change set
     * If the tag already exists, it is disconnected from any prior change set
     * If the change set is already tagged, it is first untagged
     *
     * @param tagString the tag value to persist
     * @return the tagged change set identifying attributes (ID, author, change log) or <code>null</code> if no change
     * set could be found
     */
    private Map<String, Object> mergeTag(String tagString) throws DatabaseException {
        try {
            Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> changeSetIds = executor.queryForList(new RawParameterizedSqlStatement(
                    "MERGE (tag:__LiquibaseTag {tag: $1}) " +
                            "   ON CREATE SET tag.dateCreated = datetime() " +
                            "   ON MATCH SET tag.dateUpdated = datetime() " +
                            "WITH tag " +
                            "OPTIONAL MATCH (tag)-[r:TAGS]->(:__LiquibaseChangeSet) DELETE r " +
                            "WITH tag " +
                            "MATCH (changeSet:__LiquibaseChangeSet)-[execution:IN_CHANGELOG]->(changeLog:__LiquibaseChangeLog) " +
                            "SET changeLog.dateUpdated = datetime() " +
                            "WITH tag, changeSet " +
                            "ORDER BY execution.dateExecuted DESC, execution.orderExecuted DESC " +
                            "LIMIT 1 " +
                            "MERGE (tag)-[:TAGS]->(changeSet) " +
                            "RETURN changeSet {.id, .author, .changeLog}", tagString),
                    Map.class
            );
            database.commit();
            return changeSetIds.size() == 0 ? null : changeSetIds.iterator().next();
        } catch (LiquibaseException e) {
            database.rollback();
            throw new DatabaseException(String.format("Could not create tag with value: %s", tagString), e);
        }
    }

    private RanChangeSet mapRanChangeSet(Map<String, Object> row) {
        @SuppressWarnings("unchecked")
        Collection<String> contexts = (Collection<String>) row.get("contexts");
        @SuppressWarnings("unchecked")
        Collection<String> labels = (Collection<String>) row.get("labels");
        RanChangeSet ranChangeSet = new RanChangeSet(
                (String) row.get("changeLog"),
                (String) row.get("id"),
                (String) row.get("author"),
                CheckSum.parse((String) row.get("checkSum")),
                Date.from(((Timestamp) row.get("dateExecuted")).toInstant()),
                (String) row.get("tag"),
                ChangeSet.ExecType.valueOf((String) row.get("execType")),
                (String) row.get("description"),
                (String) row.get("comments"),
                new ContextExpression(contexts),
                new Labels(labels),
                (String) row.get("deploymentId"),
                (String) row.get("storedChangeLog")
        );
        ranChangeSet.setOrderExecuted(Integer.valueOf(row.get("orderExecuted").toString(), 10));
        ranChangeSet.setLiquibaseVersion(row.get("liquibaseVersion").toString());
        return ranChangeSet;
    }


    private void createConstraints() throws DatabaseException {
        database.createUniqueConstraint(TAG_CONSTRAINT_NAME, "__LiquibaseTag", "tag");
        database.createUniqueConstraint(CONTEXT_CONSTRAINT_NAME, "__LiquibaseContext", "context");
        database.createUniqueConstraint(LABEL_CONSTRAINT_NAME, "__LiquibaseLabel", "label");
        database.createNodeKeyConstraint(CHANGE_SET_CONSTRAINT_NAME, "__LiquibaseChangeSet", "id", "author", "changeLog");
    }


    private void createIndices() throws DatabaseException {
        database.createIndex(CHANGE_SET_CHECK_SUM_INDEX_NAME, "__LiquibaseChangeSet", "checkSum");
    }

    private void initializeHistory() throws DatabaseException {
        try {
            database.execute(new RawSqlStatement("MERGE (changeLog:__LiquibaseChangeLog) " +
                    "   ON CREATE SET changeLog.dateCreated = datetime() " +
                    "   ON MATCH SET changeLog.dateUpdated = datetime()"));
            database.commit();
        } catch (LiquibaseException e) {
            database.rollback();
            throw new DatabaseException("Could not upsert changelog", e);
        }
    }

    private void cleanUpGraph() throws DatabaseException {
        removeDisconnectedLabels();
        removeDisconnectedContexts();
    }

    private void removeDisconnectedLabels() throws DatabaseException {
        try {
            database.execute(new RawSqlStatement("MATCH (label:__LiquibaseLabel) " +
                    "WHERE NOT (label)--() " +
                    "DELETE label"));
            database.commit();
        } catch (LiquibaseException e) {
            database.rollback();
            throw new DatabaseException("Could not delete disconnected labels", e);
        }
    }

    private void removeDisconnectedContexts() throws DatabaseException {
        try {
            database.execute(new RawSqlStatement("MATCH (context:__LiquibaseContext) " +
                    "WHERE NOT (context)--() " +
                    "DELETE context"));
            database.commit();
        } catch (LiquibaseException e) {
            database.rollback();
            throw new DatabaseException("Could not delete disconnected labels", e);
        }
    }

    private void removeHistory() throws DatabaseException {
        try {
            database.execute(new RawSqlStatement("MATCH (changeLog:__LiquibaseChangeLog) DETACH DELETE changeLog"));
            database.execute(new RawSqlStatement("MATCH (changeSet:__LiquibaseChangeSet) DETACH DELETE changeSet"));
            database.execute(new RawSqlStatement("MATCH (label:__LiquibaseLabel)         DETACH DELETE label"));
            database.execute(new RawSqlStatement("MATCH (context:__LiquibaseContext)     DETACH DELETE context"));
            database.execute(new RawSqlStatement("MATCH (tag:__LiquibaseTag)             DETACH DELETE tag"));
            database.commit();
        } catch (LiquibaseException e) {
            database.rollback();
            throw new DatabaseException("Could not delete history", e);
        }
    }

    private void removeIndices() throws DatabaseException {
        database.dropIndex(CHANGE_SET_CHECK_SUM_INDEX_NAME, "__LiquibaseChangeSet", "checkSum");
        database.commit();
    }

    private void removeConstraints() throws DatabaseException {
        database.dropUniqueConstraint(TAG_CONSTRAINT_NAME, "__LiquibaseTag", "tag");
        database.dropUniqueConstraint(CONTEXT_CONSTRAINT_NAME, "__LiquibaseContext", "context");
        database.dropUniqueConstraint(LABEL_CONSTRAINT_NAME, "__LiquibaseLabel", "label");
        database.dropNodeKeyConstraint(CHANGE_SET_CONSTRAINT_NAME, "__LiquibaseChangeSet", "id", "author", "changeLog");
        database.commit();
    }

    private void readCheckSums() throws DatabaseException {
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        boolean result = executor.queryForObject(
                new RawParameterizedSqlStatement(
                        "MATCH (changeSet:__LiquibaseChangeSet)-[:IN_CHANGELOG]->(: __LiquibaseChangeLog)" +
                                " WHERE NOT changeSet.checkSum STARTS WITH $1" +
                                " RETURN count(changeSet) > 0",
                        String.format("%d:", currentCheckSumVersion.getVersion())
                ),
                Boolean.class
        );
        database.rollback();
        containsOlderCheckSums = result;
    }

    private String getLiquibaseVersion() {
        return LiquibaseUtil.getBuildVersion()
                .replaceAll("SNAPSHOT", "SNP")
                .replaceAll("beta", "b")
                .replaceAll("alpha", "b");
    }
}
