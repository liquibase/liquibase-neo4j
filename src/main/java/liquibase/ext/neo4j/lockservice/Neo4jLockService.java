package liquibase.ext.neo4j.lockservice;

import liquibase.Scope;
import liquibase.configuration.GlobalConfiguration;
import liquibase.configuration.LiquibaseConfiguration;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.lockservice.LockService;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static java.util.function.Predicate.isEqual;
import static liquibase.ext.neo4j.lockservice.Exceptions.ignoring;
import static liquibase.ext.neo4j.lockservice.Exceptions.messageContaining;

public class Neo4jLockService implements LockService {

    static final String CONSTRAINT_NAME = "unique_liquibase_lock";

    private Neo4jDatabase database;

    private UUID lockId;

    private Long changeLogLockPollRate;

    private Long changeLogLockRecheckTime;

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
    public void waitForLock() throws LockException {
        ConditionCheckScheduler checkScheduler = new ConditionCheckScheduler(Duration.ofMinutes(this.getChangeLogLockWaitTime()));
        boolean lockAcquired = checkScheduler.scheduleCheckWithFixedDelay(
                this::acquireLock,
                isEqual(true),
                false,
                Duration.ofSeconds(this.getChangeLogLockRecheckTime())
        );

        if (!lockAcquired) {
            DatabaseChangeLogLock[] currentLocks = this.listLocks();
            if (currentLocks.length > 0) {
                throw new LockException(String.format(
                        "Could not acquire change log lock. Currently locked by %s",
                        currentLocks[0].getLockedBy()));
            }
            throw new LockException("Could not acquire change log lock despite no lock currently stored");
        }
    }

    @Override
    public void setChangeLogLockWaitTime(long changeLogLockWaitTime) {
        this.changeLogLockPollRate = changeLogLockWaitTime;
    }

    public Long getChangeLogLockWaitTime() {
        if (changeLogLockPollRate != null) {
            return changeLogLockPollRate;
        }
        return LiquibaseConfiguration.getInstance().getConfiguration(GlobalConfiguration.class)
                .getDatabaseChangeLogLockWaitTime();
    }

    @Override
    public void setChangeLogLockRecheckTime(long changeLogLocRecheckTime) {
        this.changeLogLockRecheckTime = changeLogLocRecheckTime;
    }

    public Long getChangeLogLockRecheckTime() {
        if (changeLogLockRecheckTime != null) {
            return changeLogLockRecheckTime;
        }
        return LiquibaseConfiguration.getInstance().getConfiguration(GlobalConfiguration.class)
                .getDatabaseChangeLogLockPollRate();
    }

    @Override
    public boolean acquireLock() throws LockException {
        if (hasChangeLogLock()) {
            return true;
        }

        UUID formerLockId = this.lockId;
        try {
            this.init();
            UUID newLockId = UUID.randomUUID();
            run(query(String.format(
                    "CREATE (lock:__LiquibaseLock:__LiquigraphLock {id: '%s', grant_date: DATETIME(), locked_by: '%2$s', name: '%2$s'})",
                    newLockId.toString(),
                    Neo4jLockService.class.getSimpleName()
            )));
            database.commit();
            this.lockId = newLockId;
            return true;
        } catch (LiquibaseException e) {
            this.lockId = formerLockId;
            if (e.getMessage().contains("already exists")) {
                return false;
            }
            throw new LockException("Could not acquire lock", e);
        }
    }

    @Override
    public void init() throws DatabaseException {
        try {
            createConstraints();
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not initialize lock", e);
        }
    }

    @Override
    public void destroy() throws DatabaseException {
        try {
            removeConstraints();
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not remove lock constraints upon destroy", e);
        }
        try {
            this.releaseLock();
        } catch (LockException e) {
            throw new DatabaseException("Could not release lock upon destroy", e);
        }
    }

    @Override
    public void releaseLock() throws LockException {
        if (!hasChangeLogLock()) {
            return;
        }
        try {
            run(query(String.format(
                    "MATCH (lock:__LiquibaseLock:__LiquigraphLock {id: '%s'}) DELETE lock",
                    this.lockId.toString()
            )));
            database.commit();
            this.reset();
        } catch (LiquibaseException e) {
            throw new LockException("Could not release lock", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public DatabaseChangeLogLock[] listLocks() throws LockException {
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        try {
            List<Map<String, Object>> results = executor.queryForObject(new RawSqlStatement("MATCH (lock:__LiquibaseLock:__LiquigraphLock) WITH lock ORDER BY lock.grant_date ASC RETURN COLLECT(lock) AS locks"), List.class);
            return results.stream().map(this::mapRow).toArray(DatabaseChangeLogLock[]::new);
        } catch (DatabaseException e) {
            throw new LockException("Could not list locks", e);
        }
    }

    @Override
    public void forceReleaseLock() throws LockException {
        try {
            run(query("MATCH (lock:__LiquibaseLock:__LiquigraphLock) DELETE lock"));
            database.commit();
            this.reset();
        } catch (LiquibaseException e) {
            throw new LockException("Could not force release lock", e);
        }
    }

    @Override
    public void reset() {
        this.lockId = null;
    }

    @Override
    public boolean hasChangeLogLock() {
        return lockId != null;
    }

    private void createConstraints() throws LiquibaseException {
        String neo4jVersion = database.getNeo4jVersion();
        if (neo4jVersion.startsWith("3.5")) {
            createConstraintsForNeo4j3();
        } else if (neo4jVersion.startsWith("4")) {
            createConstraintsForNeo4j4();
        } else {
            throw new DatabaseException(String.format(
                    "Init aborted: Neo4j version %s is not supported",
                    neo4jVersion
            ));
        }
    }

    private void removeConstraints() throws LiquibaseException {
        String neo4jVersion = database.getNeo4jVersion();
        if (neo4jVersion.startsWith("3.5")) {
            removeConstraintsForNeo4j3();
        } else if (neo4jVersion.startsWith("4")) {
            removeConstraintsForNeo4j4();
        } else {
            throw new DatabaseException(String.format(
                    "Init aborted: Neo4j version %s is not supported",
                    neo4jVersion
            ));
        }
    }

    private void createConstraintsForNeo4j3() throws LiquibaseException {
        Predicate<Exception> constraintExists = messageContaining("constraint already exists");
        // before 4.x, constraints cannot be given names
        ignoring(constraintExists, () -> run(query("CREATE CONSTRAINT ON (lock:__LiquibaseLock) ASSERT lock.locked_by IS UNIQUE")));
        ignoring(constraintExists, () -> run(query("CREATE CONSTRAINT ON (lock:__LiquigraphLock) ASSERT lock.name IS UNIQUE")));
        database.commit();
    }

    private void createConstraintsForNeo4j4() throws LiquibaseException {
        Predicate<Exception> constraintExists = messageContaining("constraint already exists");
        // `CREATE CONSTRAINT IF NOT EXISTS` is only available with Neo4j 4.2+
        ignoring(constraintExists, () -> run(query("CREATE CONSTRAINT %s ON (lock:__LiquibaseLock) ASSERT lock.locked_by IS UNIQUE", CONSTRAINT_NAME)));
        ignoring(constraintExists, () -> run(query("CREATE CONSTRAINT %s_bc ON (lock:__LiquigraphLock) ASSERT lock.name IS UNIQUE", CONSTRAINT_NAME)));
        database.commit();
    }

    private void removeConstraintsForNeo4j4() throws LiquibaseException {
        Predicate<Exception> constraintNotFoundError = messageContaining("no such constraint");
        ignoring(constraintNotFoundError, () -> run(query("DROP CONSTRAINT %s", CONSTRAINT_NAME)));
        ignoring(constraintNotFoundError, () -> run(query("DROP CONSTRAINT %s_bc", CONSTRAINT_NAME)));
        database.commit();
    }

    private void removeConstraintsForNeo4j3() throws LiquibaseException {
        Predicate<Exception> constraintNotFoundError = messageContaining("no such constraint");
        // before 4.x, constraints cannot be given names
        ignoring(constraintNotFoundError, () -> run(query("DROP CONSTRAINT ON (lock:__LiquibaseLock) ASSERT lock.locked_by IS UNIQUE")));
        ignoring(constraintNotFoundError, () -> run(query("DROP CONSTRAINT ON (lock:__LiquigraphLock) ASSERT lock.name IS UNIQUE")));
        database.commit();
    }

    private void run(SqlStatement[] sqlStatements) throws LiquibaseException {
        database.execute(sqlStatements, Collections.emptyList());
    }

    private SqlStatement[] query(String cypher, Object... arguments) {
        return new SqlStatement[]{new RawSqlStatement(String.format(cypher, arguments))};
    }

    private DatabaseChangeLogLock mapRow(Map<String, Object> row) {
        int id = UUID.fromString(row.get("id").toString()).hashCode();
        Timestamp grantDate = (Timestamp) row.get("grant_date");
        String lockedBy = row.get("locked_by").toString();
        return new DatabaseChangeLogLock(id, grantDate, lockedBy);
    }
}
