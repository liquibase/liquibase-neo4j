package liquibase.ext.neo4j.lockservice;

import liquibase.GlobalConfiguration;
import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.LockException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.lockservice.LockService;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.statement.core.RawSqlStatement;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.function.Predicate.isEqual;

public class Neo4jLockService implements LockService {

    static final String LOCK_CONSTRAINT_NAME = "unique_liquibase_lock";

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
        Exception lastException;
        boolean lockAcquired;
        try (ConditionCheckScheduler checkScheduler = new ConditionCheckScheduler(Duration.ofMinutes(getChangeLogLockWaitTime()))) {
            lockAcquired = checkScheduler.scheduleCheckWithFixedDelay(
                    this::acquireLock,
                    isEqual(true),
                    false,
                    Duration.ofSeconds(getChangeLogLockRecheckTime())
            );
            lastException = checkScheduler.getLastException();
        }

        if (!lockAcquired) {
            DatabaseChangeLogLock[] currentLocks = listLocks();
            if (currentLocks.length > 0) {
                throw new LockException(String.format(
                        "Could not acquire change log lock. Currently locked by %s",
                        currentLocks[0].getLockedBy()));
            }
            throw new LockException("Could not acquire change log lock despite no lock currently stored", lastException);
        }
    }

    @Override
    public void setChangeLogLockWaitTime(long changeLogLockWaitTime) {
        changeLogLockPollRate = changeLogLockWaitTime;
    }

    public Long getChangeLogLockWaitTime() {
        if (changeLogLockPollRate != null) {
            return changeLogLockPollRate;
        }
        return GlobalConfiguration.CHANGELOGLOCK_WAIT_TIME.getCurrentValue();
    }

    @Override
    public void setChangeLogLockRecheckTime(long changeLogLocRecheckTime) {
        this.changeLogLockRecheckTime = changeLogLocRecheckTime;
    }

    public Long getChangeLogLockRecheckTime() {
        if (changeLogLockRecheckTime != null) {
            return changeLogLockRecheckTime;
        }
        return GlobalConfiguration.CHANGELOGLOCK_POLL_RATE.getCurrentValue();
    }

    @Override
    public boolean acquireLock() throws LockException {
        if (hasChangeLogLock()) {
            return true;
        }

        UUID formerLockId = lockId;
        try {
            init();
            UUID newLockId = UUID.randomUUID();
            database.execute(new RawParameterizedSqlStatement(
                    "CREATE (lock:__LiquibaseLock {id: $0, grantDate: DATETIME(), lockedBy: $1})",
                    newLockId.toString(),
                    Neo4jLockService.class.getSimpleName()
            ));
            database.commit();
            lockId = newLockId;
            return true;
        } catch (LiquibaseException e) {
            try {
                database.rollback();
            } catch (DatabaseException databaseException) {
                e.addSuppressed(databaseException);
            }
            lockId = formerLockId;
            if (e.getMessage().contains("already exists")) {
                return false;
            }
            throw new LockException("Could not acquire lock", e);
        }
    }

    @Override
    public void init() throws DatabaseException {
        database.createUniqueConstraint(LOCK_CONSTRAINT_NAME, "__LiquibaseLock", "lockedBy");
    }

    @Override
    public void destroy() throws DatabaseException {
        database.dropUniqueConstraint(LOCK_CONSTRAINT_NAME, "__LiquibaseLock", "lockedBy");
        try {
            forceReleaseLock();
        } catch (LockException e) {
            DatabaseException databaseException = new DatabaseException("Could not release lock upon destroy", e);
            try {
                database.rollback();
            } catch (DatabaseException de) {
                databaseException.addSuppressed(de);
            }
            throw databaseException;
        }
    }

    @Override
    public void releaseLock() throws LockException {
        if (!hasChangeLogLock()) {
            return;
        }
        try {
            database.execute(new RawParameterizedSqlStatement(
                    "MATCH (lock:__LiquibaseLock {id: $0}) DELETE lock",
                    lockId.toString()
            ));
            database.commit();
            reset();
        } catch (LiquibaseException e) {
            try {
                database.rollback();
            } catch (DatabaseException databaseException) {
                e.addSuppressed(databaseException);
            }
            throw new LockException("Could not release lock", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public DatabaseChangeLogLock[] listLocks() throws LockException {
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        try {
            RawSqlStatement cypher = new RawSqlStatement("MATCH (lock:__LiquibaseLock) WITH lock ORDER BY lock.grantDate ASC RETURN COLLECT(lock) AS locks");
            List<Map<String, Object>> results = executor.queryForObject(cypher, List.class);
            return results.stream().map(this::mapRow).toArray(DatabaseChangeLogLock[]::new);
        } catch (DatabaseException e) {
            throw new LockException("Could not list locks", e);
        }
    }

    @Override
    public void forceReleaseLock() throws LockException {
        try {
            database.execute(new RawSqlStatement("MATCH (lock:__LiquibaseLock) DELETE lock"));
            database.commit();
            reset();
        } catch (LiquibaseException e) {
            throw new LockException("Could not force release lock", e);
        }
    }

    @Override
    public void reset() {
        lockId = null;
    }

    @Override
    public boolean hasChangeLogLock() {
        return lockId != null;
    }

    private DatabaseChangeLogLock mapRow(Map<String, Object> row) {
        int id = UUID.fromString(row.get("id").toString()).hashCode();
        Timestamp grantDate = (Timestamp) row.get("grantDate");
        String lockedBy = row.get("lockedBy").toString();
        return new DatabaseChangeLogLock(id, grantDate, lockedBy);
    }
}
