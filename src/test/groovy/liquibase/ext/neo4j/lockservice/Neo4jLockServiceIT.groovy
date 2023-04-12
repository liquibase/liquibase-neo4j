package liquibase.ext.neo4j.lockservice


import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.ReflectionUtils
import org.neo4j.driver.exceptions.ClientException

import java.time.ZonedDateTime

import static java.time.temporal.ChronoUnit.MINUTES
import static liquibase.ext.neo4j.DateUtils.date
import static liquibase.ext.neo4j.DateUtils.nowMinus
import static liquibase.ext.neo4j.lockservice.Neo4jLockService.LOCK_CONSTRAINT_NAME

class Neo4jLockServiceIT extends Neo4jContainerSpec {

    def neo4jLockService = new Neo4jLockService()

    def setup() {
        neo4jLockService.setDatabase(database)
    }

    def cleanup() {
        queryRunner.dropUniqueConstraint(LOCK_CONSTRAINT_NAME, "__LiquibaseLock", "lockedBy")
    }

    def "acquires lock from the underlying database"() {
        when:
        def acquiredLock = neo4jLockService.acquireLock()

        then:
        acquiredLock

        when:
        def lockNodeProperties = queryRunner.getSingleRow(
                "MATCH (l:__LiquibaseLock) " +
                        "RETURN l.id AS id, l.lockedBy AS lockedBy, l.grantDate AS grantDate")

        then:
        lockNodeProperties["id"] != null
        lockNodeProperties["grantDate"] < ZonedDateTime.now()
        lockNodeProperties["lockedBy"] == "Neo4jLockService"
    }

    def "ensures Liquibase lock node uniqueness after initialization"() {
        given:
        neo4jLockService.init()

        when:
        2.times {
            queryRunner.run("CREATE (:__LiquibaseLock { lockedBy: 'Neo4jLockService' })")
        }

        then:
        def e = thrown(ClientException)
        e.message.contains("already exists with label `__LiquibaseLock` and property `lockedBy` = 'Neo4jLockService'")
    }

    def "has lock if lock UUID is set"() {
        given:
        ReflectionUtils.setField("lockId", neo4jLockService, new UUID(42, 42))

        when:
        def hasLock = neo4jLockService.hasChangeLogLock()

        then:
        hasLock
    }

    def "does not have lock if lock UUID is unset"() {
        when:
        def hasLock = neo4jLockService.hasChangeLogLock()

        then:
        !hasLock
    }

    def "does not acquire lock on subsequent calls"() {
        given:
        def lockIds = new HashSet()

        when:
        2.times {
            neo4jLockService.acquireLock()
            lockIds << ReflectionUtils.getField("lockId", neo4jLockService)
        }

        then:
        lockIds.size() == 1
    }

    def "cleans up database upon lock release"() {
        given:
        manuallyRegisterLock()

        when:
        neo4jLockService.releaseLock()

        then:
        !neo4jLockService.hasChangeLogLock()
        countLockNodes() == 0L
    }

    def "cleans up database upon lock release, despite 2nd unsuccessful lock acquisition (1st lock ID is retained)"() {
        given:
        2.times {
            neo4jLockService.acquireLock()
        }

        expect:
        countLockNodes() == 1L

        when:
        neo4jLockService.releaseLock()

        then:
        !neo4jLockService.hasChangeLogLock()
        countLockNodes() == 0L
    }

    def "resets lock service state"() {
        given:
        ReflectionUtils.setField("lockId", neo4jLockService, new UUID(44, 40))

        when:
        neo4jLockService.reset()

        then:
        !neo4jLockService.hasChangeLogLock()
    }

    def "removes constraints and locks upon destroy call"() {
        given:
        queryRunner.createUniqueConstraint(LOCK_CONSTRAINT_NAME, "__LiquibaseLock", "lockedBy")
        manuallyRegisterLock()
        queryRunner.run('''CREATE (:__LiquibaseLock { id: $id, name: 'ExtraFakeLock', lockedBy: 'ExtraFakeLock', grantDate: datetime() })''',
                [id: UUID.randomUUID().toString()])

        when:
        neo4jLockService.destroy()

        then:
        !neo4jLockService.hasChangeLogLock()
        countLockNodes() == 0L
        def constraintDescriptions = queryRunner.listExistingConstraints()
        constraintDescriptions.findIndexOf { it.contains(":__LiquibaseLock") } == -1
    }

    def "does not fail upon destroy call before database and service are initialized"() {
        when:
        neo4jLockService.destroy()

        then:
        !neo4jLockService.hasChangeLogLock()
        countLockNodes() == 0L
        def constraintDescriptions = queryRunner.listExistingConstraints()
        !constraintDescriptions.contains(LOCK_CONSTRAINT_NAME)
        !constraintDescriptions.contains(String.format("%s_bc", LOCK_CONSTRAINT_NAME))
    }

    def "forcibly releases all locks"() {
        given:
        manuallyRegisterLock()
        queryRunner.run('''CREATE (:__LiquibaseLock { id: $id, lockedBy: 'ExtraFakeLock', grantDate: datetime() })''',
                [id: UUID.randomUUID().toString()])

        expect:
        countLockNodes() == 2L

        when:
        neo4jLockService.forceReleaseLock()

        then:
        !neo4jLockService.hasChangeLogLock()
        countLockNodes() == 0L
    }

    def "lists all locks"() {
        given:
        manuallyRegisterLock()
        def extraLockId = UUID.randomUUID()
        queryRunner.run('''CREATE (:__LiquibaseLock { id: $id, lockedBy: 'ExtraFakeLock', grantDate: datetime({year:1986, month:3, day:4})})''',
                [id: extraLockId.toString()])

        when:
        def locks = neo4jLockService.listLocks()

        then:
        locks.size() == 2
        verifyAll(locks[0]) {
            id == extraLockId.hashCode()
            lockGranted == date(1986, 3, 4)
            lockedBy == "ExtraFakeLock"
        }
        verifyAll(locks[1]) {
            lockGranted > nowMinus(1, MINUTES)
            lockedBy == "Neo4jLockService"
        }
    }

    def "waits for lock"() {
        given:
        neo4jLockService.setChangeLogLockWaitTime(1)
        neo4jLockService.setChangeLogLockRecheckTime(1)

        when:
        neo4jLockService.waitForLock()

        then:
        neo4jLockService.hasChangeLogLock()
        countLockNodes() == 1L
        def constraints = queryRunner.listExistingConstraints()
        constraints.findIndexOf { it.contains(":__LiquibaseLock") } >= 0
    }

    private Object countLockNodes() {
        def row = queryRunner.getSingleRow("MATCH (l:__LiquibaseLock) RETURN count(l) AS count")
        return row["count"]
    }

    private void manuallyRegisterLock() {
        def lockId = UUID.randomUUID()
        ReflectionUtils.setField("lockId", neo4jLockService, lockId)
        queryRunner.run(
                '''CREATE (:__LiquibaseLock { id: $uuid, lockedBy: 'Neo4jLockService', grantDate: datetime() })''',
                [uuid: lockId.toString()]
        )
    }
}
