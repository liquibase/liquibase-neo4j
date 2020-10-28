package liquibase.ext.neo4j.lockservice

import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.ext.neo4j.DockerNeo4j
import org.apache.groovy.util.Maps
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.exceptions.ClientException
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Neo4jContainer
import spock.lang.Shared
import spock.lang.Specification

import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.function.Predicate
import java.util.logging.LogManager

import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.lockservice.Neo4jLockService.CONSTRAINT_NAME

class Neo4jLockServiceTest extends Specification {

    static {
        LogManager.getLogManager().reset();
    }

    private static final String PASSWORD = "s3cr3t"

    private static final TIMEZONE = ZoneId.of("Europe/Paris")

    @Shared
    GenericContainer<Neo4jContainer> neo4jContainer = DockerNeo4j.container(PASSWORD, TIMEZONE)

    @Shared
    Driver driver

    Database database

    def neo4jLockService = new Neo4jLockService()

    def setupSpec() {
        neo4jContainer.start()
        driver = driver()
    }

    def cleanupSpec() {
        driver.close()
        neo4jContainer.stop()
    }

    def setup() {
        database = DatabaseFactory.instance.openDatabase(
                "jdbc:neo4j:" + neo4jContainer.getBoltUrl(),
                "neo4j",
                PASSWORD,
                null,
                null
        )
        neo4jLockService.setDatabase(database)
    }

    def cleanup() {
        run("MATCH (n) DETACH DELETE n")
        manuallyRemoveConstraints()
        database.close()
    }

    def "acquires lock from the underlying database"() {
        when:
        def acquiredLock = neo4jLockService.acquireLock()

        then:
        acquiredLock

        when:
        def lockNodeProperties = querySingleRow(
                "MATCH (l:__LiquibaseLock:__LiquigraphLock) " +
                        "RETURN l.id AS id, l.locked_by AS locked_by, l.grant_date AS grant_date")

        then:
        lockNodeProperties["id"] != null
        lockNodeProperties["grant_date"] < ZonedDateTime.now()
        lockNodeProperties["locked_by"] == "Neo4jLockService"
    }

    def "ensures Liquibase lock node uniqueness after initialization"() {
        given:
        neo4jLockService.init()

        when:
        2.times {
            run("CREATE (:__LiquibaseLock { locked_by: 'Neo4jLockService' })")
        }

        then:
        def e = thrown(ClientException)
        e.message.contains("already exists with label `__LiquibaseLock` and property `locked_by` = 'Neo4jLockService'")
    }

    def "ensures Liquigraph lock node uniqueness after initialization (for backward compatibility)"() {
        given:
        neo4jLockService.init()

        when:
        2.times {
            run("CREATE (:__LiquigraphLock { name: 'Neo4jLockService' })")
        }

        then:
        def e = thrown(ClientException)
        e.message.contains("already exists with label `__LiquigraphLock` and property `name` = 'Neo4jLockService'")
    }

    def "has lock if lock UUID is set"() {
        given:
        setField("lockId", neo4jLockService, new UUID(42, 42))

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

    def "does not acquire lock a second time but does not fail either"() {
        given:
        def lockIds = new Object[2]

        when:
        2.times {
            neo4jLockService.acquireLock()
            lockIds[it] = getField("lockId", neo4jLockService)
        }

        then:
        lockIds.length == 2
        lockIds[0] == lockIds[1]
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
        setField("lockId", neo4jLockService, new UUID(44, 40))

        when:
        neo4jLockService.reset()

        then:
        !neo4jLockService.hasChangeLogLock()
    }

    def "removes constraints and locks upon destroy call"() {
        given:
        manuallyDefineConstraints()
        manuallyRegisterLock()

        when:
        neo4jLockService.destroy()

        then:
        !neo4jLockService.hasChangeLogLock()
        countLockNodes() == 0L
        def constraintDescriptions = existingConstraintDescriptions()
        constraintDescriptions.findIndexOf {it.contains("__liquibaselock:__LiquibaseLock")} == -1
        constraintDescriptions.findIndexOf {it.contains("__liquigraphlock:__LiquigraphLock")} == -1
    }

    def "does not fail upon destroy call before database and service are initialized"() {
        when:
        neo4jLockService.destroy()

        then:
        !neo4jLockService.hasChangeLogLock()
        countLockNodes() == 0L
        def constraintDescriptions = existingConstraintDescriptions()
        !constraintDescriptions.contains(CONSTRAINT_NAME)
        !constraintDescriptions.contains(String.format("%s_bc", CONSTRAINT_NAME))
    }

    def "forcibly releases all locks"() {
        given:
        manuallyRegisterLock()
        run('''CREATE (:__LiquibaseLock:__LiquigraphLock { id: $id, name: 'ExtraFakeLock', locked_by: 'ExtraFakeLock', grant_date: DATETIME() })''',
                Maps.of("id", UUID.randomUUID().toString()))

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
        run('''CREATE (:__LiquibaseLock:__LiquigraphLock { id: $id, name: 'ExtraFakeLock', locked_by: 'ExtraFakeLock', grant_date: DATETIME({year:1986, month:3, day:4})})''',
                Maps.of("id", extraLockId.toString()))

        when:
        def locks = neo4jLockService.listLocks()

        then:
        locks.size() == 2
        with(locks[0]) {
            it.id == extraLockId.hashCode()
            it.lockGranted == date(1986, 3, 4)
            it.lockedBy == "ExtraFakeLock"
        }
        with(locks[1]) {
            it.lockGranted > nowMinus(Duration.of(1, ChronoUnit.MINUTES))
            it.lockedBy == "Neo4jLockService"
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
        def constraintDescriptions = existingConstraintDescriptions()
        constraintDescriptions.findIndexOf {it.contains("__liquibaselock:__LiquibaseLock")} >= 0
        constraintDescriptions.findIndexOf {it.contains("__liquigraphlock:__LiquigraphLock")} >= 0
    }

    private Object countLockNodes() {
        def row = querySingleRow("MATCH (l:__LiquibaseLock:__LiquigraphLock) RETURN COUNT(l) AS count")
        return row["count"]
    }

    private List<String> existingConstraintDescriptions() {
        // names are not available before Neo4j 4.x
        def descriptions = (String[]) querySingleRow("CALL db.constraints() YIELD description RETURN COLLECT(description) AS descriptions")["descriptions"]
        return Arrays.asList(descriptions)
    }

    private Map<String, Object> querySingleRow(String query) {
        driver.session().withCloseable { session ->
            session.readTransaction({ tx ->
                tx.run(query).single().asMap()
            })
        }
    }

    private void manuallyDefineConstraints() {
        run(neo4jVersion().startsWith("4") ?
                String.format("CREATE CONSTRAINT %s ON (lock:__LiquibaseLock) ASSERT lock.locked_by IS UNIQUE", CONSTRAINT_NAME):
                "CREATE CONSTRAINT ON (lock:__LiquibaseLock) ASSERT lock.locked_by IS UNIQUE")
        run(neo4jVersion().startsWith("4") ?
                String.format("CREATE CONSTRAINT %s_bc ON (lock:__LiquigraphLock) ASSERT lock.name IS UNIQUE", CONSTRAINT_NAME):
                "CREATE CONSTRAINT ON (lock:__LiquigraphLock) ASSERT lock.name IS UNIQUE"
        )
    }

    private void manuallyRemoveConstraints() {
        ignoring(exceptionMessageContaining("no such constraint"), {
            def query = neo4jVersion().startsWith("4") ?
                    String.format("DROP CONSTRAINT %s", CONSTRAINT_NAME) :
                    "DROP CONSTRAINT ON (lock:__LiquibaseLock) ASSERT lock.locked_by IS UNIQUE"
            run(query)
        })
        ignoring(exceptionMessageContaining("no such constraint"), {
            def query = neo4jVersion().startsWith("4") ?
                    String.format("DROP CONSTRAINT %s_bc", CONSTRAINT_NAME) :
                    "DROP CONSTRAINT ON (lock:__LiquigraphLock) ASSERT lock.name IS UNIQUE"
            run(query)
        })
    }

    private void manuallyRegisterLock() {
        def lockId = UUID.randomUUID()
        setField("lockId", neo4jLockService, lockId)
        run(
                '''CREATE (:__LiquibaseLock:__LiquigraphLock { id: $uuid, name: 'Neo4jLockService', locked_by: 'Neo4jLockService', grant_date: DATETIME() })''',
                mapOf("uuid", (Object) lockId.toString())
        )
    }

    private void run(String query) {
        run(query, new HashMap<String, Object>(0))
    }

    private void run(String query, Map<String, Object> params) {
        driver.session().withCloseable { session ->
            session.writeTransaction({ tx ->
                tx.run(query, params)
            })
        }
    }

    private Driver driver() {
        GraphDatabase.driver(neo4jContainer.getBoltUrl(),
                AuthTokens.basic("neo4j", PASSWORD))
    }

    private static <K, V> Map<K, V> mapOf(K key, V value) {
        def result = new HashMap<K, V>(1)
        result[key] = value
        return result
    }

    private static void setField(String fieldName, Object instance, Object value) {
        def field = instance.getClass().getDeclaredField(fieldName)
        field.setAccessible(true)
        field.set(instance, value)
    }

    private static Object getField(String fieldName, Object instance) {
        def field = instance.getClass().getDeclaredField(fieldName)
        field.setAccessible(true)
        return field.get(instance)
    }

    private static Predicate<Exception> exceptionMessageContaining(String message) {
        return { e -> e.getMessage().toLowerCase(Locale.ENGLISH).contains(message) }
    }

    private static void ignoring(Predicate<Exception> predicate, Closure closure) {
        try {
            closure.run()
        } catch (e) {
            if (!predicate.test(e)) {
                throw e
            }
        }
    }

    private static Date date(Integer year, Integer month, Integer day) {
        Date.from(LocalDate.of(year, month, day).atStartOfDay().atZone(TIMEZONE).toInstant())
    }

    private static Date nowMinus(Duration duration) {
        Date.from((LocalDateTime.now() - duration).atZone(TIMEZONE).toInstant())
    }
}
