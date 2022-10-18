package liquibase.ext.neo4j.precondition

import liquibase.database.DatabaseConnection
import liquibase.database.DatabaseFactory
import liquibase.database.core.H2Database
import liquibase.exception.PreconditionFailedException
import liquibase.exception.ValidationErrors
import liquibase.ext.neo4j.DockerNeo4j
import liquibase.ext.neo4j.database.Neo4jDatabase
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Neo4jContainer
import spock.lang.Shared
import spock.lang.Specification

import java.time.ZoneId
import java.util.logging.LogManager

class Neo4jVersionPreconditionIT extends Specification {

    private static final String VERSION = "4.4.4"

    private static final String PASSWORD = "s3cr3t"

    @Shared
    private GenericContainer<Neo4jContainer> neo4jContainer = DockerNeo4j.container(
            PASSWORD,
            ZoneId.of("Europe/Paris"),
            "4.4.4"
    )

    @Shared
    private Neo4jDatabase database

    private Neo4jVersionPrecondition precondition

    static {
        LogManager.getLogManager().reset()
    }

    def setupSpec() {
        neo4jContainer.start()
        database = new Neo4jDatabase()
        database.setConnection(openConnection())
    }

    def setup() {
        precondition = new Neo4jVersionPrecondition()
    }

    def cleanupSpec() {
        database.close()
        neo4jContainer.stop()
    }

    def "passes validation only with Neo4j databases and non-blank versions"() {
        expect:
        precondition.setMatches(version)
        precondition.validate(db) == expected

        where:
        version | db               | expected
        null    | database         | withErrors("version must be set and not blank")
        ""      | database         | withErrors("version must be set and not blank")
        " "     | database         | withErrors("version must be set and not blank")
        "foo"   | database         | noErrors()
        null    | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "version must be set and not blank")
        ""      | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "version must be set and not blank")
        " "     | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "version must be set and not blank")
        "foo"   | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2")
        null    | null             | withErrors("this precondition applies only to Neo4j but got ", "version must be set and not blank")
        ""      | null             | withErrors("this precondition applies only to Neo4j but got ", "version must be set and not blank")
        " "     | null             | withErrors("this precondition applies only to Neo4j but got ", "version must be set and not blank")
        "foo"   | null             | withErrors("this precondition applies only to Neo4j but got ")
    }

    def "successfully runs only if version matches"() {
        when:
        precondition.setMatches(version)
        precondition.check(database, null, null, null)

        then:
        noExceptionThrown()

        where:
        version << ["4", "4.4", "4.4.4"]
    }

    def "fails if version does not match"() {
        when:
        precondition.setMatches(version)
        precondition.check(database, null, null, null)

        then:
        def ex = thrown(PreconditionFailedException)
        ex.failedPreconditions.size() == 1
        ex.failedPreconditions.iterator().next().message == "expected ${version} version but got ${VERSION}"

        where:
        version << ["3", "4.5", "4.4.5"]
    }

    private DatabaseConnection openConnection() {
        return DatabaseFactory.instance.openConnection(
                "jdbc:neo4j:" + neo4jContainer.getBoltUrl(),
                "neo4j",
                PASSWORD,
                null,
                null
        )
    }

    private static ValidationErrors withErrors(String... errors) {
        def result = new ValidationErrors()
        for (def error in errors) {
            result.addError(error)
        }
        return result
    }

    private static ValidationErrors noErrors() {
        return new ValidationErrors()
    }
}
