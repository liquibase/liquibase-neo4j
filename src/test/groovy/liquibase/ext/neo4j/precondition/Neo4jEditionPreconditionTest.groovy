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

class Neo4jEditionPreconditionTest extends Specification {

    private static final String PASSWORD = "s3cr3t"

    @Shared
    private GenericContainer<Neo4jContainer> neo4jContainer = DockerNeo4j.container(
            PASSWORD,
            ZoneId.of("Europe/Paris"),
            "4.4-enterprise"
    ).withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")

    @Shared
    private Neo4jDatabase database

    private Neo4jEditionPrecondition precondition

    static {
        LogManager.getLogManager().reset()
    }

    def setupSpec() {
        neo4jContainer.start()
        database = new Neo4jDatabase()
        database.setConnection(openConnection())
    }

    def setup() {
        precondition = new Neo4jEditionPrecondition()
    }

    def cleanupSpec() {
        database.close()
        neo4jContainer.stop()
    }

    def "passes validation only with Neo4j databases"() {
        expect:
        precondition.validate(db) == expected

        where:
        db               | expected
        database         | noErrors()
        new H2Database() | withErrors("this precondition applies only to Neo4j but got h2")
        null             | withErrors("this precondition applies only to Neo4j but got ")
    }

    def "successfully runs only if edition matches"() {
        when:
        precondition.setEnterprise(true)
        precondition.check(database, null, null, null)

        then:
        noExceptionThrown()
    }

    def "fails if edition does not match"() {
        when:
        precondition.setEnterprise(false)
        precondition.check(database, null, null, null)

        then:
        def ex = thrown(PreconditionFailedException)
        ex.failedPreconditions.size() == 1
        ex.failedPreconditions.iterator().next().message == "expected community edition but got enterprise edition"
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
