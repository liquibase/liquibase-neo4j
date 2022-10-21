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

class Neo4jVersionPreconditionTest extends Specification {

    private Neo4jVersionPrecondition precondition

    def setup() {
        precondition = new Neo4jVersionPrecondition()
    }

    def "fails validation with non-Neo4j databases"() {
        expect:
        precondition.setMatches(version)
        precondition.validate(db) == expected

        where:
        version | db               | expected
        null    | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "version must be set and not blank")
        ""      | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "version must be set and not blank")
        " "     | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "version must be set and not blank")
        "foo"   | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2")
        null    | null             | withErrors("this precondition applies only to Neo4j but got ", "version must be set and not blank")
        ""      | null             | withErrors("this precondition applies only to Neo4j but got ", "version must be set and not blank")
        " "     | null             | withErrors("this precondition applies only to Neo4j but got ", "version must be set and not blank")
        "foo"   | null             | withErrors("this precondition applies only to Neo4j but got ")
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
