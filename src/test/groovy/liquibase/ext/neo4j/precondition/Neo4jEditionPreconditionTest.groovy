package liquibase.ext.neo4j.precondition


import liquibase.database.core.H2Database
import liquibase.exception.ValidationErrors
import spock.lang.Specification

class Neo4jEditionPreconditionTest extends Specification {

    private Neo4jEditionPrecondition precondition

    def setup() {
        precondition = new Neo4jEditionPrecondition()
    }

    def "fails validation with non-Neo4j databases"() {
        expect:
        precondition.validate(db) == expected

        where:
        db               | expected
        new H2Database() | withErrors("this precondition applies only to Neo4j but got h2")
        null             | withErrors("this precondition applies only to Neo4j but got ")
    }

    private static ValidationErrors withErrors(String... errors) {
        def result = new ValidationErrors()
        for (def error in errors) {
            result.addError(error)
        }
        return result
    }
}
