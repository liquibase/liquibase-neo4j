package liquibase.ext.neo4j.precondition

import liquibase.database.core.H2Database
import liquibase.exception.ValidationErrors
import spock.lang.Specification

class FunctionExistsPreconditionTest extends Specification {

    private FunctionExistsPrecondition precondition

    def setup() {
        precondition = new FunctionExistsPrecondition()
    }

    def "fails validation with non-Neo4j databases"() {
        expect:
        precondition.setFunctionName(functionName)
        precondition.validate(db) == expected

        where:
        functionName | db               | expected
        null         | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "functionName must be set and not blank")
        ""           | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "functionName must be set and not blank")
        " "          | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "functionName must be set and not blank")
        "abs"        | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2")
        null         | null             | withErrors("this precondition applies only to Neo4j but got ", "functionName must be set and not blank")
        ""           | null             | withErrors("this precondition applies only to Neo4j but got ", "functionName must be set and not blank")
        " "          | null             | withErrors("this precondition applies only to Neo4j but got ", "functionName must be set and not blank")
        "abs"        | null             | withErrors("this precondition applies only to Neo4j but got ")
    }

    private static ValidationErrors withErrors(String... errors) {
        def result = new ValidationErrors()
        for (def error in errors) {
            result.addError(error)
        }
        return result
    }
}
