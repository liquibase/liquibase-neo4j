package liquibase.ext.neo4j.precondition

import liquibase.database.core.H2Database
import liquibase.exception.ValidationErrors
import spock.lang.Specification

class ProcedureExistsPreconditionTest extends Specification {

    private ProcedureExistsPrecondition precondition

    def setup() {
        precondition = new ProcedureExistsPrecondition()
    }

    def "fails validation with non-Neo4j databases"() {
        expect:
        precondition.setProcedureName(procedureName)
        precondition.validate(db) == expected

        where:
        procedureName | db               | expected
        null          | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "procedureName must be set and not blank")
        ""            | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "procedureName must be set and not blank")
        " "           | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2", "procedureName must be set and not blank")
        "db.labels"   | new H2Database() | withErrors("this precondition applies only to Neo4j but got h2")
        null          | null             | withErrors("this precondition applies only to Neo4j but got ", "procedureName must be set and not blank")
        ""            | null             | withErrors("this precondition applies only to Neo4j but got ", "procedureName must be set and not blank")
        " "           | null             | withErrors("this precondition applies only to Neo4j but got ", "procedureName must be set and not blank")
        "db.labels"   | null             | withErrors("this precondition applies only to Neo4j but got ")
    }

    private static ValidationErrors withErrors(String... errors) {
        def result = new ValidationErrors()
        for (def error in errors) {
            result.addError(error)
        }
        return result
    }
}
