package liquibase.ext.neo4j.precondition


import liquibase.exception.PreconditionFailedException
import liquibase.exception.ValidationErrors
import liquibase.ext.neo4j.Neo4jContainerSpec

class Neo4jEditionPreconditionIT extends Neo4jContainerSpec {

    private Neo4jEditionPrecondition precondition

    def setup() {
        precondition = new Neo4jEditionPrecondition()
    }

    def "passes validation only with Neo4j databases"() {
        expect:
        precondition.validate(db) == expected

        where:
        db               | expected
        database         | noErrors()
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

    protected neo4jImageVersion() {
        return "4.4-enterprise"
    }

    private static ValidationErrors noErrors() {
        return new ValidationErrors()
    }
}
