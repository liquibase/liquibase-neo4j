package liquibase.ext.neo4j.precondition


import liquibase.exception.PreconditionFailedException
import liquibase.exception.ValidationErrors
import liquibase.ext.neo4j.Neo4jContainerSpec

class Neo4jVersionPreconditionIT extends Neo4jContainerSpec {

    private Neo4jVersionPrecondition precondition

    def setup() {
        precondition = new Neo4jVersionPrecondition()
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
        ex.failedPreconditions.iterator().next().message == "expected ${version} version but got ${neo4jImageVersion()}"

        where:
        version << ["3", "4.5", "4.4.5"]
    }

    protected neo4jImageVersion() {
        "4.4.4"
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
