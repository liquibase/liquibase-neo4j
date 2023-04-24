package liquibase.ext.neo4j.change

import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.change.refactoring.PropertyMergePolicy
import liquibase.ext.neo4j.change.refactoring.PropertyMergeStrategy
import liquibase.ext.neo4j.database.Neo4jDatabase
import spock.lang.Specification

import static liquibase.ext.neo4j.change.refactoring.PropertyMergeStrategy.KEEP_ALL

class MergeNodesChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new MergeNodesChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "rejects missing mandatory fields"() {
        given:
        def mergeNodes = new MergeNodesChange()
        mergeNodes.fragment = fragment
        mergeNodes.outputVariable = outputVariable
        mergeNodes.propertyPolicies = propertyPolicies

        expect:
        mergeNodes.validate(new Neo4jDatabase()).getErrorMessages() == [error]

        where:
        fragment | outputVariable | propertyPolicies                  | error
        null     | "(n)"          | [aPropertyPolicy()]               | "missing Cypher fragment"
        ""       | "(n)"          | [aPropertyPolicy()]               | "missing Cypher fragment"
        "   "    | "(n)"          | [aPropertyPolicy()]               | "missing Cypher fragment"
        "n"      | null           | [aPropertyPolicy()]               | "missing Cypher output variable"
        "n"      | ""             | [aPropertyPolicy()]               | "missing Cypher output variable"
        "n"      | "   "          | [aPropertyPolicy()]               | "missing Cypher output variable"
        "n"      | "(n)"          | null                              | "missing property merge policy"
        "n"      | "(n)"          | []                                | "missing property merge policy"
        "n"      | "(n)"          | [null]                            | "property merge policy cannot be null"
        "n"      | "(n)"          | [propertyPolicy(null, ".*")]      | "missing merge strategy of property merge policy"
        "n"      | "(n)"          | [propertyPolicy(KEEP_ALL, "")]    | "missing property matcher of property merge policy"
        "n"      | "(n)"          | [propertyPolicy(KEEP_ALL, "   ")] | "missing property matcher of property merge policy"
    }

    private PropertyMergePolicy aPropertyPolicy() {
        return propertyPolicy(KEEP_ALL, ".*")
    }

    private PropertyMergePolicy propertyPolicy(PropertyMergeStrategy strategy, String matcher) {
        def policy = new PropertyMergePolicy()
        policy.mergeStrategy = strategy
        policy.nameMatcher = matcher
        return policy
    }
}
